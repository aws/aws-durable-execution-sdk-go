package durable

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"
)

// warnRecord is one WARN-level record with the attributes it carried,
// including the attributes attached to the handler that received it.
type warnRecord struct {
	message string
	attrs   map[string]string
}

// warnRecorder is a slog.Handler that keeps every WARN-level record.
type warnRecorder struct {
	mu      *sync.Mutex
	records *[]warnRecord
	attrs   []slog.Attr
}

func newWarnRecorder() *warnRecorder {
	return &warnRecorder{mu: &sync.Mutex{}, records: &[]warnRecord{}}
}

func (h *warnRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *warnRecorder) Handle(_ context.Context, r slog.Record) error {
	if r.Level < slog.LevelWarn {
		return nil
	}
	rec := warnRecord{message: r.Message, attrs: map[string]string{}}
	for _, a := range h.attrs {
		rec.attrs[a.Key] = a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	*h.records = append(*h.records, rec)
	h.mu.Unlock()
	return nil
}

func (h *warnRecorder) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &warnRecorder{mu: h.mu, records: h.records, attrs: append(append([]slog.Attr{}, h.attrs...), attrs...)}
}

func (h *warnRecorder) WithGroup(string) slog.Handler { return h }

func (h *warnRecorder) all() []warnRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]warnRecord(nil), *h.records...)
}

// tokenlessPollClient answers every request that carries updates with a
// token, and every poll (a request with no updates) without one.
type tokenlessPollClient struct {
	mu    sync.Mutex
	n     int
	polls int
}

func (c *tokenlessPollClient) GetExecutionState(context.Context, GetExecutionStateInput) (GetExecutionStateOutput, error) {
	return GetExecutionStateOutput{}, nil
}

func (c *tokenlessPollClient) Checkpoint(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	if len(in.Updates) == 0 {
		c.polls++
		return CheckpointOutput{}, nil
	}
	out := CheckpointOutput{CheckpointToken: "tok-" + strings.Repeat("x", c.n)}
	for _, u := range in.Updates {
		if u.Type == OperationTypeCallback && u.Action == OperationActionStart {
			out.NewExecutionState = append(out.NewExecutionState, Operation{
				Id: u.Id, Type: u.Type, SubType: u.SubType, Status: OperationStatusStarted,
				CallbackDetails: &CallbackDetails{CallbackId: ptrTo("cb-1")},
			})
		}
	}
	return out, nil
}

func (c *tokenlessPollClient) pollCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.polls
}

// TestTokenlessPollResponsePends covers a poll answered without a token.
// A poll carries no execution update, so the response is not terminal: the
// invocation responds PENDING and writes one WARN record carrying the
// execution ARN and the request ID.
func TestTokenlessPollResponsePends(t *testing.T) {
	client := &tokenlessPollClient{}
	rec := newWarnRecorder()
	h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
		cb, err := CreateCallback[string](ctx, "approval")
		if err != nil {
			return "", err
		}
		// The step keeps the invocation running past the callback's
		// first poll, one second after the handler blocks on it.
		long := StepAsync(ctx, "work", func(StepContext) (string, error) {
			time.Sleep(1500 * time.Millisecond)
			return "worked", nil
		})
		v, err := cb.Result(ctx)
		if err != nil {
			return "", err
		}
		if _, err := long.Result(ctx); err != nil {
			return "", err
		}
		return v, nil
	}, WithExecutionClient(client), WithLogHandler(rec))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{AwsRequestID: "req-1"})
	out, err := h(ctx, stepPayload(`""`))
	if err != nil {
		t.Fatal(err)
	}
	resp := parseResponse(t, out)
	if resp.Status != invocationPending {
		t.Fatalf("status = %q, want %q", resp.Status, invocationPending)
	}
	if got := client.pollCount(); got != 1 {
		t.Errorf("polls = %d, want 1", got)
	}
	warns := rec.all()
	if len(warns) != 1 {
		t.Fatalf("WARN records = %d (%+v), want 1", len(warns), warns)
	}
	w := warns[0]
	if w.message != tokenWithdrawnMessage {
		t.Errorf("message = %q, want %q", w.message, tokenWithdrawnMessage)
	}
	if got := w.attrs[logKeyRequestID]; got != "req-1" {
		t.Errorf("%s = %q, want req-1", logKeyRequestID, got)
	}
	if got := w.attrs[logKeyExecutionArn]; got == "" {
		t.Errorf("%s is missing", logKeyExecutionArn)
	}
}

// TestTerminalTokenlessResponseLogsNoWarn covers the terminal case: a
// response without a token to the execution's terminal update reports
// SUCCEEDED and writes no WARN record.
func TestTerminalTokenlessResponseLogsNoWarn(t *testing.T) {
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)
	fake, calls := countingClient(func(CheckpointInput) (CheckpointOutput, error) {
		return CheckpointOutput{}, nil
	})
	rec := newWarnRecorder()
	h := Wrap(func(_ Context, _ string) (string, error) {
		return large, nil
	}, withLambdaAPI(fake), WithLogHandler(rec))
	raw, err := h(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatal(err)
	}
	if resp := parseResponse(t, raw); resp.Status != invocationSucceeded {
		t.Fatalf("status = %q, want %q", resp.Status, invocationSucceeded)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("checkpoint calls = %d, want 1", got)
	}
	if warns := rec.all(); len(warns) != 0 {
		t.Errorf("WARN records = %+v, want none", warns)
	}
}

// TestTokenlessResponseDoesNotApplyState covers a non-terminal response
// without a token that also reports execution state: the state is not
// merged.
func TestTokenlessResponseDoesNotApplyState(t *testing.T) {
	fake, _ := countingClient(func(CheckpointInput) (CheckpointOutput, error) {
		return CheckpointOutput{NewExecutionState: []Operation{{
			Id: ptrTo("reported"), Type: OperationTypeStep, Status: OperationStatusSucceeded,
		}}}, nil
	})
	cp := newCheckpointer(fake, "arn:test", "token-0")
	cp.state = newExecutionState(nil)
	stepID := "step-1"
	err := cp.checkpoint(context.Background(), []OperationUpdate{{
		Id: &stepID, Type: OperationTypeStep, Action: OperationActionSucceed,
	}})
	if err != errCheckpointTerminated {
		t.Fatalf("checkpoint() = %v, want errCheckpointTerminated", err)
	}
	if op := cp.state.getByWireID("reported"); op != nil {
		t.Errorf("state holds %+v, want the response's state not applied", op)
	}
}
