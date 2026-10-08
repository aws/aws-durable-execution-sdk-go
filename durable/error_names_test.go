package durable_test

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// hashID mirrors the SDK's wire-ID hashing (durable/state.go): a
// checkpointed operation's wire ID is the first 16 hex characters of the
// MD5 of its positional ID.
func hashID(positionalID string) string {
	sum := md5.Sum([]byte(positionalID))
	return hex.EncodeToString(sum[:])[:16]
}

// staticClient is a fake ExecutionClient that stores nothing and returns a
// fresh token on every checkpoint. The replay state the SDK reads comes
// from the invocation input, not from this client.
type staticClient struct{ n int }

func (c *staticClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, nil
}

func (c *staticClient) Checkpoint(_ context.Context, _ durable.CheckpointInput) (durable.CheckpointOutput, error) {
	c.n++
	return durable.CheckpointOutput{CheckpointToken: fmt.Sprintf("tok-%d", c.n)}, nil
}

// recordedStep is a checkpointed STEP record at the first root position
// (positional ID "1"), named name and already succeeded. Seeding it makes
// the next invocation replay: the first operation the handler claims is
// positional ID "1", which this record already occupies.
func recordedStep(name string) map[string]any {
	return map[string]any{
		"Id": hashID("1"), "Type": "STEP", "SubType": "Step", "Status": "SUCCEEDED",
		"Name":        name,
		"StepDetails": map[string]any{"Attempt": 1, "Result": "\"old\""},
	}
}

// runReplay invokes h once with a seeded checkpoint log and returns the
// Status, ErrorType, and ErrorMessage of the invocation response.
func runReplay(t *testing.T, h durable.Handler[struct{}, string], recorded map[string]any) (status, errType, errMsg string) {
	t.Helper()
	in, err := json.Marshal(map[string]any{
		"DurableExecutionArn": "arn:aws:lambda:us-west-2:acct:function:fn:$LATEST/durable-execution/e/1",
		"CheckpointToken":     "tok-0",
		"InitialExecutionState": map[string]any{
			"Operations": []map[string]any{
				{"Id": "exec-op", "Type": "EXECUTION", "Status": "STARTED",
					"ExecutionDetails": map[string]any{"InputPayload": "{}"}},
				recorded,
			},
			"NextMarker": "",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := durable.Wrap(h, durable.WithExecutionClient(&staticClient{}))(ctx, in)
	if err != nil {
		t.Fatalf("invocation error: %v", err)
	}
	var r struct {
		Status string
		Error  *struct {
			ErrorType    string
			ErrorMessage string
		}
	}
	if err := json.Unmarshal(out, &r); err != nil {
		t.Fatal(err)
	}
	if r.Error != nil {
		return r.Status, r.Error.ErrorType, r.Error.ErrorMessage
	}
	return r.Status, "", ""
}

// TestNonDeterminismWireNameAndMessage asserts that a replay mismatch
// records the wire ErrorType "NonDeterministicExecutionError" and names
// both the expected and the actual operation, so a name-only mismatch
// shows the checkpointed name as well as the current one.
func TestNonDeterminismWireNameAndMessage(t *testing.T) {
	t.Run("name mismatch", func(t *testing.T) {
		// Checkpoint holds a step named "a"; the handler creates "b" there.
		h := func(ctx durable.Context, _ struct{}) (string, error) {
			return durable.Step(ctx, "b", func(durable.StepContext) (string, error) {
				return "new", nil
			})
		}
		status, errType, errMsg := runReplay(t, h, recordedStep("a"))
		t.Logf("name-mismatch: status=%s type=%s msg=%q", status, errType, errMsg)
		if errType != "NonDeterministicExecutionError" {
			t.Errorf("recorded ErrorType = %q, want %q", errType, "NonDeterministicExecutionError")
		}
		if !strings.Contains(errMsg, `"a"`) {
			t.Errorf("message = %q, want it to name the actual (checkpointed) operation name %q", errMsg, "a")
		}
	})

	t.Run("type mismatch", func(t *testing.T) {
		// Checkpoint holds a step named "a"; the handler creates a wait "a".
		h := func(ctx durable.Context, _ struct{}) (string, error) {
			if err := durable.Wait(ctx, "a", time.Second); err != nil {
				return "", err
			}
			return "done", nil
		}
		status, errType, errMsg := runReplay(t, h, recordedStep("a"))
		t.Logf("type-mismatch: status=%s type=%s msg=%q", status, errType, errMsg)
		if errType != "NonDeterministicExecutionError" {
			t.Errorf("recorded ErrorType = %q, want %q", errType, "NonDeterministicExecutionError")
		}
	})
}

// TestBatchCustomFailedWireType asserts that a batch failed by a custom
// completion decision with no failed item records the wire ErrorType
// "BatchCompletionError", not "BatchError".
func TestBatchCustomFailedWireType(t *testing.T) {
	h := func(ctx durable.Context, _ struct{}) (int, error) {
		res, err := durable.Map(ctx, "m", []int{0, 1, 2},
			func(c durable.Context, item, _ int) (int, error) {
				return durable.Step(c, "s", func(durable.StepContext) (int, error) { return item, nil })
			},
			durable.WithMaxConcurrency(1),
			durable.WithCompletion(durable.CompletionConfig{
				ShouldComplete: func(durable.BatchProgress) durable.CompletionDecision {
					return durable.CompleteBatch(durable.CompletionOutcomeFailed)
				},
			}))
		if err != nil {
			return 0, err
		}
		return res.SuccessCount(), nil
	}
	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", r.Status)
	}
	if r.Error == nil {
		t.Fatal("no error recorded on the failed execution")
	}
	t.Logf("batch custom-failed: type=%s msg=%q", r.Error.Type, r.Error.Message)
	if r.Error.Type != "BatchCompletionError" {
		t.Errorf("recorded ErrorType = %q, want %q (a custom-failed batch with no failed item)", r.Error.Type, "BatchCompletionError")
	}
}

// TestBatchItemFailedWireTypeUnchanged asserts that a batch failed with a
// failed item still returns a *BatchError recorded as "BatchError".
func TestBatchItemFailedWireTypeUnchanged(t *testing.T) {
	var gotErr error
	h := func(ctx durable.Context, _ struct{}) (int, error) {
		res, err := durable.Map(ctx, "m", []int{0, 1, 2},
			func(c durable.Context, item, _ int) (int, error) {
				return durable.Step(c, "s", func(durable.StepContext) (int, error) {
					if item == 1 {
						return 0, fmt.Errorf("item %d failed", item)
					}
					return item, nil
				}, durable.WithRetry(durable.NoRetry()))
			},
			durable.WithMaxConcurrency(1),
			durable.WithCompletion(durable.CompletionConfig{
				ShouldComplete: func(p durable.BatchProgress) durable.CompletionDecision {
					if p.FailureCount > 0 {
						return durable.CompleteBatch(durable.CompletionOutcomeFailed)
					}
					return durable.ContinueBatch()
				},
			}))
		gotErr = err
		if err != nil {
			return 0, err
		}
		return res.SuccessCount(), nil
	}
	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	var berr *durable.BatchError
	if !errors.As(gotErr, &berr) {
		t.Fatalf("Map err = %v (%T), want *BatchError", gotErr, gotErr)
	}
	var cerr *durable.BatchCompletionError
	if errors.As(gotErr, &cerr) {
		t.Errorf("Map err matches *BatchCompletionError, want only *BatchError")
	}
	if r.Status != durabletest.Failed || r.Error == nil {
		t.Fatalf("status = %s, error = %v; want FAILED with an error", r.Status, r.Error)
	}
	if r.Error.Type != "BatchError" {
		t.Errorf("recorded ErrorType = %q, want %q", r.Error.Type, "BatchError")
	}
}
