package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// transientUnmarshalSerdes marshals with encoding/json. Its Unmarshal
// returns a RetryableSerdesError on the failOn-th call whose data equals
// target, and delegates to encoding/json on every other call.
type transientUnmarshalSerdes struct {
	target string
	failOn int64
	seen   *int64
}

func newTransientUnmarshalSerdes(target string, failOn int64) transientUnmarshalSerdes {
	return transientUnmarshalSerdes{target: target, failOn: failOn, seen: new(int64)}
}

func (transientUnmarshalSerdes) Marshal(_ context.Context, _ durable.SerdesContext, v any) ([]byte, error) {
	return json.Marshal(v)
}

func (s transientUnmarshalSerdes) Unmarshal(_ context.Context, _ durable.SerdesContext, data []byte, v any) error {
	if string(data) == s.target && atomic.AddInt64(s.seen, 1) == s.failOn {
		return durable.RetryableSerdesError(errors.New("offload store read timed out"))
	}
	return json.Unmarshal(data, v)
}

// terminalEvents counts the Succeeded and Failed history events recorded
// for operations named name.
func terminalEvents(events []types.Event, name string) (succeeded, failed int) {
	for _, e := range events {
		if aws.ToString(e.Name) != name {
			continue
		}
		switch t := string(e.EventType); {
		case strings.HasSuffix(t, "Succeeded"):
			succeeded++
		case strings.HasSuffix(t, "Failed"):
			failed++
		}
	}
	return succeeded, failed
}

// A RetryableSerdesError from the Unmarshal that round-trips a live result
// ends the invocation before the operation records an outcome. So the next
// invocation finds no recorded result and runs the body again under
// AtLeastOncePerRetry. A recorded SUCCEED would make the next invocation
// replay it, and the body would run once only.
func TestRetryableSerdesErrorOnLiveUnmarshalRecordsNothing(t *testing.T) {
	cases := []struct {
		name string
		// op names the operation that round-trips the value, when its
		// history events carry that name.
		op string
		// run executes the operation. body is the counter of body runs.
		run func(ctx durable.Context, s durable.Serdes, body *int64) (string, error)
	}{
		{
			name: "step",
			op:   "s",
			run: func(ctx durable.Context, s durable.Serdes, body *int64) (string, error) {
				return durable.Step(ctx, "s", func(_ durable.StepContext) (string, error) {
					atomic.AddInt64(body, 1)
					return "value", nil
				}, durable.WithStepSerdes(s))
			},
		},
		{
			name: "child context",
			op:   "child",
			run: func(ctx durable.Context, s durable.Serdes, body *int64) (string, error) {
				return durable.RunInChildContext(ctx, "child", func(_ durable.Context) (string, error) {
					atomic.AddInt64(body, 1)
					return "value", nil
				}, durable.WithChildSerdes(s))
			},
		},
		{
			name: "child context async",
			op:   "child",
			run: func(ctx durable.Context, s durable.Serdes, body *int64) (string, error) {
				return durable.RunInChildContextAsync(ctx, "child", func(_ durable.Context) (string, error) {
					atomic.AddInt64(body, 1)
					return "value", nil
				}, durable.WithChildSerdes(s)).Result(ctx)
			},
		},
		{
			name: "map item",
			run: func(ctx durable.Context, s durable.Serdes, body *int64) (string, error) {
				res, err := durable.Map(ctx, "map", []string{"value"}, func(_ durable.Context, item string, _ int) (string, error) {
					atomic.AddInt64(body, 1)
					return item, nil
				}, durable.WithBatchSerdes(s))
				if err != nil {
					return "", err
				}
				if len(res.Failed()) > 0 {
					return "", errors.New("the BatchResult reported a failed item")
				}
				return res.Results()[0], nil
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body int64
			serdes := newTransientUnmarshalSerdes(`"value"`, 1)
			h := func(ctx durable.Context, _ struct{}) (string, error) {
				v, err := tc.run(ctx, serdes, &body)
				if err != nil && errors.Is(err, durable.ErrRetryableSerdes) {
					// The handler catches the error; the invocation
					// must end with it anyway.
					return "caught", nil
				}
				return v, err
			}
			runner := durabletest.NewLocalRunner(h)

			if _, err := runner.Run(struct{}{}); !errors.Is(err, durable.ErrRetryableSerdes) {
				t.Fatalf("first invocation error = %v, want one matching ErrRetryableSerdes", err)
			}
			r, err := runner.RunUntilComplete(struct{}{})
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != durabletest.Succeeded {
				t.Fatalf("status = %s, error = %+v; want SUCCEEDED", r.Status, r.Error)
			}
			if got, err := durabletest.ResultAs[string](r); err != nil || got != "value" {
				t.Errorf("result = %q, %v; want \"value\"", got, err)
			}
			if n := atomic.LoadInt64(&body); n != 2 {
				t.Errorf("body ran %d times, want 2 (once per invocation)", n)
			}
			if tc.op != "" {
				succeeded, failed := terminalEvents(r.Events, tc.op)
				if succeeded != 1 || failed != 0 {
					t.Errorf("operation %q recorded %d Succeeded and %d Failed events, want 1 and 0", tc.op, succeeded, failed)
				}
			}
		})
	}
}

// A RetryableSerdesError from the Unmarshal that round-trips the live
// result of a WaitForCallback ends the invocation before the context
// records an outcome. The next invocation runs the context body again and
// reads the callback result recorded before the failure.
func TestRetryableSerdesErrorOnWaitForCallbackUnmarshalRecordsNothing(t *testing.T) {
	var submits int64
	// The first decode of "approved" is the callback's own result; the
	// second is the round-trip of the context result.
	serdes := newTransientUnmarshalSerdes(`"approved"`, 2)
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.WaitForCallback[string](ctx, "approval", func(_ durable.StepContext, _ string) error {
			atomic.AddInt64(&submits, 1)
			return nil
		})
	}
	runner := durabletest.NewLocalRunner(h, durable.WithSerdes(serdes))

	r, err := runner.Run(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", r.Status)
	}
	open := runner.OpenCallbacks()
	if len(open) != 1 {
		t.Fatalf("open callbacks = %d, want 1", len(open))
	}
	if err := runner.SendCallbackSuccess(open[0].CallbackID, "approved"); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(struct{}{}); !errors.Is(err, durable.ErrRetryableSerdes) {
		t.Fatalf("invocation after the callback error = %v, want one matching ErrRetryableSerdes", err)
	}
	r, err = runner.RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, error = %+v; want SUCCEEDED", r.Status, r.Error)
	}
	if got, err := durabletest.ResultAs[string](r); err != nil || got != "approved" {
		t.Errorf("result = %q, %v; want \"approved\"", got, err)
	}
	succeeded, failed := terminalEvents(r.Events, "approval")
	if succeeded != 1 || failed != 0 {
		t.Errorf("operation approval recorded %d Succeeded and %d Failed events, want 1 and 0", succeeded, failed)
	}
	// The callback result is decoded once per run of the context body,
	// and once more by each round-trip of the context result. A context
	// whose success was recorded before the failure would be replayed
	// from that record: one decode in the last invocation instead of two.
	if n := atomic.LoadInt64(serdes.seen); n != 4 {
		t.Errorf("decoded the callback result %d times, want 4 (the body ran again after the failure)", n)
	}
	if n := atomic.LoadInt64(&submits); n != 1 {
		t.Errorf("submitter ran %d times, want 1 (its step result is recorded)", n)
	}
}

// A RetryableSerdesError ends only the invocation even when the error it
// wraps states an execution scope. The serdes marked the failure
// transient, so the scope of the wrapped cause does not apply.
func TestRetryableSerdesErrorWrappingExecutionScopeEndsInvocation(t *testing.T) {
	cause := &durable.ClientError{Scope: durable.ErrorScopeExecution, Err: errors.New("access denied")}
	var runs int64
	var calls int64
	s := durable.SerdesOf(
		func(_ context.Context, _ durable.SerdesContext, v string) ([]byte, error) {
			if atomic.AddInt64(&calls, 1) == 1 {
				return nil, durable.RetryableSerdesError(cause)
			}
			return json.Marshal(v)
		},
		func(_ context.Context, _ durable.SerdesContext, data []byte) (string, error) {
			var v string
			err := json.Unmarshal(data, &v)
			return v, err
		},
	)
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.Step(ctx, "s", func(_ durable.StepContext) (string, error) {
			atomic.AddInt64(&runs, 1)
			return "value", nil
		}, durable.WithStepSerdes(s))
	}
	runner := durabletest.NewLocalRunner(h)
	if _, err := runner.Run(struct{}{}); !errors.Is(err, durable.ErrRetryableSerdes) {
		t.Fatalf("first invocation error = %v, want one matching ErrRetryableSerdes, not a FAILED response", err)
	}
	r, err := runner.RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, error = %+v; want SUCCEEDED", r.Status, r.Error)
	}
	if n := atomic.LoadInt64(&runs); n != 2 {
		t.Errorf("step body ran %d times, want 2", n)
	}
}
