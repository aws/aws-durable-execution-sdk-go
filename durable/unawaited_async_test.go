// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// A StepAsync the handler never awaits, whose body fails with a one-minute
// retry delay, does not keep the invocation PENDING for the retry. The
// execution succeeds in one invocation, and the step is recorded.
func TestUnawaitedStepRetry(t *testing.T) {
	retry := func(durable.RetryAttempt) durable.RetryDecision {
		return durable.RetryDecision{Retry: true, Delay: time.Minute}
	}
	handler := func(ctx durable.Context, _ any) (string, error) {
		_ = durable.StepAsync(ctx, "bg", func(_ durable.StepContext) (string, error) {
			return "", errors.New("transient")
		}, durable.WithRetry(retry))
		return "done", nil
	}
	r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("unawaited-step-retry status=%s events=%v", r.Status, r.EventTypes())
	assertUnawaitedRecorded(t, r, "bg", "StepStarted", "STARTED", "PENDING")
}

// The START of an asynchronous operation is queued at its call. So it is
// recorded before the START of an operation the caller starts after the
// call, even when the caller awaits the future only after that operation.
func TestAsyncStartPrecedesLaterOperation(t *testing.T) {
	later := func(ctx durable.Context) error {
		_, err := durable.Step(ctx, "later", func(_ durable.StepContext) (string, error) {
			return "x", nil
		})
		return err
	}
	cases := []struct {
		name       string
		startEvent string
		handler    func(ctx durable.Context, _ any) (string, error)
	}{
		{
			name:       "step",
			startEvent: "StepStarted",
			handler: func(ctx durable.Context, _ any) (string, error) {
				fut := durable.StepAsync(ctx, "async", func(_ durable.StepContext) (string, error) {
					return "a", nil
				})
				if err := later(ctx); err != nil {
					return "", err
				}
				return fut.Result(ctx)
			},
		},
		{
			name:       "step-at-most-once",
			startEvent: "StepStarted",
			handler: func(ctx durable.Context, _ any) (string, error) {
				fut := durable.StepAsync(ctx, "async", func(_ durable.StepContext) (string, error) {
					return "a", nil
				}, durable.WithSemantics(durable.AtMostOncePerRetry))
				if err := later(ctx); err != nil {
					return "", err
				}
				return fut.Result(ctx)
			},
		},
		{
			name:       "wait",
			startEvent: "WaitStarted",
			handler: func(ctx durable.Context, _ any) (string, error) {
				fut := durable.WaitAsync(ctx, "async", time.Second)
				if err := later(ctx); err != nil {
					return "", err
				}
				_, err := fut.Result(ctx)
				return "w", err
			},
		},
		{
			name:       "invoke",
			startEvent: "ChainedInvokeStarted",
			handler: func(ctx durable.Context, _ any) (string, error) {
				fut := durable.InvokeAsync[string](ctx, "async", "target-function:$LATEST", "in")
				if err := later(ctx); err != nil {
					return "", err
				}
				return fut.Result(ctx)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := durabletest.NewLocalRunner(tc.handler).Run(nil)
			if err != nil {
				t.Fatal(err)
			}
			asyncStart := eventIndex(r, tc.startEvent, "async")
			laterStart := eventIndex(r, "StepStarted", "later")
			if asyncStart < 0 || laterStart < 0 {
				t.Fatalf("missing start events: %s at %d, later StepStarted at %d (events %v)",
					tc.startEvent, asyncStart, laterStart, r.EventTypes())
			}
			if asyncStart > laterStart {
				t.Fatalf("%s of the async operation at %d follows StepStarted of later at %d (events %v)",
					tc.startEvent, asyncStart, laterStart, r.EventTypes())
			}
		})
	}
}

// A WaitAsync future the handler awaits, directly or through a combinator,
// still holds the invocation: the first invocation answers PENDING, and a
// later invocation succeeds once the wait elapses.
func TestAwaitedWaitAsyncHoldsInvocation(t *testing.T) {
	cases := []struct {
		name    string
		handler func(ctx durable.Context, _ any) (string, error)
	}{
		{
			name: "direct",
			handler: func(ctx durable.Context, _ any) (string, error) {
				if _, err := durable.WaitAsync(ctx, "w", time.Hour).Result(ctx); err != nil {
					return "", err
				}
				return "done", nil
			},
		},
		{
			name: "combinator",
			handler: func(ctx durable.Context, _ any) (string, error) {
				fut := durable.WaitAsync(ctx, "w", time.Hour)
				if _, err := durable.All(ctx, "all", []*durable.Future[durable.Void]{fut}); err != nil {
					return "", err
				}
				return "done", nil
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runner := durabletest.NewLocalRunner(tc.handler)
			first, err := runner.Run(nil)
			if err != nil {
				t.Fatal(err)
			}
			if first.Status != durabletest.Pending {
				t.Fatalf("first invocation status = %s, want PENDING (events %v)", first.Status, first.EventTypes())
			}
			r, err := runner.RunUntilComplete(nil)
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != durabletest.Succeeded {
				t.Fatalf("status = %s, want SUCCEEDED (events %v)", r.Status, r.EventTypes())
			}
			if n := countEvents(r, "InvocationCompleted"); n < 2 {
				t.Fatalf("InvocationCompleted events = %d, want at least 2 (events %v)", n, r.EventTypes())
			}
		})
	}
}
