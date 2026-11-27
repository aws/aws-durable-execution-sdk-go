package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// failingUnmarshalSerdes marshals with encoding/json and fails every
// Unmarshal.
type failingUnmarshalSerdes struct{}

func (failingUnmarshalSerdes) Marshal(_ context.Context, _ durable.SerdesContext, v any) ([]byte, error) {
	return json.Marshal(v)
}

func (failingUnmarshalSerdes) Unmarshal(context.Context, durable.SerdesContext, []byte, any) error {
	return errors.New("unmarshal boom")
}

// transientMarshalSerdes returns a RetryableSerdesError from the first
// failures calls to Marshal and delegates to encoding/json afterwards.
type transientMarshalSerdes struct {
	failures int64
	calls    *int64
}

func newTransientMarshalSerdes(failures int64) transientMarshalSerdes {
	return transientMarshalSerdes{failures: failures, calls: new(int64)}
}

func (s transientMarshalSerdes) Marshal(_ context.Context, _ durable.SerdesContext, v any) ([]byte, error) {
	if atomic.AddInt64(s.calls, 1) <= s.failures {
		return nil, durable.RetryableSerdesError(errors.New("offload store timed out"))
	}
	return json.Marshal(v)
}

func (transientMarshalSerdes) Unmarshal(_ context.Context, _ durable.SerdesContext, data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// serdesOutcome is what a handler under test reports about the error an
// operation returned.
type serdesOutcome struct {
	IsSerdesError bool   `json:"isSerdesError"`
	Direction     string `json:"direction"`
	Operation     string `json:"operation"`
}

// describeSerdesError returns the serdesOutcome of err.
func describeSerdesError(err error) serdesOutcome {
	var se *durable.SerdesError
	if !errors.As(err, &se) {
		return serdesOutcome{}
	}
	return serdesOutcome{IsSerdesError: true, Direction: se.Direction, Operation: se.Operation}
}

// runToOutcome drives h to completion and decodes the serdesOutcome it
// returned. It fails the test unless the execution succeeded.
func runToOutcome(t *testing.T, h durable.Handler[struct{}, serdesOutcome], register func(*durabletest.LocalRunner[struct{}, serdesOutcome])) serdesOutcome {
	t.Helper()
	runner := durabletest.NewLocalRunner(h)
	if register != nil {
		register(runner)
	}
	r, err := runner.RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := durabletest.ResultAs[serdesOutcome](r)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// A step whose recorded result cannot be decoded fails in the invocation
// that ran it. The body runs once and the caller receives a catchable
// *SerdesError with direction "unmarshal".
func TestStepDeserializeFailureFailsOnce(t *testing.T) {
	var runs, invocations int64
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		atomic.AddInt64(&invocations, 1)
		return durable.Step(ctx, "s", func(_ durable.StepContext) (string, error) {
			atomic.AddInt64(&runs, 1)
			return "value", nil
		}, durable.WithStepSerdes(failingUnmarshalSerdes{}))
	}
	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", r.Status)
	}
	if r.Error == nil || r.Error.Type != "SerdesError" || !strings.Contains(r.Error.Message, "unmarshal") {
		t.Errorf("recorded error = %+v, want a SerdesError naming the unmarshal direction", r.Error)
	}
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Errorf("step body ran %d times, want 1", got)
	}
	if got := atomic.LoadInt64(&invocations); got != 1 {
		t.Errorf("handler ran in %d invocations, want 1", got)
	}

	// The same failure is catchable as a *SerdesError.
	caught := func(ctx durable.Context, _ struct{}) (serdesOutcome, error) {
		_, err := durable.Step(ctx, "s", func(_ durable.StepContext) (string, error) {
			return "value", nil
		}, durable.WithStepSerdes(failingUnmarshalSerdes{}))
		return describeSerdesError(err), nil
	}
	want := serdesOutcome{IsSerdesError: true, Direction: "unmarshal", Operation: "s"}
	if got := runToOutcome(t, caught, nil); got != want {
		t.Errorf("caught error = %+v, want %+v", got, want)
	}
}

// Every operation other than a step returns a permanent serdes failure to
// its caller as a *SerdesError, after running its body or check once.
func TestOperationSerdesFailureIsCatchableAndNotRetried(t *testing.T) {
	cases := []struct {
		name     string
		want     serdesOutcome
		register func(*durabletest.LocalRunner[struct{}, serdesOutcome])
		run      func(ctx durable.Context, runs *int64) error
	}{
		{
			name: "invoke input",
			want: serdesOutcome{IsSerdesError: true, Direction: "marshal", Operation: "call"},
			run: func(ctx durable.Context, runs *int64) error {
				atomic.AddInt64(runs, 1)
				_, err := durable.Invoke[string](ctx, "call", "target:$LATEST", "in",
					durable.WithInvokePayloadSerdes(failingMarshalSerdes{}))
				return err
			},
		},
		{
			name: "invoke result",
			want: serdesOutcome{IsSerdesError: true, Direction: "unmarshal", Operation: "call"},
			register: func(r *durabletest.LocalRunner[struct{}, serdesOutcome]) {
				r.RegisterFunction("target:$LATEST", durabletest.PlainFunction(func(_ context.Context, _ string) (string, error) {
					return "out", nil
				}))
			},
			run: func(ctx durable.Context, runs *int64) error {
				_, err := durable.Invoke[string](ctx, "call", "target:$LATEST", "in",
					durable.WithInvokeResultSerdes(failingUnmarshalSerdes{}))
				// The result is decoded once, in the invocation after
				// the target completed.
				if describeSerdesError(err).IsSerdesError {
					atomic.AddInt64(runs, 1)
				}
				return err
			},
		},
		{
			name: "child context",
			want: serdesOutcome{IsSerdesError: true, Direction: "marshal", Operation: "child"},
			run: func(ctx durable.Context, runs *int64) error {
				_, err := durable.RunInChildContext(ctx, "child", func(_ durable.Context) (string, error) {
					atomic.AddInt64(runs, 1)
					return "v", nil
				}, durable.WithChildSerdes(failingMarshalSerdes{}))
				return err
			},
		},
		{
			name: "map item",
			want: serdesOutcome{IsSerdesError: true, Direction: "marshal"},
			run: func(ctx durable.Context, runs *int64) error {
				_, err := durable.Map(ctx, "map", []string{"a"}, func(_ durable.Context, item string, _ int) (string, error) {
					atomic.AddInt64(runs, 1)
					return item, nil
				}, durable.WithBatchSerdes(failingMarshalSerdes{}))
				return err
			},
		},
		{
			name: "parallel branch",
			want: serdesOutcome{IsSerdesError: true, Direction: "marshal"},
			run: func(ctx durable.Context, runs *int64) error {
				_, err := durable.Parallel(ctx, "par", []durable.Branch[string]{{
					Name: "b",
					Func: func(_ durable.Context) (string, error) {
						atomic.AddInt64(runs, 1)
						return "v", nil
					},
				}}, durable.WithBatchSerdes(failingMarshalSerdes{}))
				return err
			},
		},
		{
			name: "wait for condition",
			want: serdesOutcome{IsSerdesError: true, Direction: "marshal", Operation: "poll"},
			run: func(ctx durable.Context, runs *int64) error {
				_, err := durable.WaitForCondition(ctx, "poll", func(_ durable.StepContext, s int) (int, error) {
					atomic.AddInt64(runs, 1)
					return s + 1, nil
				}, durable.ConditionConfig[int]{
					Serdes: failingMarshalSerdes{},
					WaitStrategy: func(int, int) durable.WaitDecision {
						return durable.WaitDecision{Continue: false}
					},
				})
				return err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var runs int64
			h := func(ctx durable.Context, _ struct{}) (serdesOutcome, error) {
				return describeSerdesError(tc.run(ctx, &runs)), nil
			}
			got := runToOutcome(t, h, tc.register)
			if tc.want.Operation == "" {
				// A batch item's operation name is derived from the
				// item; only its presence is asserted.
				if got.Operation == "" {
					t.Errorf("SerdesError.Operation is empty")
				}
				got.Operation = ""
			}
			if got != tc.want {
				t.Errorf("error = %+v, want %+v", got, tc.want)
			}
			if n := atomic.LoadInt64(&runs); n != 1 {
				t.Errorf("body or check ran %d times, want 1", n)
			}
		})
	}
}

// A RetryableSerdesError from a step's Marshal ends the invocation without
// recording an outcome, even when the handler catches the error. The next
// invocation runs the body again under AtLeastOnce and completes.
func TestRetryableSerdesErrorEndsInvocation(t *testing.T) {
	var runs int64
	serdes := newTransientMarshalSerdes(1)
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		v, err := durable.Step(ctx, "s", func(_ durable.StepContext) (string, error) {
			atomic.AddInt64(&runs, 1)
			return "value", nil
		}, durable.WithStepSerdes(serdes))
		if err != nil {
			// The handler catches the error; the invocation must end
			// with it anyway.
			return "caught", nil
		}
		return v, nil
	}
	runner := durabletest.NewLocalRunner(h)

	_, err := runner.Run(struct{}{})
	if err == nil || !errors.Is(err, durable.ErrRetryableSerdes) {
		t.Fatalf("first invocation error = %v, want one matching ErrRetryableSerdes", err)
	}

	r, err := runner.RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error %+v)", r.Status, r.Error)
	}
	got, err := durabletest.ResultAs[string](r)
	if err != nil || got != "value" {
		t.Errorf("result = %q, %v; want \"value\"", got, err)
	}
	if n := atomic.LoadInt64(&runs); n != 2 {
		t.Errorf("step body ran %d times, want 2 (once per invocation)", n)
	}
}

// Under AtMostOnce the next invocation finds the attempt started
// and treats it as interrupted, applying the step retry strategy instead
// of running the body again.
func TestRetryableSerdesErrorAtMostOnceIsInterrupted(t *testing.T) {
	var runs int64
	serdes := newTransientMarshalSerdes(1)
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.Step(ctx, "s", func(_ durable.StepContext) (string, error) {
			atomic.AddInt64(&runs, 1)
			return "value", nil
		}, durable.WithStepSerdes(serdes),
			durable.WithSemantics(durable.AtMostOnce),
			durable.WithRetry(durable.NoRetry()))
	}
	runner := durabletest.NewLocalRunner(h)
	if _, err := runner.Run(struct{}{}); !errors.Is(err, durable.ErrRetryableSerdes) {
		t.Fatalf("first invocation error = %v, want one matching ErrRetryableSerdes", err)
	}
	r, err := runner.RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Failed || r.Error == nil || r.Error.Type != "StepError" {
		t.Fatalf("status = %s, error = %+v; want FAILED with a StepError", r.Status, r.Error)
	}
	if !strings.Contains(r.Error.Message, "interrupted") {
		t.Errorf("error message = %q, want the interrupted attempt", r.Error.Message)
	}
	if n := atomic.LoadInt64(&runs); n != 1 {
		t.Errorf("step body ran %d times, want 1", n)
	}
}

// A RetryableSerdesError in a Map item or a Parallel branch ends the
// invocation. It never becomes a failed item of the BatchResult.
func TestRetryableSerdesErrorInBatchEndsInvocation(t *testing.T) {
	cases := []struct {
		name string
		run  func(ctx durable.Context, s durable.Serdes) (durable.BatchResult[string], error)
	}{
		{
			name: "map",
			run: func(ctx durable.Context, s durable.Serdes) (durable.BatchResult[string], error) {
				return durable.Map(ctx, "map", []string{"a", "b"}, func(_ durable.Context, item string, _ int) (string, error) {
					return item, nil
				}, durable.WithBatchSerdes(s))
			},
		},
		{
			name: "parallel",
			run: func(ctx durable.Context, s durable.Serdes) (durable.BatchResult[string], error) {
				return durable.Parallel(ctx, "par", []durable.Branch[string]{
					{Name: "a", Func: func(_ durable.Context) (string, error) { return "a", nil }},
					{Name: "b", Func: func(_ durable.Context) (string, error) { return "b", nil }},
				}, durable.WithBatchSerdes(s))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			serdes := newTransientMarshalSerdes(1)
			var sawFailedItem atomic.Bool
			h := func(ctx durable.Context, _ struct{}) (int, error) {
				res, err := tc.run(ctx, serdes)
				if err != nil {
					return 0, err
				}
				if len(res.Failed()) > 0 {
					sawFailedItem.Store(true)
				}
				return len(res.Succeeded()), nil
			}
			runner := durabletest.NewLocalRunner(h)
			if _, err := runner.Run(struct{}{}); !errors.Is(err, durable.ErrRetryableSerdes) {
				t.Fatalf("first invocation error = %v, want one matching ErrRetryableSerdes", err)
			}
			r, err := runner.RunUntilComplete(struct{}{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := durabletest.ResultAs[int](r)
			if err != nil {
				t.Fatal(err)
			}
			if got != 2 {
				t.Errorf("succeeded items = %d, want 2", got)
			}
			if sawFailedItem.Load() {
				t.Errorf("a BatchResult reported a failed item for the retryable serdes failure")
			}
		})
	}
}

// RetryableSerdesError matches ErrRetryableSerdes and keeps its cause.
func TestRetryableSerdesErrorMatching(t *testing.T) {
	cause := errors.New("timed out")
	err := durable.RetryableSerdesError(cause)
	if !errors.Is(err, durable.ErrRetryableSerdes) {
		t.Errorf("errors.Is(err, ErrRetryableSerdes) = false")
	}
	if !errors.Is(err, cause) {
		t.Errorf("errors.Is(err, cause) = false")
	}
	if errors.Is(cause, durable.ErrRetryableSerdes) {
		t.Errorf("the plain cause matches ErrRetryableSerdes")
	}
}
