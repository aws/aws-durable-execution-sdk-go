package durable_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// attemptRecorder records the attempt hooks of every invocation. A wrap
// hook sleeps for wrapDelay after the body returns, so a test can check
// that the attempt end includes the time the wrap hooks spend.
type attemptRecorder struct {
	wrapDelay time.Duration

	mu     sync.Mutex
	starts []durable.AttemptHookInfo
	ends   []durable.AttemptEndHookInfo
}

func (r *attemptRecorder) factory() durable.PluginFactory {
	return durable.PluginFactoryFunc(func(context.Context, durable.InvocationHookInfo) (*durable.Plugin, error) {
		return &durable.Plugin{
			OnOperationAttemptStart: func(_ context.Context, info durable.AttemptHookInfo) {
				r.mu.Lock()
				defer r.mu.Unlock()
				r.starts = append(r.starts, info)
			},
			OnOperationAttemptEnd: func(_ context.Context, info durable.AttemptEndHookInfo) {
				r.mu.Lock()
				defer r.mu.Unlock()
				r.ends = append(r.ends, info)
			},
			WrapOperationAttemptFn: func(ctx context.Context, _ durable.AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
				res, err := fn(ctx)
				time.Sleep(r.wrapDelay)
				return res, err
			},
		}, nil
	})
}

// checkAttempts asserts that the recorder saw the attempts with the given
// outcomes, in order, and that each attempt-end info carries the attempt's
// own start time and an end time no earlier than the start plus the wrap
// delay.
func (r *attemptRecorder) checkAttempts(t *testing.T, outcomes ...durable.PluginAttemptOutcome) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.starts) != len(outcomes) || len(r.ends) != len(outcomes) {
		t.Fatalf("got %d attempt starts and %d attempt ends, want %d of each", len(r.starts), len(r.ends), len(outcomes))
	}
	for i, want := range outcomes {
		start, end := r.starts[i], r.ends[i]
		if end.Attempt != i+1 || end.Outcome != want {
			t.Errorf("attempt end %d: attempt %d outcome %s, want attempt %d outcome %s", i, end.Attempt, end.Outcome, i+1, want)
		}
		if !start.EndTimestamp.IsZero() {
			t.Errorf("attempt %d start: EndTimestamp = %v, want zero", i+1, start.EndTimestamp)
		}
		if !end.StartTimestamp.Equal(start.StartTimestamp) {
			t.Errorf("attempt %d end: StartTimestamp = %v, want the attempt start %v", i+1, end.StartTimestamp, start.StartTimestamp)
		}
		if end.EndTimestamp.IsZero() {
			t.Fatalf("attempt %d end: EndTimestamp is zero", i+1)
		}
		if got := end.EndTimestamp.Sub(end.StartTimestamp); got < r.wrapDelay {
			t.Errorf("attempt %d end: duration %v, want at least the wrap delay %v", i+1, got, r.wrapDelay)
		}
	}
}

const attemptWrapDelay = 20 * time.Millisecond

func noJitterRetry(maxAttempts int) durable.StepOption {
	return durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  maxAttempts,
		InitialDelay: time.Second,
		Jitter:       durable.JitterNone,
	}))
}

// TestAttemptEndTimestampRetriedStep runs a step that fails on attempt 1
// and succeeds on attempt 2. Both attempt-end infos carry an end time.
func TestAttemptEndTimestampRetriedStep(t *testing.T) {
	rec := &attemptRecorder{wrapDelay: attemptWrapDelay}
	runner := durabletest.NewLocalRunner(func(ctx durable.Context, _ any) (string, error) {
		return durable.Step(ctx, "flaky", func(sc durable.StepContext) (string, error) {
			if sc.Attempt() < 2 {
				return "", fmt.Errorf("attempt %d failed", sc.Attempt())
			}
			return "ok", nil
		}, noJitterRetry(3))
	}, durable.WithPlugins(rec.factory()))
	result, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	rec.checkAttempts(t, durable.PluginAttemptFailed, durable.PluginAttemptSucceeded)
}

// TestAttemptEndTimestampFinalFailure runs a step whose only attempt fails.
func TestAttemptEndTimestampFinalFailure(t *testing.T) {
	rec := &attemptRecorder{wrapDelay: attemptWrapDelay}
	runner := durabletest.NewLocalRunner(func(ctx durable.Context, _ any) (string, error) {
		return durable.Step(ctx, "fails", func(durable.StepContext) (string, error) {
			return "", errors.New("boom")
		}, noJitterRetry(1))
	}, durable.WithPlugins(rec.factory()))
	result, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", result.Status)
	}
	rec.checkAttempts(t, durable.PluginAttemptFailed)
}

// TestAttemptEndTimestampWaitForCondition runs a wait-for-condition whose
// check runs three times, then a second whose check fails.
func TestAttemptEndTimestampWaitForCondition(t *testing.T) {
	t.Run("succeeded checks", func(t *testing.T) {
		rec := &attemptRecorder{wrapDelay: attemptWrapDelay}
		runner := durabletest.NewLocalRunner(func(ctx durable.Context, _ any) (int, error) {
			return durable.WaitForCondition(ctx, "poll", func(_ durable.StepContext, state int) (int, error) {
				return state + 1, nil
			}, durable.ConditionConfig[int]{
				WaitStrategy: func(state int, _ int) durable.WaitDecision {
					return durable.WaitDecision{Continue: state < 3, Delay: time.Second}
				},
			})
		}, durable.WithPlugins(rec.factory()))
		result, err := runner.RunUntilComplete(nil)
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != durabletest.Succeeded {
			t.Fatalf("status = %s, want SUCCEEDED", result.Status)
		}
		rec.checkAttempts(t, durable.PluginAttemptSucceeded, durable.PluginAttemptSucceeded, durable.PluginAttemptSucceeded)
	})
	t.Run("failed check", func(t *testing.T) {
		rec := &attemptRecorder{wrapDelay: attemptWrapDelay}
		runner := durabletest.NewLocalRunner(func(ctx durable.Context, _ any) (int, error) {
			return durable.WaitForCondition(ctx, "poll", func(durable.StepContext, int) (int, error) {
				return 0, errors.New("check failed")
			}, durable.ConditionConfig[int]{
				WaitStrategy: func(int, int) durable.WaitDecision { return durable.WaitDecision{} },
			})
		}, durable.WithPlugins(rec.factory()))
		result, err := runner.RunUntilComplete(nil)
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != durabletest.Failed {
			t.Fatalf("status = %s, want FAILED", result.Status)
		}
		rec.checkAttempts(t, durable.PluginAttemptFailed)
	})
}
