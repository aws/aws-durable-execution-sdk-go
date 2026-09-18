package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// flakyError is the error the attempt bodies below return. Its type name
// is what the SDK records as the escaping error's ErrorType.
type flakyError struct{ attempt int }

func (e *flakyError) Error() string { return fmt.Sprintf("attempt %d flaked", e.attempt) }

// retryEvery returns a strategy that retries with a fixed one-second delay
// until maxAttempts have been made, and records every RetryAttempt it saw
// in seen.
func retryEvery(maxAttempts int, seen *[]durable.RetryAttempt) durable.RetryStrategy {
	return func(a durable.RetryAttempt) durable.RetryDecision {
		*seen = append(*seen, a)
		if a.Attempt >= maxAttempts {
			return durable.RetryDecision{}
		}
		return durable.RetryDecision{Retry: true, Delay: time.Second}
	}
}

// requireOp fails the test unless the result holds an operation named name
// with the given type and status, and returns it.
func requireOp(t *testing.T, r *durabletest.TestResult, name, typ, status string) *durabletest.TestOperation {
	t.Helper()
	op := r.Operation(name)
	if op == nil {
		names := make([]string, 0, len(r.Operations))
		for _, o := range r.Operations {
			names = append(names, fmt.Sprintf("%s(%s/%s)", o.Name, o.Type, o.Status))
		}
		t.Fatalf("no operation named %q; have %v", name, names)
	}
	if op.Type != typ || op.Status != status {
		t.Fatalf("operation %q = %s/%s, want %s/%s", name, op.Type, op.Status, typ, status)
	}
	return op
}

// TestRetrySucceedsOnLaterAttempt runs a group whose first attempt fails and
// second succeeds. The backoff suspends the execution: the first invocation
// ends PENDING with the failed attempt and the started wait recorded, and
// the second invocation runs attempt 2 and completes.
func TestRetrySucceedsOnLaterAttempt(t *testing.T) {
	var attempts []int
	var seen []durable.RetryAttempt
	handler := func(ctx durable.Context, _ string) (string, error) {
		return durable.Retry(ctx, "charge", func(c durable.Context, attempt int) (string, error) {
			attempts = append(attempts, attempt)
			return durable.Step(c, "call", func(durable.StepContext) (string, error) {
				if attempt == 1 {
					return "", &flakyError{attempt: attempt}
				}
				return fmt.Sprintf("charged on attempt %d", attempt), nil
			}, durable.WithRetry(durable.NoRetry()))
		}, retryEvery(3, &seen))
	}

	runner := durabletest.NewLocalRunner(handler)
	first := runner.Run(t, "x")
	if first.Status != durabletest.Pending {
		t.Fatalf("first invocation status = %s, want PENDING; error = %+v", first.Status, first.Error)
	}
	requireOp(t, first, "charge-attempt-1", "CONTEXT", "FAILED")
	requireOp(t, first, "charge-backoff-1", "WAIT", "STARTED")
	if len(seen) != 1 || seen[0].Attempt != 1 {
		t.Fatalf("strategy saw %+v, want one attempt numbered 1", seen)
	}
	var childErr *durable.ChildContextError
	if !errors.As(seen[0].Err, &childErr) {
		t.Fatalf("strategy Err = %T, want *ChildContextError", seen[0].Err)
	}
	if childErr.Name != "charge-attempt-1" || childErr.ErrorType != "StepError" {
		t.Errorf("ChildContextError = {Name %q, ErrorType %q}, want {charge-attempt-1, StepError}", childErr.Name, childErr.ErrorType)
	}

	if !runner.CompletePendingTimers() {
		t.Fatal("no pending backoff wait to complete")
	}
	second := runner.Run(t, "x")
	if second.Status != durabletest.Succeeded {
		t.Fatalf("second invocation status = %s, want SUCCEEDED; error = %+v", second.Status, second.Error)
	}
	out, err := durabletest.ResultAs[string](second)
	if err != nil {
		t.Fatal(err)
	}
	if out != "charged on attempt 2" {
		t.Errorf("result = %q, want %q", out, "charged on attempt 2")
	}
	requireOp(t, second, "charge-attempt-2", "CONTEXT", "SUCCEEDED")
	// Attempt 1 ran on the first invocation and was replayed from its
	// record on the second; attempt 2 ran once. fn never re-ran attempt 1.
	if want := []int{1, 2}; fmt.Sprint(attempts) != fmt.Sprint(want) {
		t.Errorf("attempt numbers fn saw = %v, want %v", attempts, want)
	}
	// Replay hands the strategy the same recorded failure again.
	if len(seen) != 2 || seen[1].Attempt != 1 || seen[1].Err.Error() != seen[0].Err.Error() {
		t.Errorf("strategy calls = %+v, want the attempt-1 failure twice", seen)
	}
}

// TestRetryExhaustionReturnsRetryError runs a group whose attempts always
// fail. When the strategy stops, Retry returns a *RetryError carrying the
// attempt count and the escaping error's type.
func TestRetryExhaustionReturnsRetryError(t *testing.T) {
	var retryErr *durable.RetryError
	var seen []durable.RetryAttempt
	handler := func(ctx durable.Context, _ string) (string, error) {
		_, err := durable.Retry(ctx, "charge", func(c durable.Context, attempt int) (string, error) {
			return "", &flakyError{attempt: attempt}
		}, retryEvery(2, &seen))
		if !errors.As(err, &retryErr) {
			return "", fmt.Errorf("Retry returned %T, want *RetryError: %w", err, err)
		}
		return "", err
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "x")
	if result.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", result.Status)
	}
	if result.Error == nil || result.Error.Type != "RetryError" {
		t.Fatalf("recorded error = %+v, want type RetryError", result.Error)
	}
	if retryErr.Name != "charge" || retryErr.Attempts != 2 {
		t.Errorf("RetryError = {Name %q, Attempts %d}, want {charge, 2}", retryErr.Name, retryErr.Attempts)
	}
	if retryErr.ErrorType != "flakyError" || retryErr.Message != "attempt 2 flaked" {
		t.Errorf("RetryError = {ErrorType %q, Message %q}, want {flakyError, attempt 2 flaked}", retryErr.ErrorType, retryErr.Message)
	}
	var childErr *durable.ChildContextError
	if !errors.As(retryErr, &childErr) || childErr.Name != "charge-attempt-2" {
		t.Errorf("RetryError.Err = %v, want the attempt-2 ChildContextError", retryErr.Err)
	}
	var opErr *durable.OperationError
	if !errors.As(retryErr, &opErr) || opErr.Name != "charge" || opErr.ErrorType != "flakyError" {
		t.Errorf("OperationError view = %+v", opErr)
	}
	want := `durable: retry "charge" failed after 2 attempts: flakyError: attempt 2 flaked`
	if retryErr.Error() != want {
		t.Errorf("Error() = %q, want %q", retryErr.Error(), want)
	}
	if !strings.Contains(result.Error.Message, want) {
		t.Errorf("recorded message = %q, want it to contain %q", result.Error.Message, want)
	}
	requireOp(t, result, "charge-attempt-1", "CONTEXT", "FAILED")
	requireOp(t, result, "charge-backoff-1", "WAIT", "SUCCEEDED")
	requireOp(t, result, "charge-attempt-2", "CONTEXT", "FAILED")
	if result.Operation("charge-backoff-2") != nil {
		t.Error("a backoff wait was recorded after the final attempt")
	}
}

// TestRetrySuspensionInsideAttemptIsNotAFailure runs a group whose first
// attempt waits. The wait suspends the execution; the strategy is not
// consulted, no backoff is recorded, and the attempt resumes on the next
// invocation and succeeds.
func TestRetrySuspensionInsideAttemptIsNotAFailure(t *testing.T) {
	var fnRuns atomic.Int32
	var seen []durable.RetryAttempt
	handler := func(ctx durable.Context, _ string) (string, error) {
		return durable.Retry(ctx, "approval", func(c durable.Context, attempt int) (string, error) {
			fnRuns.Add(1)
			if err := durable.Wait(c, "settle", time.Second); err != nil {
				return "", err
			}
			return fmt.Sprintf("approved on attempt %d", attempt), nil
		}, retryEvery(3, &seen))
	}

	runner := durabletest.NewLocalRunner(handler)
	first := runner.Run(t, "x")
	if first.Status != durabletest.Pending {
		t.Fatalf("first invocation status = %s, want PENDING; error = %+v", first.Status, first.Error)
	}
	requireOp(t, first, "approval-attempt-1", "CONTEXT", "STARTED")
	requireOp(t, first, "settle", "WAIT", "STARTED")
	if first.Operation("approval-backoff-1") != nil {
		t.Error("suspension recorded a backoff wait")
	}
	if len(seen) != 0 {
		t.Fatalf("strategy saw %+v during suspension, want no calls", seen)
	}

	if !runner.CompletePendingTimers() {
		t.Fatal("no pending wait to complete")
	}
	second := runner.Run(t, "x")
	if second.Status != durabletest.Succeeded {
		t.Fatalf("second invocation status = %s, want SUCCEEDED; error = %+v", second.Status, second.Error)
	}
	out, err := durabletest.ResultAs[string](second)
	if err != nil {
		t.Fatal(err)
	}
	if out != "approved on attempt 1" {
		t.Errorf("result = %q, want %q", out, "approved on attempt 1")
	}
	if got := fnRuns.Load(); got != 2 {
		t.Errorf("fn ran %d times, want 2 (one per invocation, both as attempt 1)", got)
	}
	if len(seen) != 0 {
		t.Errorf("strategy saw %+v, want no calls", seen)
	}
}

// TestRetryReplayOfCompletedGroupDoesNotReExecute completes a group over
// two invocations, then suspends after it. The third invocation replays the
// whole group from its records: fn does not run and the same result is
// returned.
func TestRetryReplayOfCompletedGroupDoesNotReExecute(t *testing.T) {
	var fnRuns atomic.Int32
	var seen []durable.RetryAttempt
	handler := func(ctx durable.Context, _ string) (string, error) {
		out, err := durable.Retry(ctx, "charge", func(c durable.Context, attempt int) (string, error) {
			fnRuns.Add(1)
			if attempt == 1 {
				return "", &flakyError{attempt: attempt}
			}
			return "charged", nil
		}, retryEvery(3, &seen))
		if err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "after", time.Second); err != nil {
			return "", err
		}
		return out, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	if r := runner.Run(t, "x"); r.Status != durabletest.Pending {
		t.Fatalf("first invocation status = %s, want PENDING; error = %+v", r.Status, r.Error)
	}
	runner.CompletePendingTimers()
	second := runner.Run(t, "x")
	if second.Status != durabletest.Pending {
		t.Fatalf("second invocation status = %s, want PENDING; error = %+v", second.Status, second.Error)
	}
	requireOp(t, second, "charge-attempt-2", "CONTEXT", "SUCCEEDED")
	requireOp(t, second, "after", "WAIT", "STARTED")
	runsBefore := fnRuns.Load()
	if runsBefore != 2 {
		t.Fatalf("fn ran %d times over two invocations, want 2", runsBefore)
	}
	strategyCallsBefore := len(seen)

	runner.CompletePendingTimers()
	third := runner.Run(t, "x")
	if third.Status != durabletest.Succeeded {
		t.Fatalf("third invocation status = %s, want SUCCEEDED; error = %+v", third.Status, third.Error)
	}
	out, err := durabletest.ResultAs[string](third)
	if err != nil {
		t.Fatal(err)
	}
	if out != "charged" {
		t.Errorf("result = %q, want charged", out)
	}
	if got := fnRuns.Load(); got != runsBefore {
		t.Errorf("fn ran %d more times on replay, want 0", got-runsBefore)
	}
	// The recorded attempt-1 failure is replayed through the strategy so
	// the backoff wait is reached at the same position.
	if len(seen) != strategyCallsBefore+1 {
		t.Errorf("strategy calls on replay = %d, want 1", len(seen)-strategyCallsBefore)
	}
}

// TestRetryWithoutChildContext runs attempts directly in the caller's
// context. The strategy receives the error fn returned, the attempt's
// operations are recorded at the top level, and no attempt context exists.
func TestRetryWithoutChildContext(t *testing.T) {
	sentinel := errors.New("not yet")
	var seen []durable.RetryAttempt
	var retryErr *durable.RetryError
	handler := func(ctx durable.Context, _ string) (string, error) {
		_, err := durable.Retry(ctx, "poll", func(c durable.Context, attempt int) (string, error) {
			_, err := durable.Step(c, fmt.Sprintf("probe-%d", attempt), func(durable.StepContext) (string, error) {
				return "probed", nil
			})
			if err != nil {
				return "", err
			}
			return "", fmt.Errorf("attempt %d: %w", attempt, sentinel)
		}, retryEvery(2, &seen), durable.WithAttemptChildContext(false))
		if !errors.As(err, &retryErr) {
			return "", fmt.Errorf("Retry returned %T, want *RetryError: %w", err, err)
		}
		return "", err
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "x")
	if result.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED; error = %+v", result.Status, result.Error)
	}
	if len(seen) < 1 || !errors.Is(seen[0].Err, sentinel) {
		t.Fatalf("strategy Err = %v, want the error fn returned", seen)
	}
	if !errors.Is(retryErr, sentinel) || retryErr.Attempts != 2 || retryErr.ErrorType != "Error" {
		t.Errorf("RetryError = %+v, want Attempts 2, ErrorType Error, unwrapping to sentinel", retryErr)
	}
	probe := requireOp(t, result, "probe-1", "STEP", "SUCCEEDED")
	if probe.ParentID != "" {
		t.Errorf("probe-1 ParentID = %q, want top level", probe.ParentID)
	}
	requireOp(t, result, "poll-backoff-1", "WAIT", "SUCCEEDED")
	requireOp(t, result, "probe-2", "STEP", "SUCCEEDED")
	for _, op := range result.Operations {
		if op.Type == "CONTEXT" {
			t.Errorf("attempt context %q recorded with WithAttemptChildContext(false)", op.Name)
		}
	}
}

// upperCaseSerdes serializes as JSON in upper case and reads JSON back. It
// is observable through the checkpointed attempt result.
type upperCaseSerdes struct{}

func (upperCaseSerdes) Marshal(_ context.Context, _ durable.SerdesContext, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	return []byte(strings.ToUpper(string(b))), err
}

func (upperCaseSerdes) Unmarshal(_ context.Context, _ durable.SerdesContext, data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// TestRetryAttemptChildOptionsApply forwards a child serdes to the
// per-attempt child context, and checks that a child error mapper's result
// is what the strategy and the RetryError see.
func TestRetryAttemptChildOptionsApply(t *testing.T) {
	t.Run("serdes", func(t *testing.T) {
		handler := func(ctx durable.Context, _ string) (string, error) {
			return durable.Retry(ctx, "fetch", func(durable.Context, int) (string, error) {
				return "lower", nil
			}, durable.NoRetry(), durable.WithAttemptChildOptions(durable.WithChildSerdes(upperCaseSerdes{})))
		}
		result := durabletest.NewLocalRunner(handler).RunUntilComplete(t, "x")
		if result.Status != durabletest.Succeeded {
			t.Fatalf("status = %s, want SUCCEEDED; error = %+v", result.Status, result.Error)
		}
		out, err := durabletest.ResultAs[string](result)
		if err != nil {
			t.Fatal(err)
		}
		if out != "LOWER" {
			t.Errorf("result = %q, want LOWER (round-tripped through the child serdes)", out)
		}
		op := requireOp(t, result, "fetch-attempt-1", "CONTEXT", "SUCCEEDED")
		if op.ContextDetails == nil || op.ContextDetails.Result != `"LOWER"` {
			t.Errorf("checkpointed attempt result = %+v, want \"LOWER\"", op.ContextDetails)
		}
	})

	t.Run("error mapper", func(t *testing.T) {
		mapped := errors.New("mapped")
		var seen []durable.RetryAttempt
		var retryErr *durable.RetryError
		handler := func(ctx durable.Context, _ string) (string, error) {
			_, err := durable.Retry(ctx, "fetch", func(durable.Context, int) (string, error) {
				return "", &flakyError{attempt: 1}
			}, retryEvery(1, &seen), durable.WithAttemptChildOptions(
				durable.WithChildErrorMapper(func(*durable.ChildContextError) error { return mapped }),
			))
			errors.As(err, &retryErr)
			return "", err
		}
		result := durabletest.NewLocalRunner(handler).RunUntilComplete(t, "x")
		if result.Status != durabletest.Failed {
			t.Fatalf("status = %s, want FAILED", result.Status)
		}
		if len(seen) != 1 || !errors.Is(seen[0].Err, mapped) {
			t.Errorf("strategy Err = %v, want the mapped error", seen)
		}
		if retryErr == nil || !errors.Is(retryErr, mapped) || retryErr.ErrorType != "Error" || retryErr.Message != "mapped" {
			t.Errorf("RetryError = %+v, want the mapped error as cause", retryErr)
		}
	})
}

// TestRetryBackoffDelay checks the delay rules: a zero delay waits
// DefaultRetryDelay, a fractional delay rounds up, and a negative delay is
// an error before any wait is recorded.
func TestRetryBackoffDelay(t *testing.T) {
	run := func(t *testing.T, delay time.Duration) (*durabletest.TestResult, error) {
		t.Helper()
		var retryErr error
		handler := func(ctx durable.Context, _ string) (string, error) {
			_, err := durable.Retry(ctx, "g", func(c durable.Context, attempt int) (string, error) {
				return "", &flakyError{attempt: attempt}
			}, func(durable.RetryAttempt) durable.RetryDecision {
				return durable.RetryDecision{Retry: true, Delay: delay}
			})
			retryErr = err
			return "", err
		}
		return durabletest.NewLocalRunner(handler).Run(t, "x"), retryErr
	}

	t.Run("zero selects default", func(t *testing.T) {
		result, _ := run(t, 0)
		if result.Status != durabletest.Pending {
			t.Fatalf("status = %s, want PENDING; error = %+v", result.Status, result.Error)
		}
		requireOp(t, result, "g-backoff-1", "WAIT", "STARTED")
	})

	t.Run("negative is an error", func(t *testing.T) {
		result, err := run(t, -time.Second)
		if result.Status != durabletest.Failed {
			t.Fatalf("status = %s, want FAILED", result.Status)
		}
		if err == nil || !strings.Contains(err.Error(), `durable: Retry "g": retry delay:`) {
			t.Errorf("error = %v, want a retry delay error", err)
		}
		if result.Operation("g-backoff-1") != nil {
			t.Error("a backoff wait was recorded for a negative delay")
		}
	})
}

// TestRetryUnnamed records unnamed attempt and backoff operations when
// name is empty.
func TestRetryUnnamed(t *testing.T) {
	var seen []durable.RetryAttempt
	handler := func(ctx durable.Context, _ string) (string, error) {
		return durable.Retry(ctx, "", func(c durable.Context, attempt int) (string, error) {
			if attempt == 1 {
				return "", &flakyError{attempt: attempt}
			}
			return "ok", nil
		}, retryEvery(2, &seen))
	}
	result := durabletest.NewLocalRunner(handler).RunUntilComplete(t, "x")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED; error = %+v", result.Status, result.Error)
	}
	for _, op := range result.Operations {
		if op.Name != "" {
			t.Errorf("operation %s/%s named %q, want unnamed", op.Type, op.Status, op.Name)
		}
	}
	if len(result.Operations) != 3 {
		t.Errorf("recorded %d operations, want 3 (attempt, backoff, attempt)", len(result.Operations))
	}
	var childErr *durable.ChildContextError
	if len(seen) == 0 || !errors.As(seen[0].Err, &childErr) || childErr.Name != "" {
		t.Errorf("strategy Err = %v, want an unnamed ChildContextError", seen)
	}
}

// TestRetryRejectsInvalidArguments returns an error, without recording an
// operation, for a nil strategy, a nil fn, or a Context the SDK did not
// create.
func TestRetryRejectsInvalidArguments(t *testing.T) {
	noop := func(durable.Context, int) (string, error) { return "", nil }

	t.Run("nil strategy", func(t *testing.T) {
		var got error
		handler := func(ctx durable.Context, _ string) (string, error) {
			_, got = durable.Retry(ctx, "g", noop, nil)
			return "", got
		}
		result := durabletest.NewLocalRunner(handler).Run(t, "x")
		if result.Status != durabletest.Failed || len(result.Operations) != 0 {
			t.Fatalf("status = %s with %d operations, want FAILED with none", result.Status, len(result.Operations))
		}
		if got == nil || !strings.Contains(got.Error(), "strategy must not be nil") {
			t.Errorf("error = %v, want a nil strategy error", got)
		}
	})

	t.Run("nil fn", func(t *testing.T) {
		var got error
		handler := func(ctx durable.Context, _ string) (string, error) {
			_, got = durable.Retry[string](ctx, "g", nil, durable.NoRetry())
			return "", got
		}
		result := durabletest.NewLocalRunner(handler).Run(t, "x")
		if result.Status != durabletest.Failed || len(result.Operations) != 0 {
			t.Fatalf("status = %s with %d operations, want FAILED with none", result.Status, len(result.Operations))
		}
		if got == nil || !strings.Contains(got.Error(), "fn must not be nil") {
			t.Errorf("error = %v, want a nil fn error", got)
		}
	})

	t.Run("foreign context", func(t *testing.T) {
		_, err := durable.Retry(nil, "g", noop, durable.NoRetry())
		if err == nil || !strings.Contains(err.Error(), "Context was not created by the SDK") {
			t.Errorf("error = %v, want a foreign context error", err)
		}
	})
}
