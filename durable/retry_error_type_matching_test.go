package durable_test

// Tests for error matching inside durable.Retry (issue #109).
//
// A plain Step consults its retry strategy with the live error its body
// returned, so a RetryableErrors matcher built with ErrorAs or ErrorIs
// matches the caller's own error type and sentinel.
//
// durable.Retry runs each attempt in its own child context. A child context
// records only the wire ErrorType string and the message, so a failed
// attempt reaches the strategy as a reconstructed *durable.ChildContextError,
// on the first invocation and on replay alike. ErrorAs[*T]() and
// ErrorIs(sentinel) therefore do not match a failed attempt's error type
// inside Retry, while ErrorTypeIs and ErrorContains do.

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// errCardDeclined is a sentinel. cardDeclinedError wraps it, so one value
// matches both errors.As(&*cardDeclinedError) and errors.Is(errCardDeclined),
// and its message contains "card declined".
var errCardDeclined = errors.New("card declined")

type cardDeclinedError struct{}

func (*cardDeclinedError) Error() string { return errCardDeclined.Error() }
func (*cardDeclinedError) Unwrap() error { return errCardDeclined }

type otherError struct{}

func (*otherError) Error() string { return "other" }

// matcherObservation records what one retry-strategy call inside Retry saw.
type matcherObservation struct {
	replaying   bool
	concreteTyp string
	errorType   string // *OperationError.ErrorType, "" when not an OperationError
	errorAs     bool   // ErrorAs[*cardDeclinedError]()
	errorIs     bool   // ErrorIs(errCardDeclined)
	errContains bool   // ErrorContains("card declined")
	errorTypeIs bool   // ErrorTypeIs("cardDeclinedError")
}

func TestRetryErrorTypeMatchingGap(t *testing.T) {
	t.Run("inside Retry the strategy sees ChildContextError on both paths", func(t *testing.T) {
		var obs []matcherObservation
		handler := func(ctx durable.Context, _ struct{}) (int, error) {
			strategy := func(a durable.RetryAttempt) durable.RetryDecision {
				var oe *durable.OperationError
				et := ""
				if errors.As(a.Err, &oe) {
					et = oe.ErrorType
				}
				obs = append(obs, matcherObservation{
					replaying:   ctx.IsReplaying(),
					concreteTyp: fmt.Sprintf("%T", a.Err),
					errorType:   et,
					errorAs:     durable.ErrorAs[*cardDeclinedError]()(a.Err),
					errorIs:     durable.ErrorIs(errCardDeclined)(a.Err),
					errContains: durable.ErrorContains("card declined")(a.Err),
					errorTypeIs: durable.ErrorTypeIs("cardDeclinedError")(a.Err),
				})
				if a.Attempt >= 3 {
					return durable.RetryDecision{}
				}
				return durable.RetryDecision{Retry: true, Delay: time.Second}
			}
			_, err := durable.Retry(ctx, "grp", func(child durable.Context, _ int) (int, error) {
				return 0, &cardDeclinedError{}
			}, strategy)
			var re *durable.RetryError
			if errors.As(err, &re) {
				return re.Attempts, nil
			}
			return -1, err
		}

		r, runErr := durabletest.NewLocalRunner(handler).RunUntilComplete(struct{}{})
		if runErr != nil {
			t.Fatal(runErr)
		}
		if r.Status != durabletest.Succeeded {
			t.Fatalf("status=%s", r.Status)
		}
		attempts, err := durabletest.ResultAs[int](r)
		if err != nil {
			t.Fatal(err)
		}
		if attempts != 3 {
			t.Fatalf("RetryError.Attempts=%d, want 3", attempts)
		}
		sawLive, sawReplay := false, false
		for i, o := range obs {
			t.Logf("strategy call %d: %+v", i, o)
			if o.concreteTyp != "*durable.ChildContextError" {
				t.Errorf("call %d: concrete type=%s, want *durable.ChildContextError", i, o.concreteTyp)
			}
			if o.errorAs {
				t.Errorf("call %d: ErrorAs[*cardDeclinedError]() matched inside Retry, want no match", i)
			}
			if o.errorIs {
				t.Errorf("call %d: ErrorIs(errCardDeclined) matched inside Retry, want no match", i)
			}
			if !o.errContains {
				t.Errorf("call %d: ErrorContains(\"card declined\")=false, want true", i)
			}
			if !o.errorTypeIs {
				t.Errorf("call %d: ErrorTypeIs(\"cardDeclinedError\")=false, want true", i)
			}
			if o.errorType != "cardDeclinedError" {
				t.Errorf("call %d: recorded ErrorType=%q, want cardDeclinedError", i, o.errorType)
			}
			if o.replaying {
				sawReplay = true
			} else {
				sawLive = true
			}
		}
		if !sawLive || !sawReplay {
			t.Fatalf("want the strategy observed on both the live path and the replay path; live=%v replay=%v", sawLive, sawReplay)
		}
	})

	t.Run("a plain Step matches the same ErrorAs and ErrorIs", func(t *testing.T) {
		attemptsWith := func(t *testing.T, matchers []durable.ErrorMatcher) int {
			t.Helper()
			handler := func(ctx durable.Context, _ struct{}) (int, error) {
				_, err := durable.Step(ctx, "charge", func(durable.StepContext) (int, error) {
					return 0, &cardDeclinedError{}
				}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
					MaxAttempts:     3,
					InitialDelay:    time.Second,
					Jitter:          durable.JitterNone,
					RetryableErrors: matchers,
				})))
				var se *durable.StepError
				if errors.As(err, &se) {
					return se.Attempts, nil
				}
				return -1, err
			}
			r, runErr := durabletest.NewLocalRunner(handler).RunUntilComplete(struct{}{})
			if runErr != nil {
				t.Fatal(runErr)
			}
			if r.Status != durabletest.Succeeded {
				t.Fatalf("status=%s", r.Status)
			}
			n, err := durabletest.ResultAs[int](r)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}

		if n := attemptsWith(t, []durable.ErrorMatcher{durable.ErrorAs[*cardDeclinedError]()}); n != 3 {
			t.Errorf("ErrorAs on a plain Step: attempts=%d, want 3 (matched the live error)", n)
		}
		if n := attemptsWith(t, []durable.ErrorMatcher{durable.ErrorIs(errCardDeclined)}); n != 3 {
			t.Errorf("ErrorIs on a plain Step: attempts=%d, want 3 (matched the live error)", n)
		}
		if n := attemptsWith(t, []durable.ErrorMatcher{durable.ErrorAs[*otherError]()}); n != 1 {
			t.Errorf("ErrorAs[*otherError] on a plain Step: attempts=%d, want 1 (no match)", n)
		}
		if n := attemptsWith(t, []durable.ErrorMatcher{durable.ErrorTypeIs("cardDeclinedError")}); n != 3 {
			t.Errorf("ErrorTypeIs on a plain Step: attempts=%d, want 3 (matched live wire type)", n)
		}
	})
}

func TestRetryErrorTypeIsMatchesInsideRetry(t *testing.T) {
	// ErrorTypeIs in RetryableErrors retries a default Retry attempt whose
	// recorded ErrorType matches, on the first invocation and on replay.
	var strategyCalls int
	handler := func(ctx durable.Context, _ struct{}) (int, error) {
		strategy := durable.MustNewRetryStrategy(durable.RetryConfig{
			MaxAttempts:     3,
			InitialDelay:    time.Second,
			Jitter:          durable.JitterNone,
			RetryableErrors: []durable.ErrorMatcher{durable.ErrorTypeIs("cardDeclinedError")},
		})
		wrapped := func(a durable.RetryAttempt) durable.RetryDecision {
			strategyCalls++
			return strategy(a)
		}
		_, err := durable.Retry(ctx, "grp", func(durable.Context, int) (int, error) {
			return 0, &cardDeclinedError{}
		}, wrapped)
		var re *durable.RetryError
		if errors.As(err, &re) {
			return re.Attempts, nil
		}
		return -1, err
	}

	r, runErr := durabletest.NewLocalRunner(handler).RunUntilComplete(struct{}{})
	if runErr != nil {
		t.Fatal(runErr)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status=%s error=%v", r.Status, r.Error)
	}
	attempts, err := durabletest.ResultAs[int](r)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("RetryError.Attempts=%d, want 3 (ErrorTypeIs matched on every attempt)", attempts)
	}
	if strategyCalls < 3 {
		t.Fatalf("strategyCalls=%d, want at least 3 (live + replay observations)", strategyCalls)
	}
}

func TestRetryErrorAsDoesNotMatchInsideRetry(t *testing.T) {
	// ErrorAs / ErrorIs in RetryableErrors do not retry inside a default Retry,
	// even when the live type would match a plain Step.
	for _, tc := range []struct {
		name    string
		matcher durable.ErrorMatcher
	}{
		{"ErrorAs", durable.ErrorAs[*cardDeclinedError]()},
		{"ErrorIs", durable.ErrorIs(errCardDeclined)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := func(ctx durable.Context, _ struct{}) (int, error) {
				_, err := durable.Retry(ctx, "grp", func(durable.Context, int) (int, error) {
					return 0, &cardDeclinedError{}
				}, durable.MustNewRetryStrategy(durable.RetryConfig{
					MaxAttempts:     3,
					InitialDelay:    time.Second,
					Jitter:          durable.JitterNone,
					RetryableErrors: []durable.ErrorMatcher{tc.matcher},
				}))
				var re *durable.RetryError
				if errors.As(err, &re) {
					return re.Attempts, nil
				}
				return -1, err
			}
			r, runErr := durabletest.NewLocalRunner(handler).RunUntilComplete(struct{}{})
			if runErr != nil {
				t.Fatal(runErr)
			}
			if r.Status != durabletest.Succeeded {
				t.Fatalf("status=%s", r.Status)
			}
			attempts, err := durabletest.ResultAs[int](r)
			if err != nil {
				t.Fatal(err)
			}
			if attempts != 1 {
				t.Fatalf("attempts=%d, want 1 (%s must not match ChildContextError)", attempts, tc.name)
			}
		})
	}
}

func TestErrorTypeIsUnit(t *testing.T) {
	live := &cardDeclinedError{}
	if !durable.ErrorTypeIs("cardDeclinedError")(live) {
		t.Fatal("ErrorTypeIs should match live cardDeclinedError via wireErrorType")
	}
	if durable.ErrorTypeIs("otherError")(live) {
		t.Fatal("ErrorTypeIs should not match a different type name")
	}

	child := &durable.ChildContextError{
		Name:      "grp-attempt-1",
		ErrorType: "cardDeclinedError",
		Message:   "card declined",
		Err:       errors.New("card declined"),
	}
	if !durable.ErrorTypeIs("cardDeclinedError")(child) {
		t.Fatal("ErrorTypeIs should match ChildContextError via OperationError.As")
	}
	if durable.ErrorTypeIs("otherError")(child) {
		t.Fatal("ErrorTypeIs should not match ChildContextError with a different ErrorType")
	}
}
