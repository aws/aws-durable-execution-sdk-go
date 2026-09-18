package durable

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestNewRetryStrategyDeterministic(t *testing.T) {
	// Jitter NONE makes delays exact: initial × rate^(attempt-1), capped.
	strategy := MustNewRetryStrategy(RetryConfig{
		MaxAttempts:  5,
		InitialDelay: 2 * time.Second,
		MaxDelay:     20 * time.Second,
		BackoffRate:  3,
		Jitter:       JitterNone,
	})
	err := errors.New("x")

	tests := []struct {
		attempt   int
		wantRetry bool
		wantDelay time.Duration
	}{
		{1, true, 2 * time.Second},
		{2, true, 6 * time.Second},
		{3, true, 18 * time.Second},
		{4, true, 20 * time.Second}, // 54s capped at MaxDelay
		{5, false, 0},               // attempts exhausted
		{6, false, 0},
	}
	for _, tt := range tests {
		d := strategy(RetryAttempt{Err: err, Attempt: tt.attempt})
		if d.Retry != tt.wantRetry {
			t.Errorf("attempt %d: Retry = %v, want %v", tt.attempt, d.Retry, tt.wantRetry)
		}
		if tt.wantRetry && d.Delay != tt.wantDelay {
			t.Errorf("attempt %d: Delay = %v, want %v", tt.attempt, d.Delay, tt.wantDelay)
		}
	}
}

func TestNewRetryStrategyDefaults(t *testing.T) {
	// Zero-value config: 3 attempts, 5s initial, 2x backoff, full jitter.
	strategy := MustNewRetryStrategy(RetryConfig{Jitter: JitterNone})
	err := errors.New("x")

	if d := strategy(RetryAttempt{Err: err, Attempt: 1}); !d.Retry || d.Delay != 5*time.Second {
		t.Errorf("attempt 1 = %+v, want retry with 5s delay", d)
	}
	if d := strategy(RetryAttempt{Err: err, Attempt: 2}); !d.Retry || d.Delay != 10*time.Second {
		t.Errorf("attempt 2 = %+v, want retry with 10s delay", d)
	}
	if d := strategy(RetryAttempt{Err: err, Attempt: 3}); d.Retry {
		t.Errorf("attempt 3 = %+v, want no retry (default 3 max attempts)", d)
	}
}

func TestNewRetryStrategyValidConfigs(t *testing.T) {
	// Configs that must be accepted, including boundary cases that
	// validation deliberately does not reject.
	tests := []struct {
		name string
		cfg  RetryConfig
	}{
		{"zero value", RetryConfig{}},
		{"single attempt", RetryConfig{MaxAttempts: 1}},
		{"max delay below initial delay", RetryConfig{InitialDelay: 10 * time.Second, MaxDelay: 2 * time.Second}},
		{"fractional backoff rate", RetryConfig{BackoffRate: 0.5}},
		{"one second delays", RetryConfig{InitialDelay: time.Second, MaxDelay: time.Second}},
		{"all jitter constants", RetryConfig{Jitter: JitterHalf}},
		{"jitter none", RetryConfig{Jitter: JitterNone}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, err := NewRetryStrategy(tt.cfg)
			if err != nil {
				t.Fatalf("NewRetryStrategy(%+v) error = %v, want nil", tt.cfg, err)
			}
			if strategy == nil {
				t.Fatal("strategy is nil")
			}
		})
	}
}

func TestNewRetryStrategyInvalidConfig(t *testing.T) {
	tests := []struct {
		name       string
		cfg        RetryConfig
		wantFields []string
	}{
		{"negative max attempts", RetryConfig{MaxAttempts: -1}, []string{"MaxAttempts"}},
		{"sub-second initial delay", RetryConfig{InitialDelay: 500 * time.Millisecond}, []string{"InitialDelay"}},
		{"negative initial delay", RetryConfig{InitialDelay: -time.Second}, []string{"InitialDelay"}},
		{"sub-second max delay", RetryConfig{MaxDelay: time.Millisecond}, []string{"MaxDelay"}},
		{"negative max delay", RetryConfig{MaxDelay: -time.Minute}, []string{"MaxDelay"}},
		{"negative backoff rate", RetryConfig{BackoffRate: -1}, []string{"BackoffRate"}},
		{"NaN backoff rate", RetryConfig{BackoffRate: math.NaN()}, []string{"BackoffRate"}},
		{"positive infinite backoff rate", RetryConfig{BackoffRate: math.Inf(1)}, []string{"BackoffRate"}},
		{"negative infinite backoff rate", RetryConfig{BackoffRate: math.Inf(-1)}, []string{"BackoffRate"}},
		{"undefined jitter", RetryConfig{Jitter: "BOGUS"}, []string{"Jitter"}},
		{
			"multiple invalid fields",
			RetryConfig{MaxAttempts: -3, InitialDelay: -time.Second, MaxDelay: 10 * time.Millisecond, BackoffRate: -0.5, Jitter: "??"},
			[]string{"MaxAttempts", "InitialDelay", "MaxDelay", "BackoffRate", "Jitter"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, err := NewRetryStrategy(tt.cfg)
			if err == nil {
				t.Fatalf("NewRetryStrategy(%+v) error = nil, want error", tt.cfg)
			}
			if strategy != nil {
				t.Error("strategy is non-nil, want nil on invalid config")
			}
			for _, field := range tt.wantFields {
				if !strings.Contains(err.Error(), field) {
					t.Errorf("error %q does not name field %s", err, field)
				}
			}
		})
	}
}

func TestMustNewRetryStrategyPanicsOnInvalidConfig(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustNewRetryStrategy did not panic on invalid config")
		}
	}()
	MustNewRetryStrategy(RetryConfig{MaxAttempts: -1})
}

// TestMustNewRetryStrategyPanicsOnNaNBackoffRate pins that a non-finite
// BackoffRate is a construction-time error: it must never reach the delay
// computation, where NaN would violate the whole-second, at-least-one
// delay guarantee.
func TestMustNewRetryStrategyPanicsOnNaNBackoffRate(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustNewRetryStrategy did not panic on NaN BackoffRate")
		}
	}()
	MustNewRetryStrategy(RetryConfig{BackoffRate: math.NaN()})
}

func TestMustNewRetryStrategyValidConfig(t *testing.T) {
	strategy := MustNewRetryStrategy(RetryConfig{MaxAttempts: 2, Jitter: JitterNone})
	if d := strategy(RetryAttempt{Err: errors.New("x"), Attempt: 1}); !d.Retry {
		t.Errorf("attempt 1 = %+v, want retry", d)
	}
}

func TestNewRetryStrategyScheduleUnchanged(t *testing.T) {
	// Every previously-valid config must produce an identical delay
	// schedule to the pre-validation implementation. Expected delays are
	// hard-coded from the formula: min(initial × rate^(n-1), max),
	// rounded to whole seconds, no less than one (jitter NONE for
	// determinism).
	err := errors.New("x")
	tests := []struct {
		name   string
		cfg    RetryConfig
		want   []time.Duration // delays for attempts 1..len(want)
		noMore int             // first attempt with no retry
	}{
		{
			"defaults",
			RetryConfig{Jitter: JitterNone},
			[]time.Duration{5 * time.Second, 10 * time.Second},
			3,
		},
		{
			"capped growth",
			RetryConfig{MaxAttempts: 5, InitialDelay: 2 * time.Second, MaxDelay: 20 * time.Second, BackoffRate: 3, Jitter: JitterNone},
			[]time.Duration{2 * time.Second, 6 * time.Second, 18 * time.Second, 20 * time.Second},
			5,
		},
		{
			"max delay below initial delay",
			RetryConfig{MaxAttempts: 3, InitialDelay: 10 * time.Second, MaxDelay: 2 * time.Second, Jitter: JitterNone},
			[]time.Duration{2 * time.Second, 2 * time.Second},
			3,
		},
		{
			"single attempt",
			RetryConfig{MaxAttempts: 1, Jitter: JitterNone},
			nil,
			1,
		},
		{
			"flat rate one",
			RetryConfig{MaxAttempts: 4, InitialDelay: 3 * time.Second, BackoffRate: 1, Jitter: JitterNone},
			[]time.Duration{3 * time.Second, 3 * time.Second, 3 * time.Second},
			4,
		},
		{
			"decaying rate floors at one second",
			RetryConfig{MaxAttempts: 4, InitialDelay: 4 * time.Second, BackoffRate: 0.5, Jitter: JitterNone},
			[]time.Duration{4 * time.Second, 2 * time.Second, time.Second},
			4,
		},
		{
			"fractional initial delay rounds",
			RetryConfig{MaxAttempts: 3, InitialDelay: 1500 * time.Millisecond, Jitter: JitterNone},
			[]time.Duration{2 * time.Second, 3 * time.Second},
			3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, cerr := NewRetryStrategy(tt.cfg)
			if cerr != nil {
				t.Fatalf("NewRetryStrategy(%+v) error = %v, want nil", tt.cfg, cerr)
			}
			for i, want := range tt.want {
				d := strategy(RetryAttempt{Err: err, Attempt: i + 1})
				if !d.Retry || d.Delay != want {
					t.Errorf("attempt %d = %+v, want retry with %v delay", i+1, d, want)
				}
			}
			if d := strategy(RetryAttempt{Err: err, Attempt: tt.noMore}); d.Retry {
				t.Errorf("attempt %d = %+v, want retries exhausted", tt.noMore, d)
			}
		})
	}
}

func TestNewRetryStrategyFullJitterBounds(t *testing.T) {
	strategy := MustNewRetryStrategy(RetryConfig{
		MaxAttempts:  10,
		InitialDelay: 8 * time.Second,
		BackoffRate:  1,
		Jitter:       JitterFull,
	})
	err := errors.New("x")

	for range 100 {
		d := strategy(RetryAttempt{Err: err, Attempt: 1})
		if !d.Retry {
			t.Fatal("expected retry")
		}
		if d.Delay < time.Second || d.Delay > 8*time.Second {
			t.Fatalf("full jitter Delay = %v, want within [1s, 8s]", d.Delay)
		}
		if d.Delay%time.Second != 0 {
			t.Fatalf("Delay = %v, want whole seconds", d.Delay)
		}
	}
}

func TestNewRetryStrategyHalfJitterBounds(t *testing.T) {
	strategy := MustNewRetryStrategy(RetryConfig{
		MaxAttempts:  10,
		InitialDelay: 8 * time.Second,
		BackoffRate:  1,
		Jitter:       JitterHalf,
	})
	err := errors.New("x")

	for range 100 {
		d := strategy(RetryAttempt{Err: err, Attempt: 1})
		if d.Delay < 4*time.Second || d.Delay > 8*time.Second {
			t.Fatalf("half jitter Delay = %v, want within [4s, 8s]", d.Delay)
		}
	}
}

func TestNewRetryStrategyMinimumOneSecond(t *testing.T) {
	// A decaying backoff rate drives the computed delay below one
	// second; the final delay must floor at one second.
	strategy := MustNewRetryStrategy(RetryConfig{
		MaxAttempts:  4,
		InitialDelay: time.Second,
		BackoffRate:  0.25,
		Jitter:       JitterNone,
	})
	if d := strategy(RetryAttempt{Err: errors.New("x"), Attempt: 3}); d.Delay != time.Second {
		t.Errorf("sub-second delay = %v, want floored to 1s", d.Delay)
	}
}

func TestNoRetry(t *testing.T) {
	if d := NoRetry()(RetryAttempt{Err: errors.New("x"), Attempt: 1}); d.Retry {
		t.Errorf("NoRetry attempt 1 = %+v, want no retry", d)
	}
}

func TestExponentialBackoffPreset(t *testing.T) {
	// Cross-SDK default: 6 total attempts, delays within full-jitter
	// bounds of 5s × 2^(n-1) capped at 60s.
	strategy := ExponentialBackoff()
	err := errors.New("x")

	for attempt := 1; attempt <= 5; attempt++ {
		d := strategy(RetryAttempt{Err: err, Attempt: attempt})
		if !d.Retry {
			t.Fatalf("attempt %d: want retry", attempt)
		}
		maxBase := min(5*(1<<(attempt-1)), 60)
		if d.Delay < time.Second || d.Delay > time.Duration(maxBase)*time.Second {
			t.Errorf("attempt %d: Delay = %v, want within [1s, %ds]", attempt, d.Delay, maxBase)
		}
	}
	if d := strategy(RetryAttempt{Err: err, Attempt: 6}); d.Retry {
		t.Errorf("attempt 6 = %+v, want retries exhausted", d)
	}
}

func TestLinearBackoffDefaultSequence(t *testing.T) {
	// The zero-value config is the documented default: 6 total attempts,
	// delays 1s, 2s, 3s, 4s, 5s, no jitter.
	strategy := MustLinearBackoff(LinearRetryConfig{})
	cause := errors.New("x")

	for attempt := 1; attempt <= 5; attempt++ {
		d := strategy(RetryAttempt{Err: cause, Attempt: attempt})
		if want := time.Duration(attempt) * time.Second; !d.Retry || d.Delay != want {
			t.Errorf("attempt %d = %+v, want retry with %v delay", attempt, d, want)
		}
	}
	for _, attempt := range []int{6, 7} {
		if d := strategy(RetryAttempt{Err: cause, Attempt: attempt}); d.Retry {
			t.Errorf("attempt %d = %+v, want retries exhausted", attempt, d)
		}
	}
}

func TestLinearBackoffIncrementAndCap(t *testing.T) {
	// The worked example from the LinearBackoff documentation: delay
	// before retry n is InitialDelay + Increment × (n-1), capped at
	// MaxDelay, so 2s, 5s, 8s, then 11s and 14s capped to 10s.
	strategy := MustLinearBackoff(LinearRetryConfig{
		InitialDelay: 2 * time.Second,
		Increment:    3 * time.Second,
		MaxDelay:     10 * time.Second,
	})
	cause := errors.New("x")

	want := []time.Duration{2 * time.Second, 5 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for i, wantDelay := range want {
		attempt := i + 1
		d := strategy(RetryAttempt{Err: cause, Attempt: attempt})
		if !d.Retry || d.Delay != wantDelay {
			t.Errorf("attempt %d = %+v, want retry with %v delay", attempt, d, wantDelay)
		}
	}
	if d := strategy(RetryAttempt{Err: cause, Attempt: 6}); d.Retry {
		t.Errorf("attempt 6 = %+v, want retries exhausted", d)
	}
}

func TestLinearBackoffMaxAttempts(t *testing.T) {
	strategy := MustLinearBackoff(LinearRetryConfig{MaxAttempts: 3})
	cause := errors.New("x")

	if d := strategy(RetryAttempt{Err: cause, Attempt: 2}); !d.Retry || d.Delay != 2*time.Second {
		t.Errorf("attempt 2 = %+v, want retry with 2s delay", d)
	}
	if d := strategy(RetryAttempt{Err: cause, Attempt: 3}); d.Retry {
		t.Errorf("attempt 3 = %+v, want retries exhausted", d)
	}
}

func TestLinearBackoffFullJitterBounds(t *testing.T) {
	// Full jitter draws from [0, base]; the result is still rounded to a
	// whole second no less than one.
	strategy := MustLinearBackoff(LinearRetryConfig{
		InitialDelay: 10 * time.Second,
		Increment:    10 * time.Second,
		Jitter:       JitterFull,
	})
	cause := errors.New("x")

	for attempt := 1; attempt <= 5; attempt++ {
		base := time.Duration(10*attempt) * time.Second
		for range 50 {
			d := strategy(RetryAttempt{Err: cause, Attempt: attempt})
			if !d.Retry {
				t.Fatalf("attempt %d: want retry", attempt)
			}
			if d.Delay < time.Second || d.Delay > base {
				t.Errorf("attempt %d: Delay = %v, want within [1s, %v]", attempt, d.Delay, base)
			}
			if d.Delay%time.Second != 0 {
				t.Errorf("attempt %d: Delay = %v, want whole seconds", attempt, d.Delay)
			}
		}
	}
}

func TestLinearBackoffInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  LinearRetryConfig
		want string
	}{
		{"negative MaxAttempts", LinearRetryConfig{MaxAttempts: -1}, "MaxAttempts must not be negative"},
		{"sub-second InitialDelay", LinearRetryConfig{InitialDelay: 999 * time.Millisecond}, "InitialDelay must be at least 1 second"},
		{"negative InitialDelay", LinearRetryConfig{InitialDelay: -time.Second}, "InitialDelay must be at least 1 second"},
		{"negative Increment", LinearRetryConfig{Increment: -time.Second}, "Increment must not be negative"},
		{"sub-second MaxDelay", LinearRetryConfig{MaxDelay: time.Millisecond}, "MaxDelay must be at least 1 second"},
		{"unknown Jitter", LinearRetryConfig{Jitter: "SOME"}, "Jitter must be a defined JitterStrategy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, err := LinearBackoff(tt.cfg)
			if err == nil {
				t.Fatalf("LinearBackoff(%+v) error = nil, want error", tt.cfg)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want containing %q", err, tt.want)
			}
			if strategy != nil {
				t.Errorf("LinearBackoff(%+v) strategy is non-nil, want nil", tt.cfg)
			}
		})
	}
}

func TestLinearBackoffInvalidConfigJoinsErrors(t *testing.T) {
	_, err := LinearBackoff(LinearRetryConfig{MaxAttempts: -1, Increment: -time.Second})
	if err == nil {
		t.Fatal("error = nil, want joined errors")
	}
	for _, want := range []string{"MaxAttempts", "Increment"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want mention of %s", err, want)
		}
	}
}

func TestMustLinearBackoffPanicsOnInvalidConfig(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustLinearBackoff did not panic on invalid config")
		}
	}()
	MustLinearBackoff(LinearRetryConfig{InitialDelay: -time.Second})
}

// --- RetryableErrors ---

// matcherTestError is an error type for ErrorAs tests. It has a pointer
// receiver, so *matcherTestError is the type errors.As targets.
type matcherTestError struct{ code int }

func (e *matcherTestError) Error() string { return fmt.Sprintf("matcher test error %d", e.code) }

// matcherOtherError is a distinct type that must not match
// ErrorAs[*matcherTestError].
type matcherOtherError struct{}

func (*matcherOtherError) Error() string { return "other error" }

var errMatcherSentinel = errors.New("matcher sentinel")

// filteredStrategy builds a deterministic exponential strategy that retries
// only errors matching the given matchers, with room for several attempts.
func filteredStrategy(t *testing.T, matchers ...ErrorMatcher) RetryStrategy {
	t.Helper()
	strategy, err := NewRetryStrategy(RetryConfig{
		MaxAttempts:     5,
		Jitter:          JitterNone,
		RetryableErrors: matchers,
	})
	if err != nil {
		t.Fatalf("NewRetryStrategy error = %v, want nil", err)
	}
	return strategy
}

func TestRetryableErrorsMatchByType(t *testing.T) {
	// ErrorAs matches the type directly and through fmt.Errorf wrapping,
	// and does not match a different type.
	strategy := filteredStrategy(t, ErrorAs[*matcherTestError]())

	direct := &matcherTestError{code: 1}
	wrapped := fmt.Errorf("outer: %w", &matcherTestError{code: 2})
	other := &matcherOtherError{}

	if d := strategy(RetryAttempt{Err: direct, Attempt: 1}); !d.Retry {
		t.Errorf("direct type match = %+v, want retry", d)
	}
	if d := strategy(RetryAttempt{Err: wrapped, Attempt: 1}); !d.Retry {
		t.Errorf("wrapped type match = %+v, want retry", d)
	}
	if d := strategy(RetryAttempt{Err: other, Attempt: 1}); d.Retry {
		t.Errorf("non-matching type = %+v, want no retry", d)
	}
}

func TestRetryableErrorsMatchByInterfaceType(t *testing.T) {
	// ErrorAs accepts an interface type, matching any error that
	// implements it, wrapped or not.
	type coded interface {
		error
		Code() int
	}
	strategy := filteredStrategy(t, ErrorAs[coded]())

	if d := strategy(RetryAttempt{Err: fmt.Errorf("wrap: %w", codedError{7}), Attempt: 1}); !d.Retry {
		t.Errorf("wrapped interface match = %+v, want retry", d)
	}
	if d := strategy(RetryAttempt{Err: errors.New("plain"), Attempt: 1}); d.Retry {
		t.Errorf("non-implementing error = %+v, want no retry", d)
	}
}

type codedError struct{ code int }

func (e codedError) Error() string { return "coded" }
func (e codedError) Code() int     { return e.code }

func TestRetryableErrorsMatchBySentinel(t *testing.T) {
	// ErrorIs matches the sentinel directly and through wrapping, and does
	// not match an error with the same message but a different identity.
	strategy := filteredStrategy(t, ErrorIs(errMatcherSentinel))

	wrapped := fmt.Errorf("outer: %w", errMatcherSentinel)
	sameText := errors.New(errMatcherSentinel.Error())

	if d := strategy(RetryAttempt{Err: errMatcherSentinel, Attempt: 1}); !d.Retry {
		t.Errorf("direct sentinel match = %+v, want retry", d)
	}
	if d := strategy(RetryAttempt{Err: wrapped, Attempt: 1}); !d.Retry {
		t.Errorf("wrapped sentinel match = %+v, want retry", d)
	}
	if d := strategy(RetryAttempt{Err: sameText, Attempt: 1}); d.Retry {
		t.Errorf("same text, different identity = %+v, want no retry", d)
	}
}

func TestRetryableErrorsMatchByPattern(t *testing.T) {
	// ErrorContains is a substring test on Error(); ErrorMatches is a
	// regexp search on Error(). Both see the full wrapped message.
	tests := []struct {
		name    string
		matcher ErrorMatcher
		err     error
		want    bool
	}{
		{"contains hit", ErrorContains("throttl"), errors.New("request throttled"), true},
		{"contains hit in wrapped message", ErrorContains("throttl"), fmt.Errorf("call: %w", errors.New("throttled")), true},
		{"contains is case-sensitive", ErrorContains("Throttl"), errors.New("request throttled"), false},
		{"contains miss", ErrorContains("timeout"), errors.New("request throttled"), false},
		{"contains empty matches all", ErrorContains(""), errors.New("anything"), true},
		{"regexp hit", ErrorMatches(regexp.MustCompile(`(?i)time ?out`)), errors.New("Read TimeOut"), true},
		{"regexp hit in wrapped message", ErrorMatches(regexp.MustCompile(`code=5\d\d`)), fmt.Errorf("http: %w", errors.New("code=503")), true},
		{"regexp miss", ErrorMatches(regexp.MustCompile(`^code=`)), errors.New("http: code=503"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := filteredStrategy(t, tt.matcher)
			if d := strategy(RetryAttempt{Err: tt.err, Attempt: 1}); d.Retry != tt.want {
				t.Errorf("Retry = %v, want %v", d.Retry, tt.want)
			}
		})
	}
}

func TestRetryableErrorsAnyMatcherSuffices(t *testing.T) {
	// Several matchers combine with OR: an error retries when any one
	// matches, and stops retrying when none does.
	strategy := filteredStrategy(t,
		ErrorAs[*matcherTestError](),
		ErrorIs(errMatcherSentinel),
		ErrorContains("throttl"),
	)

	for _, err := range []error{
		&matcherTestError{code: 1},
		fmt.Errorf("wrap: %w", errMatcherSentinel),
		errors.New("throttled by downstream"),
	} {
		if d := strategy(RetryAttempt{Err: err, Attempt: 1}); !d.Retry {
			t.Errorf("%v: Retry = false, want true", err)
		}
	}
	if d := strategy(RetryAttempt{Err: errors.New("permanent"), Attempt: 1}); d.Retry {
		t.Errorf("non-matching error: Retry = true, want false")
	}
}

func TestRetryableErrorsNonMatchStopsAtAnyAttempt(t *testing.T) {
	// A non-matching error stops retrying regardless of attempts
	// remaining; a matching error still follows the schedule and the
	// attempt limit.
	strategy := filteredStrategy(t, ErrorIs(errMatcherSentinel))
	for attempt := 1; attempt <= 4; attempt++ {
		if d := strategy(RetryAttempt{Err: errors.New("permanent"), Attempt: attempt}); d.Retry {
			t.Errorf("attempt %d, non-matching: Retry = true, want false", attempt)
		}
		if d := strategy(RetryAttempt{Err: errMatcherSentinel, Attempt: attempt}); !d.Retry {
			t.Errorf("attempt %d, matching: Retry = false, want true", attempt)
		}
	}
	if d := strategy(RetryAttempt{Err: errMatcherSentinel, Attempt: 5}); d.Retry {
		t.Errorf("attempt 5, matching: Retry = true, want false (attempts exhausted)")
	}
}

func TestRetryableErrorsEmptyRetriesEverything(t *testing.T) {
	// No criteria: every error is retryable, whether the slice is nil or
	// empty and non-nil. This preserves the behavior before the field
	// existed.
	for _, matchers := range [][]ErrorMatcher{nil, {}} {
		strategy := filteredStrategy(t, matchers...)
		for _, err := range []error{
			errors.New("anything"),
			&matcherTestError{code: 1},
			errMatcherSentinel,
		} {
			if d := strategy(RetryAttempt{Err: err, Attempt: 1}); !d.Retry {
				t.Errorf("no criteria, %v: Retry = false, want true", err)
			}
		}
	}
}

func TestRetryableErrorsDelayUnchangedForMatch(t *testing.T) {
	// Filtering does not alter the delay schedule of a matching error.
	strategy := MustNewRetryStrategy(RetryConfig{
		MaxAttempts:     4,
		InitialDelay:    2 * time.Second,
		Jitter:          JitterNone,
		RetryableErrors: []ErrorMatcher{ErrorIs(errMatcherSentinel)},
	})
	for attempt, want := range map[int]time.Duration{1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second} {
		if d := strategy(RetryAttempt{Err: errMatcherSentinel, Attempt: attempt}); !d.Retry || d.Delay != want {
			t.Errorf("attempt %d = %+v, want retry with %v", attempt, d, want)
		}
	}
}

func TestRetryableErrorsLinearBackoff(t *testing.T) {
	// LinearRetryConfig applies the same filtering.
	strategy := MustLinearBackoff(LinearRetryConfig{
		RetryableErrors: []ErrorMatcher{ErrorAs[*matcherTestError]()},
	})
	if d := strategy(RetryAttempt{Err: &matcherTestError{code: 1}, Attempt: 1}); !d.Retry || d.Delay != time.Second {
		t.Errorf("matching = %+v, want retry with 1s delay", d)
	}
	if d := strategy(RetryAttempt{Err: errors.New("permanent"), Attempt: 1}); d.Retry {
		t.Errorf("non-matching = %+v, want no retry", d)
	}
}

func TestRetryableErrorsNilEntryRejected(t *testing.T) {
	// A nil matcher is invalid configuration, reported with its index,
	// alongside any other invalid field.
	tests := []struct {
		name string
		cfg  RetryConfig
		want []string
	}{
		{"nil literal", RetryConfig{RetryableErrors: []ErrorMatcher{nil}}, []string{"RetryConfig.RetryableErrors[0]"}},
		{"ErrorIs(nil)", RetryConfig{RetryableErrors: []ErrorMatcher{ErrorIs(nil)}}, []string{"RetryableErrors[0]"}},
		{"ErrorMatches(nil)", RetryConfig{RetryableErrors: []ErrorMatcher{ErrorMatches(nil)}}, []string{"RetryableErrors[0]"}},
		{
			"nil among valid entries",
			RetryConfig{RetryableErrors: []ErrorMatcher{ErrorContains("a"), nil, ErrorContains("b"), nil}},
			[]string{"RetryableErrors[1]", "RetryableErrors[3]"},
		},
		{
			"joined with another invalid field",
			RetryConfig{MaxAttempts: -1, RetryableErrors: []ErrorMatcher{nil}},
			[]string{"MaxAttempts", "RetryableErrors[0]"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, err := NewRetryStrategy(tt.cfg)
			if err == nil {
				t.Fatal("NewRetryStrategy error = nil, want error")
			}
			if strategy != nil {
				t.Error("strategy is non-nil, want nil on invalid config")
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %s", err, want)
				}
			}
		})
	}

	if _, err := LinearBackoff(LinearRetryConfig{RetryableErrors: []ErrorMatcher{nil}}); err == nil || !strings.Contains(err.Error(), "LinearRetryConfig.RetryableErrors[0]") {
		t.Errorf("LinearBackoff error = %v, want LinearRetryConfig.RetryableErrors[0] rejected", err)
	}
}

func TestMustNewRetryStrategyPanicsOnNilMatcher(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustNewRetryStrategy did not panic on nil matcher")
		}
	}()
	MustNewRetryStrategy(RetryConfig{RetryableErrors: []ErrorMatcher{ErrorIs(nil)}})
}

func TestRetryableErrorsSliceCopied(t *testing.T) {
	// Mutating the caller's slice after construction does not change the
	// strategy.
	matchers := []ErrorMatcher{ErrorIs(errMatcherSentinel)}
	strategy := MustNewRetryStrategy(RetryConfig{RetryableErrors: matchers})
	matchers[0] = ErrorContains("")

	if d := strategy(RetryAttempt{Err: errors.New("permanent"), Attempt: 1}); d.Retry {
		t.Errorf("strategy observed the caller's mutation: Retry = true, want false")
	}
}
