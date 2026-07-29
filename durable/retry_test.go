package durable

import (
	"errors"
	"math"
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
		d := strategy(err, tt.attempt)
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

	if d := strategy(err, 1); !d.Retry || d.Delay != 5*time.Second {
		t.Errorf("attempt 1 = %+v, want retry with 5s delay", d)
	}
	if d := strategy(err, 2); !d.Retry || d.Delay != 10*time.Second {
		t.Errorf("attempt 2 = %+v, want retry with 10s delay", d)
	}
	if d := strategy(err, 3); d.Retry {
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
	if d := strategy(errors.New("x"), 1); !d.Retry {
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
				d := strategy(err, i+1)
				if !d.Retry || d.Delay != want {
					t.Errorf("attempt %d = %+v, want retry with %v delay", i+1, d, want)
				}
			}
			if d := strategy(err, tt.noMore); d.Retry {
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
		d := strategy(err, 1)
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
		d := strategy(err, 1)
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
	if d := strategy(errors.New("x"), 3); d.Delay != time.Second {
		t.Errorf("sub-second delay = %v, want floored to 1s", d.Delay)
	}
}

func TestNoRetry(t *testing.T) {
	if d := NoRetry()(errors.New("x"), 1); d.Retry {
		t.Errorf("NoRetry attempt 1 = %+v, want no retry", d)
	}
}

func TestExponentialBackoffPreset(t *testing.T) {
	// Cross-SDK default: 6 total attempts, delays within full-jitter
	// bounds of 5s × 2^(n-1) capped at 60s.
	strategy := ExponentialBackoff()
	err := errors.New("x")

	for attempt := 1; attempt <= 5; attempt++ {
		d := strategy(err, attempt)
		if !d.Retry {
			t.Fatalf("attempt %d: want retry", attempt)
		}
		maxBase := min(5*(1<<(attempt-1)), 60)
		if d.Delay < time.Second || d.Delay > time.Duration(maxBase)*time.Second {
			t.Errorf("attempt %d: Delay = %v, want within [1s, %ds]", attempt, d.Delay, maxBase)
		}
	}
	if d := strategy(err, 6); d.Retry {
		t.Errorf("attempt 6 = %+v, want retries exhausted", d)
	}
}

func TestLinearBackoffFixedDelay(t *testing.T) {
	strategy, err := LinearBackoff(3 * time.Second)
	if err != nil {
		t.Fatalf("LinearBackoff(3s) error = %v, want nil", err)
	}
	cause := errors.New("x")

	for attempt := 1; attempt <= 5; attempt++ {
		d := strategy(cause, attempt)
		if !d.Retry || d.Delay != 3*time.Second {
			t.Errorf("attempt %d = %+v, want retry with fixed 3s delay", attempt, d)
		}
	}
	if d := strategy(cause, 6); d.Retry {
		t.Errorf("attempt 6 = %+v, want retries exhausted", d)
	}
}

func TestLinearBackoffZeroDelayDefault(t *testing.T) {
	// A zero delay selects the 5 second default.
	strategy, err := LinearBackoff(0)
	if err != nil {
		t.Fatalf("LinearBackoff(0) error = %v, want nil", err)
	}
	if d := strategy(errors.New("x"), 1); !d.Retry || d.Delay != 5*time.Second {
		t.Errorf("attempt 1 = %+v, want retry with default 5s delay", d)
	}
}

func TestLinearBackoffInvalidDelay(t *testing.T) {
	for _, delay := range []time.Duration{-time.Second, time.Millisecond, 999 * time.Millisecond} {
		strategy, err := LinearBackoff(delay)
		if err == nil {
			t.Errorf("LinearBackoff(%v) error = nil, want error", delay)
		}
		if strategy != nil {
			t.Errorf("LinearBackoff(%v) strategy is non-nil, want nil", delay)
		}
	}
}

func TestMustLinearBackoffPanicsOnInvalidDelay(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustLinearBackoff did not panic on invalid delay")
		}
	}()
	MustLinearBackoff(-time.Second)
}
