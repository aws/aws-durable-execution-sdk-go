package durable

import (
	"errors"
	"testing"
	"time"
)

func TestNewRetryStrategyDeterministic(t *testing.T) {
	// Jitter NONE makes delays exact: initial × rate^(attempt-1), capped.
	strategy := NewRetryStrategy(RetryConfig{
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
	strategy := NewRetryStrategy(RetryConfig{Jitter: JitterNone})
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

func TestNewRetryStrategyFullJitterBounds(t *testing.T) {
	strategy := NewRetryStrategy(RetryConfig{
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
	strategy := NewRetryStrategy(RetryConfig{
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
	strategy := NewRetryStrategy(RetryConfig{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Jitter:       JitterNone,
	})
	if d := strategy(errors.New("x"), 1); d.Delay != time.Second {
		t.Errorf("sub-second delay = %v, want rounded up to 1s", d.Delay)
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
	strategy := LinearBackoff(3 * time.Second)
	err := errors.New("x")

	for attempt := 1; attempt <= 5; attempt++ {
		d := strategy(err, attempt)
		if !d.Retry || d.Delay != 3*time.Second {
			t.Errorf("attempt %d = %+v, want retry with fixed 3s delay", attempt, d)
		}
	}
	if d := strategy(err, 6); d.Retry {
		t.Errorf("attempt 6 = %+v, want retries exhausted", d)
	}
}
