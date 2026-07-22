package utils

import (
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// TestCreateRetryStrategy_RejectsInvalidConfig verifies the
// docs/remaining-work.md §8 task 18 fix: CreateRetryStrategy returns a
// descriptive error (rather than silently building a nonsensical
// strategy, or panicking) for a RetryStrategyConfig with no valid
// interpretation - a negative MaxAttempts or a negative BackoffRate.
func TestCreateRetryStrategy_RejectsInvalidConfig(t *testing.T) {
	t.Run("NegativeMaxAttempts", func(t *testing.T) {
		_, err := CreateRetryStrategy(RetryStrategyConfig{MaxAttempts: -1})
		if err == nil {
			t.Fatal("expected an error for negative MaxAttempts")
		}
		if !strings.Contains(err.Error(), "MaxAttempts must be non-negative") {
			t.Fatalf("unexpected error message: %v", err)
		}
	})

	t.Run("NegativeBackoffRate", func(t *testing.T) {
		_, err := CreateRetryStrategy(RetryStrategyConfig{MaxAttempts: 3, BackoffRate: -2})
		if err == nil {
			t.Fatal("expected an error for negative BackoffRate")
		}
		if !strings.Contains(err.Error(), "BackoffRate must be non-negative") {
			t.Fatalf("unexpected error message: %v", err)
		}
	})
}

// TestCreateRetryStrategy_ZeroMaxAttemptsIsValid confirms MaxAttempts: 0
// is accepted (it legitimately means "never retry, fail on the first
// attempt" - see retryOrFail's `attempt >= cfg.MaxAttempts` check in
// operations/step.go), distinguishing it from the genuinely-invalid
// negative case above.
func TestCreateRetryStrategy_ZeroMaxAttemptsIsValid(t *testing.T) {
	strategy, err := CreateRetryStrategy(RetryStrategyConfig{MaxAttempts: 0})
	if err != nil {
		t.Fatalf("expected MaxAttempts: 0 to be valid, got error: %v", err)
	}
	decision := strategy(nil, 1)
	if decision.ShouldRetry {
		t.Fatal("expected ShouldRetry=false with MaxAttempts: 0")
	}
}

// TestCreateRetryStrategy_FullJitterNeverProducesZeroDelay is a real,
// genuine regression test for a real bug found while deploying
// operations.Step's own new zero-option default (Presets.Default(),
// added alongside this fix): a Duration{Seconds: 0} decision from a
// JitterStrategyFull retry strategy is REJECTED outright by the real
// backend's own CheckpointDurableExecution API (confirmed via a real
// deployed conformance requirement 1-13 failing with an actual
// ValidationException: "Value '0' at 'updates.1.member.stepOptions.
// nextAttemptDelaySeconds' failed to satisfy constraint: Member must
// have value greater than or equal to 1"). Before this fix,
// `int(delaySeconds)` truncated toward zero with no floor at all, so
// `rand.Float64() * delaySeconds` (full jitter's own formula) could
// genuinely compute something like 0.3 and truncate it straight to 0.
//
// Runs many iterations (rather than one) specifically to give the
// underlying math/rand draw many chances to land in the danger zone
// (delaySeconds < 0.5, which rounds/truncates to 0) - a single run could
// easily pass by luck even with the old, buggy code, since only draws
// below 0.5/initialSeconds trigger it (e.g. below 0.1 for a 5s initial
// delay); 10,000 iterations makes that essentially certain to hit if
// the bug were still present, while still running in a few
// milliseconds.
func TestCreateRetryStrategy_FullJitterNeverProducesZeroDelay(t *testing.T) {
	strategy, err := CreateRetryStrategy(RetryStrategyConfig{
		MaxAttempts:  6,
		InitialDelay: &types.Duration{Seconds: 5},
		MaxDelay:     &types.Duration{Seconds: 60},
		BackoffRate:  2,
		Jitter:       JitterStrategyFull,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < 10000; i++ {
		decision := strategy(nil, 1)
		if !decision.ShouldRetry {
			t.Fatal("expected ShouldRetry=true on attempt 1 of 6")
		}
		if decision.Delay == nil {
			t.Fatal("expected a non-nil Delay when ShouldRetry is true")
		}
		seconds := decision.Delay.Days*86400 + decision.Delay.Hours*3600 + decision.Delay.Minutes*60 + decision.Delay.Seconds
		if seconds < 1 {
			t.Fatalf("iteration %d: expected a delay of at least 1 second (the real backend's own CheckpointDurableExecution API rejects 0 outright), got %d", i, seconds)
		}
	}
}

// TestCreateRetryStrategy_ValidConfigStillWorks is a basic regression
// check that a normal, valid config still produces a working strategy
// after this task's changes (CreateRetryStrategy's signature changed
// from a bare function return to (function, error)).
func TestCreateRetryStrategy_ValidConfigStillWorks(t *testing.T) {
	strategy, err := CreateRetryStrategy(RetryStrategyConfig{MaxAttempts: 3, BackoffRate: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d := strategy(nil, 1); !d.ShouldRetry {
		t.Fatal("expected ShouldRetry=true on attempt 1 of 3")
	}
	if d := strategy(nil, 3); d.ShouldRetry {
		t.Fatal("expected ShouldRetry=false once MaxAttempts is reached")
	}
}
