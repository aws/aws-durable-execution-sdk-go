package utils

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// JitterStrategy controls how randomness is applied to computed retry
// delays to avoid thundering-herd retries.
type JitterStrategy int

const (
	// JitterStrategyNone applies no jitter; the computed delay is used
	// exactly.
	JitterStrategyNone JitterStrategy = iota

	// JitterStrategyFull applies full jitter: the actual delay is a
	// uniform random value between 0 and the computed delay.
	JitterStrategyFull

	// JitterStrategyHalf applies half jitter: the actual delay is a
	// uniform random value between half the computed delay and the full
	// computed delay.
	JitterStrategyHalf
)

// RetryStrategyConfig parameterizes CreateRetryStrategy.
type RetryStrategyConfig struct {
	MaxAttempts  int
	InitialDelay *types.Duration
	MaxDelay     *types.Duration
	BackoffRate  float64
	Jitter       JitterStrategy
}

// CreateRetryStrategy builds a retry-strategy function (suitable for
// operations.WithStepRetryStrategy) from cfg: exponential backoff with
// configurable rate, cap, and jitter.
//
// Returns an error (docs/remaining-work.md §8 task 18) if cfg contains a
// value with no valid interpretation as a retry policy: a negative
// MaxAttempts (a retry count below zero means nothing - zero itself is
// valid and means "never retry, fail on the first attempt," matching
// retryOrFail's own `attempt >= cfg.MaxAttempts` check, which correctly
// treats MaxAttempts: 0 as "don't retry"), or a negative BackoffRate (the
// zero value 0 is already special-cased a few lines below to mean
// "default to 2.0," so only an explicitly negative rate - which would
// make the exponential backoff computation in the returned closure,
// initialSeconds * math.Pow(rate, attempt-1), oscillate in sign or
// otherwise produce a meaningless delay - is rejected here). Negative
// InitialDelay/MaxDelay are NOT separately checked here: they are
// *types.Duration, and a negative Duration is already rejected at a
// more appropriate, shared point - see WaitForCallback's identical
// negative-Duration check in operations/callback.go, which this package
// intentionally does not duplicate logic for since utils has no
// existing Duration-validation helper of its own to share it through
// without introducing a new cross-package dependency for this one
// check; a future shared types.Duration.Validate()-style helper could
// consolidate both, but that's a larger, unrelated change than this
// task's narrow scope.
func CreateRetryStrategy(cfg RetryStrategyConfig) (func(err error, attempt int) types.RetryDecision, error) {
	if cfg.MaxAttempts < 0 {
		return nil, fmt.Errorf("utils.CreateRetryStrategy: MaxAttempts must be non-negative, got %d", cfg.MaxAttempts)
	}
	if cfg.BackoffRate < 0 {
		return nil, fmt.Errorf("utils.CreateRetryStrategy: BackoffRate must be non-negative, got %v", cfg.BackoffRate)
	}

	initialSeconds := durationToSeconds(cfg.InitialDelay, 1)
	maxSeconds := durationToSeconds(cfg.MaxDelay, 300)
	rate := cfg.BackoffRate
	if rate <= 0 {
		rate = 2.0
	}

	return func(err error, attempt int) types.RetryDecision {
		if attempt >= cfg.MaxAttempts {
			return types.RetryDecision{ShouldRetry: false}
		}

		delaySeconds := initialSeconds * math.Pow(rate, float64(attempt-1))
		if delaySeconds > maxSeconds {
			delaySeconds = maxSeconds
		}

		switch cfg.Jitter {
		case JitterStrategyFull:
			delaySeconds = rand.Float64() * delaySeconds
		case JitterStrategyHalf:
			delaySeconds = delaySeconds/2 + rand.Float64()*(delaySeconds/2)
		}

		// Round to the nearest second and clamp to a minimum of 1 -
		// matching the JS reference SDK's own createRetryStrategy
		// exactly (retry-config/index.ts: "Ensure delay is an integer
		// >= 1", `Math.max(1, Math.round(delayWithJitter))`). Found and
		// fixed as a real, genuine bug while deploying operations.
		// Step's own new zero-option default (utils.Presets.Default(),
		// added alongside this fix - see that preset's own doc): the
		// OLD `int(delaySeconds)` here truncates toward zero rather than
		// rounding, and applies NO minimum floor at all, so full jitter
		// (rand.Float64() * delaySeconds, this same switch statement)
		// can genuinely compute e.g. 0.3s and truncate it to a bare 0 -
		// which the real backend's own CheckpointDurableExecution API
		// rejects outright with a real, observed ValidationException:
		// "Value '0' at 'updates.1.member.stepOptions.
		// nextAttemptDelaySeconds' failed to satisfy constraint: Member
		// must have value greater than or equal to 1" - confirmed via a
		// real deployed conformance requirement (1-13, "Default retry
		// strategy") genuinely failing this exact way on a real,
		// non-deterministic jitter draw. This was a LATENT bug in
		// CreateRetryStrategy's own full-jitter path that predates this
		// session's own retry-default fix entirely (ExponentialBackoff's
		// preset, and any caller-supplied JitterStrategyFull config, was
		// always theoretically exposed to it) - it simply had never been
		// exercised often/long enough by any existing test or example to
		// draw a small enough random value to trigger it before
		// Presets.Default() started actually being exercised by a real,
		// repeatedly-invoked conformance requirement.
		delaySeconds = math.Max(1, math.Round(delaySeconds))

		d := types.Duration{Seconds: int(delaySeconds)}
		return types.RetryDecision{ShouldRetry: true, Delay: &d}
	}, nil
}

func durationToSeconds(d *types.Duration, fallback float64) float64 {
	if d == nil {
		return fallback
	}
	return float64(d.Days*86400 + d.Hours*3600 + d.Minutes*60 + d.Seconds)
}

// Presets provides ready-to-use retry strategies for common cases.
var Presets = struct {
	// Default returns the SDK-wide default retry strategy: exponential
	// backoff with full jitter, maxAttempts=6, initialDelay=5s,
	// maxDelay=60s, backoffRate=2. This matches the JS reference SDK's
	// own retryPresets.default exactly (see that package's
	// retry-presets.ts) - the strategy operations.Step actually falls
	// back to when a caller supplies no WithStepRetryStrategy option at
	// all, confirmed by reading step-handler.ts's own two call sites
	// directly (`options?.retryStrategy?.(...) ?? retryPresets.default(...)`).
	Default func() func(err error, attempt int) types.RetryDecision

	// ExponentialBackoff returns exponential backoff with full jitter:
	// maxAttempts=3, initialDelay=5s, maxDelay=5m, backoffRate=2.
	ExponentialBackoff func() func(err error, attempt int) types.RetryDecision

	// NoRetry returns a strategy that never retries.
	NoRetry func() func(err error, attempt int) types.RetryDecision

	// FixedDelay returns a strategy that retries up to maxAttempts times
	// with a constant delay between attempts.
	FixedDelay func(delay types.Duration, maxAttempts int) func(err error, attempt int) types.RetryDecision
}{
	Default: func() func(err error, attempt int) types.RetryDecision {
		// Error safely discarded - see ExponentialBackoff's own identical
		// justification just below: MaxAttempts (6) and BackoffRate (2)
		// are fixed, valid, non-negative literals.
		strategy, _ := CreateRetryStrategy(RetryStrategyConfig{
			MaxAttempts:  6,
			InitialDelay: &types.Duration{Seconds: 5},
			MaxDelay:     &types.Duration{Seconds: 60},
			BackoffRate:  2,
			Jitter:       JitterStrategyFull,
		})
		return strategy
	},
	ExponentialBackoff: func() func(err error, attempt int) types.RetryDecision {
		// CreateRetryStrategy's error return (docs/remaining-work.md §8
		// task 18) is safely discarded here: this preset's own
		// MaxAttempts (3) and BackoffRate (2) are both fixed, valid,
		// non-negative literals, never derived from caller input - the
		// only way CreateRetryStrategy can return a non-nil error is if
		// one of those two fields were negative, which cannot happen for
		// a hardcoded preset.
		strategy, _ := CreateRetryStrategy(RetryStrategyConfig{
			MaxAttempts:  3,
			InitialDelay: &types.Duration{Seconds: 5},
			MaxDelay:     &types.Duration{Minutes: 5},
			BackoffRate:  2,
			Jitter:       JitterStrategyFull,
		})
		return strategy
	},
	NoRetry: func() func(err error, attempt int) types.RetryDecision {
		return func(err error, attempt int) types.RetryDecision {
			return types.RetryDecision{ShouldRetry: false}
		}
	},
	FixedDelay: func(delay types.Duration, maxAttempts int) func(err error, attempt int) types.RetryDecision {
		return func(err error, attempt int) types.RetryDecision {
			if attempt >= maxAttempts {
				return types.RetryDecision{ShouldRetry: false}
			}
			d := delay
			return types.RetryDecision{ShouldRetry: true, Delay: &d}
		}
	},
}
