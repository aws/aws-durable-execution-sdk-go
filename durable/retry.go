package durable

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// RetryDecision is a retry strategy's verdict for a failed attempt.
type RetryDecision struct {
	// Retry indicates whether the operation should be attempted again.
	Retry bool

	// Delay is how long to wait before the next attempt. It is ignored
	// when Retry is false.
	Delay time.Duration
}

// RetryStrategy decides whether and when to retry a failed step attempt.
// err is the error from the attempt that just failed, and attempt is its
// 1-based number. Strategies must be deterministic functions of their
// arguments, except for randomized jitter in the returned delay.
//
// To retry only specific errors, write a strategy that inspects err with
// [errors.Is] or [errors.As] before delegating to a configured strategy:
//
//	transientOnly := func(err error, attempt int) durable.RetryDecision {
//		var te *TransientError
//		if !errors.As(err, &te) {
//			return durable.RetryDecision{}
//		}
//		return durable.ExponentialBackoff()(err, attempt)
//	}
type RetryStrategy func(err error, attempt int) RetryDecision

// JitterStrategy randomizes retry delays to avoid thundering herds.
type JitterStrategy string

// Jitter strategies for retry delays.
const (
	// JitterFull randomizes the delay between zero and the computed
	// delay. This is the default.
	JitterFull JitterStrategy = "FULL"

	// JitterHalf randomizes the delay between half the computed delay
	// and the computed delay.
	JitterHalf JitterStrategy = "HALF"

	// JitterNone applies the computed delay unchanged.
	JitterNone JitterStrategy = "NONE"
)

// RetryConfig configures an exponential backoff retry strategy created with
// [NewRetryStrategy] or [MustNewRetryStrategy]. The zero value of each field
// selects its documented default.
type RetryConfig struct {
	// MaxAttempts is the maximum number of total attempts, including the
	// first. The default is 3. It must not be negative.
	MaxAttempts int

	// InitialDelay is the delay before the first retry. The default is
	// 5 seconds. When set, it must be at least 1 second.
	InitialDelay time.Duration

	// MaxDelay caps the delay between retries. The default is 5 minutes.
	// When set, it must be at least 1 second.
	MaxDelay time.Duration

	// BackoffRate multiplies the delay after each attempt. The default
	// is 2. It must be a finite value and must not be negative.
	BackoffRate float64

	// Jitter is the jitter strategy applied to computed delays. The
	// default is [JitterFull]. When set, it must be one of the defined
	// [JitterStrategy] constants.
	Jitter JitterStrategy
}

// NewRetryStrategy returns an exponential backoff retry strategy: the delay
// before retry n is InitialDelay × BackoffRate^(n-1), capped at MaxDelay,
// with jitter applied, rounded to a whole number of seconds no less than
// one.
//
// It returns an error if cfg is invalid; see [RetryConfig] for the
// constraints on each field. Zero-value fields are always valid and select
// their documented defaults.
func NewRetryStrategy(cfg RetryConfig) (RetryStrategy, error) {
	if err := validateRetryConfig(cfg); err != nil {
		return nil, err
	}
	return newRetryStrategy(cfg), nil
}

// MustNewRetryStrategy is like [NewRetryStrategy] but panics if cfg is
// invalid. It is intended for initialization with hard-coded
// configurations, where invalid values are programming errors.
func MustNewRetryStrategy(cfg RetryConfig) RetryStrategy {
	strategy, err := NewRetryStrategy(cfg)
	if err != nil {
		panic(err)
	}
	return strategy
}

// validateRetryConfig checks each RetryConfig field against its documented
// constraints. Zero values are valid (they select defaults). It returns an
// [errors.Join] of one plain error per invalid field, or nil.
func validateRetryConfig(cfg RetryConfig) error {
	var errs []error
	if cfg.MaxAttempts < 0 {
		errs = append(errs, errors.New("durable: RetryConfig.MaxAttempts must not be negative"))
	}
	if cfg.InitialDelay != 0 && cfg.InitialDelay < time.Second {
		errs = append(errs, errors.New("durable: RetryConfig.InitialDelay must be at least 1 second when set"))
	}
	if cfg.MaxDelay != 0 && cfg.MaxDelay < time.Second {
		errs = append(errs, errors.New("durable: RetryConfig.MaxDelay must be at least 1 second when set"))
	}
	if math.IsNaN(cfg.BackoffRate) || math.IsInf(cfg.BackoffRate, 0) {
		errs = append(errs, errors.New("durable: RetryConfig.BackoffRate must be a finite number"))
	} else if cfg.BackoffRate < 0 {
		errs = append(errs, errors.New("durable: RetryConfig.BackoffRate must not be negative"))
	}
	switch cfg.Jitter {
	case "", JitterFull, JitterHalf, JitterNone:
	default:
		errs = append(errs, fmt.Errorf("durable: RetryConfig.Jitter must be a defined JitterStrategy constant, got %q", cfg.Jitter))
	}
	return errors.Join(errs...)
}

// newRetryStrategy builds the exponential backoff strategy from a config
// already known to be valid, applying the documented default for each
// zero-value field.
func newRetryStrategy(cfg RetryConfig) RetryStrategy {
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.InitialDelay == 0 {
		cfg.InitialDelay = 5 * time.Second
	}
	if cfg.MaxDelay == 0 {
		cfg.MaxDelay = 5 * time.Minute
	}
	if cfg.BackoffRate == 0 {
		cfg.BackoffRate = 2
	}
	if cfg.Jitter == "" {
		cfg.Jitter = JitterFull
	}
	return func(_ error, attempt int) RetryDecision {
		if attempt >= cfg.MaxAttempts {
			return RetryDecision{}
		}
		base := math.Min(
			cfg.InitialDelay.Seconds()*math.Pow(cfg.BackoffRate, float64(attempt-1)),
			cfg.MaxDelay.Seconds(),
		)
		return RetryDecision{Retry: true, Delay: finalizeDelay(base, cfg.Jitter)}
	}
}

// finalizeDelay applies jitter to a delay in seconds and normalizes it to a
// whole number of seconds that is always at least one.
func finalizeDelay(seconds float64, jitter JitterStrategy) time.Duration {
	switch jitter {
	case JitterFull:
		seconds = rand.Float64() * seconds //nolint:gosec // jitter, not cryptography
	case JitterHalf:
		seconds = seconds/2 + rand.Float64()*(seconds/2) //nolint:gosec // jitter, not cryptography
	case JitterNone:
	}
	return time.Duration(math.Max(1, math.Round(seconds))) * time.Second
}

// NoRetry returns a strategy that never retries.
func NoRetry() RetryStrategy {
	return func(error, int) RetryDecision {
		return RetryDecision{}
	}
}

// ExponentialBackoff returns the default retry strategy: 6 total attempts
// with exponentially increasing delays, starting at 5 seconds, doubling
// each attempt, capped at 60 seconds, with full jitter.
func ExponentialBackoff() RetryStrategy {
	return newRetryStrategy(RetryConfig{
		MaxAttempts:  6,
		InitialDelay: 5 * time.Second,
		MaxDelay:     60 * time.Second,
		BackoffRate:  2,
		Jitter:       JitterFull,
	})
}

// LinearBackoff returns a strategy with a fixed delay between attempts and
// 6 total attempts. The delay is rounded to a whole number of seconds no
// less than one, without jitter.
//
// A zero delay selects the default of 5 seconds. It returns an error if
// delay is set and less than 1 second.
func LinearBackoff(delay time.Duration) (RetryStrategy, error) {
	if delay != 0 && delay < time.Second {
		return nil, errors.New("durable: LinearBackoff delay must be at least 1 second when set")
	}
	return newRetryStrategy(RetryConfig{
		MaxAttempts:  6,
		InitialDelay: delay,
		BackoffRate:  1,
		Jitter:       JitterNone,
	}), nil
}

// MustLinearBackoff is like [LinearBackoff] but panics if delay is
// invalid. It is intended for initialization with hard-coded
// configurations, where invalid values are programming errors.
func MustLinearBackoff(delay time.Duration) RetryStrategy {
	strategy, err := LinearBackoff(delay)
	if err != nil {
		panic(err)
	}
	return strategy
}
