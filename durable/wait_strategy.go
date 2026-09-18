package durable

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// WaitStrategy decides, after each check of a [WaitForCondition], whether
// to keep waiting and for how long. state is the value the check returned,
// round-tripped through the configured [Serdes]. attempt is the 1-based
// number of completed checks.
//
// A strategy must be a deterministic function of its arguments, except for
// randomized jitter in the returned delay. Build one from declarative
// configuration with [NewWaitStrategy] or [MustNewWaitStrategy], or write
// one by hand. [ConditionConfig].WaitStrategy has this type's underlying
// function type, so a WaitStrategy assigns to it directly.
type WaitStrategy[S any] func(state S, attempt int) WaitDecision

// Default wait strategy parameters. The zero value of each [WaitConfig]
// field selects the matching default, and a [ConditionConfig] with a nil
// WaitStrategy uses the strategy that WaitConfig[S]{} builds.
const (
	defaultConditionMaxAttempts    = 60
	defaultConditionInitialDelay   = 5 * time.Second
	defaultConditionMaxDelay       = 5 * time.Minute
	defaultConditionBackoffRate    = 1.5
	defaultConditionJitterStrategy = JitterFull
)

// WaitConfig configures an exponential backoff wait strategy created with
// [NewWaitStrategy] or [MustNewWaitStrategy]. The zero value of each field
// selects its documented default. Field names match [RetryConfig] where
// the meaning is the same; only the defaults differ.
//
// WaitConfig[S]{} builds the strategy [WaitForCondition] uses when
// [ConditionConfig].WaitStrategy is nil: poll with a 5 second initial
// delay multiplied by 1.5 after each attempt, capped at 5 minutes, with
// full jitter, and fail once 60 attempts have been made.
type WaitConfig[S any] struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// MaxAttempts is the maximum number of checks, including the first.
	// Reaching it with the condition still unmet fails the operation with
	// a [*WaitForConditionError]. The default is 60. It must not be
	// negative.
	MaxAttempts int

	// InitialDelay is the delay before the second check. The default is
	// 5 seconds. When set, it must be at least 1 second.
	InitialDelay time.Duration

	// MaxDelay caps the delay between checks. The default is 5 minutes.
	// When set, it must be at least 1 second.
	MaxDelay time.Duration

	// BackoffRate multiplies the delay after each check. The default is
	// 1.5. It must be a finite value and must not be negative.
	BackoffRate float64

	// Jitter is the jitter strategy applied to computed delays. The
	// default is [JitterFull]. When set, it must be one of the defined
	// [JitterStrategy] constants.
	Jitter JitterStrategy

	// ShouldContinue reports whether to keep polling given the state the
	// latest check returned. When it returns false the condition is met:
	// the operation succeeds with that state, even on the final attempt.
	//
	// When nil, every check continues polling, so the operation can only
	// end by reaching MaxAttempts. Set it to make the wait succeed.
	ShouldContinue func(state S) bool
}

// NewWaitStrategy returns an exponential backoff wait strategy for
// [WaitForCondition]. After each check the strategy consults
// cfg.ShouldContinue; when it reports the condition met, the strategy
// stops and the operation succeeds. Otherwise, once cfg.MaxAttempts
// checks have been made, the strategy fails the operation. Otherwise the
// delay before check n+1 is InitialDelay × BackoffRate^(n-1), capped at
// MaxDelay, with jitter applied, rounded to a whole number of seconds no
// less than one.
//
// It returns an error if cfg is invalid; see [WaitConfig] for the
// constraints on each field. Zero-value fields are always valid and select
// their documented defaults.
func NewWaitStrategy[S any](cfg WaitConfig[S]) (WaitStrategy[S], error) {
	if err := validateWaitConfig(cfg); err != nil {
		return nil, err
	}
	return newWaitStrategy(cfg), nil
}

// MustNewWaitStrategy is like [NewWaitStrategy] but panics if cfg is
// invalid. It is intended for initialization with hard-coded
// configurations, where invalid values are programming errors.
func MustNewWaitStrategy[S any](cfg WaitConfig[S]) WaitStrategy[S] {
	strategy, err := NewWaitStrategy(cfg)
	if err != nil {
		panic(err)
	}
	return strategy
}

// validateWaitConfig checks each WaitConfig field against its documented
// constraints. Zero values are valid (they select defaults). It returns an
// [errors.Join] of one plain error per invalid field, or nil.
func validateWaitConfig[S any](cfg WaitConfig[S]) error {
	var errs []error
	if cfg.MaxAttempts < 0 {
		errs = append(errs, errors.New("durable: WaitConfig.MaxAttempts must not be negative"))
	}
	if cfg.InitialDelay != 0 && cfg.InitialDelay < time.Second {
		errs = append(errs, errors.New("durable: WaitConfig.InitialDelay must be at least 1 second when set"))
	}
	if cfg.MaxDelay != 0 && cfg.MaxDelay < time.Second {
		errs = append(errs, errors.New("durable: WaitConfig.MaxDelay must be at least 1 second when set"))
	}
	if math.IsNaN(cfg.BackoffRate) || math.IsInf(cfg.BackoffRate, 0) {
		errs = append(errs, errors.New("durable: WaitConfig.BackoffRate must be a finite number"))
	} else if cfg.BackoffRate < 0 {
		errs = append(errs, errors.New("durable: WaitConfig.BackoffRate must not be negative"))
	}
	switch cfg.Jitter {
	case "", JitterFull, JitterHalf, JitterNone:
	default:
		errs = append(errs, fmt.Errorf("durable: WaitConfig.Jitter must be a defined JitterStrategy constant, got %q", cfg.Jitter))
	}
	return errors.Join(errs...)
}

// newWaitStrategy builds the exponential backoff wait strategy from a
// config already known to be valid, applying the documented default for
// each zero-value field.
func newWaitStrategy[S any](cfg WaitConfig[S]) WaitStrategy[S] {
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = defaultConditionMaxAttempts
	}
	if cfg.InitialDelay == 0 {
		cfg.InitialDelay = defaultConditionInitialDelay
	}
	if cfg.MaxDelay == 0 {
		cfg.MaxDelay = defaultConditionMaxDelay
	}
	if cfg.BackoffRate == 0 {
		cfg.BackoffRate = defaultConditionBackoffRate
	}
	if cfg.Jitter == "" {
		cfg.Jitter = defaultConditionJitterStrategy
	}
	return func(state S, attempt int) WaitDecision {
		// A met condition wins over exhaustion, so a condition met on
		// the final attempt still succeeds.
		if cfg.ShouldContinue != nil && !cfg.ShouldContinue(state) {
			return WaitDecision{}
		}
		if attempt >= cfg.MaxAttempts {
			return WaitDecision{
				Err: fmt.Errorf("durable: WaitForCondition exceeded maximum attempts (%d)", cfg.MaxAttempts),
			}
		}
		base := math.Min(
			cfg.InitialDelay.Seconds()*math.Pow(cfg.BackoffRate, float64(attempt-1)),
			cfg.MaxDelay.Seconds(),
		)
		return WaitDecision{Continue: true, Delay: finalizeDelay(base, cfg.Jitter)}
	}
}
