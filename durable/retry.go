package durable

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"regexp"
	"strings"
	"time"
)

// RetryDecision is a retry strategy's verdict for a failed attempt.
type RetryDecision struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// Retry indicates whether the operation should be attempted again.
	Retry bool

	// Delay is how long to wait before the next attempt. It is ignored
	// when Retry is false.
	//
	// The SDK sends the delay as a whole number of seconds, rounding a
	// fractional delay up. A zero Delay selects [DefaultRetryDelay], so a
	// strategy that returns RetryDecision{Retry: true} without setting
	// Delay waits one second. A negative Delay is an error that fails the
	// step.
	Delay time.Duration
}

// DefaultRetryDelay is the delay before the next attempt when a
// [RetryStrategy] returns a [RetryDecision] with Retry set and a zero
// Delay. It is also the smallest delay the SDK sends: a positive
// fractional Delay rounds up to one second.
const DefaultRetryDelay = time.Second

// RetryAttempt describes a failed attempt to a [RetryStrategy].
type RetryAttempt struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// Err is the error from the attempt that just failed.
	Err error

	// Attempt is the 1-based number of the attempt that just failed,
	// inclusive of the first attempt.
	Attempt int

	// Elapsed is the time since the first attempt began. It is zero when
	// the SDK does not track it.
	Elapsed time.Duration
}

// RetryStrategy decides whether and when to retry a failed step attempt.
// Strategies must be deterministic functions of the [RetryAttempt] they
// receive, except for randomized jitter in the returned delay.
//
// To retry only specific errors, set [RetryConfig.RetryableErrors] or
// [LinearRetryConfig.RetryableErrors] on a configured strategy. A
// hand-written strategy receives every failed attempt and decides for
// itself; it can inspect [RetryAttempt.Err] with [errors.Is] or
// [errors.As], or apply an [ErrorMatcher], before delegating to a
// configured strategy:
//
//	transientOnly := func(a durable.RetryAttempt) durable.RetryDecision {
//		var te *TransientError
//		if !errors.As(a.Err, &te) {
//			return durable.RetryDecision{}
//		}
//		return durable.ExponentialBackoff()(a)
//	}
type RetryStrategy func(RetryAttempt) RetryDecision

// ErrorMatcher reports whether a failed attempt's error is retryable. It is
// used in [RetryConfig.RetryableErrors] and
// [LinearRetryConfig.RetryableErrors]. [ErrorIs], [ErrorAs],
// [ErrorContains], and [ErrorMatches] build matchers for the common cases;
// any func(error) bool is a matcher.
//
// A matcher must be a deterministic function of the error it receives, for
// the same reason a [RetryStrategy] must be.
type ErrorMatcher func(err error) bool

// ErrorIs returns a matcher that reports whether an error matches target
// under [errors.Is], so wrapped errors match. It is intended for sentinel
// errors such as io.EOF.
//
// ErrorIs(nil) returns a nil matcher, which [NewRetryStrategy] and
// [LinearBackoff] reject.
func ErrorIs(target error) ErrorMatcher {
	if target == nil {
		return nil
	}
	return func(err error) bool { return errors.Is(err, target) }
}

// ErrorAs returns a matcher that reports whether an error matches type T
// under [errors.As], so wrapped errors match. T is the type a caller would
// pass a pointer to when calling errors.As directly: a pointer type for
// errors with pointer receivers, or an interface type.
//
//	durable.ErrorAs[*TransientError]()
//	durable.ErrorAs[net.Error]()
func ErrorAs[T error]() ErrorMatcher {
	return func(err error) bool {
		var target T
		return errors.As(err, &target)
	}
}

// ErrorContains returns a matcher that reports whether an error's message,
// the value of its Error method, contains substr. The empty string matches
// every error.
func ErrorContains(substr string) ErrorMatcher {
	return func(err error) bool { return strings.Contains(err.Error(), substr) }
}

// ErrorMatches returns a matcher that reports whether an error's message,
// the value of its Error method, contains a match of re.
//
// ErrorMatches(nil) returns a nil matcher, which [NewRetryStrategy] and
// [LinearBackoff] reject.
func ErrorMatches(re *regexp.Regexp) ErrorMatcher {
	if re == nil {
		return nil
	}
	return func(err error) bool { return re.MatchString(err.Error()) }
}

// errorRetryable reports whether err is retryable under matchers. An empty
// matcher list means every error is retryable. A non-empty list is
// retryable when any one matcher reports a match.
func errorRetryable(err error, matchers []ErrorMatcher) bool {
	if len(matchers) == 0 {
		return true
	}
	for _, match := range matchers {
		if match(err) {
			return true
		}
	}
	return false
}

// validateErrorMatchers returns one error per nil entry in matchers, naming
// the config type and index, for inclusion in a config's validation error.
func validateErrorMatchers(configType string, matchers []ErrorMatcher) []error {
	var errs []error
	for i, match := range matchers {
		if match == nil {
			errs = append(errs, fmt.Errorf("durable: %s.RetryableErrors[%d] must not be nil", configType, i))
		}
	}
	return errs
}

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
//
// The zero value of RetryConfig is not the strategy [ExponentialBackoff]
// returns. RetryConfig{} produces 3 total attempts with delays of 5 s and
// 10 s before jitter, capped at 5 minutes. ExponentialBackoff produces 6
// total attempts with delays of 5 s, 10 s, 20 s, 40 s, and 60 s before
// jitter, capped at 60 seconds. Both apply full jitter.
type RetryConfig struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

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

	// RetryableErrors restricts retries to errors that at least one
	// matcher reports as retryable. When empty, every error is retryable.
	// A non-matching error is not retried: the step fails on that attempt
	// with the attempts made so far. Entries must not be nil.
	//
	// Build matchers with [ErrorIs] for sentinel errors, [ErrorAs] for
	// error types, and [ErrorContains] or [ErrorMatches] for message
	// patterns:
	//
	//	durable.RetryConfig{
	//		RetryableErrors: []durable.ErrorMatcher{
	//			durable.ErrorAs[*TransientError](),
	//			durable.ErrorIs(io.ErrUnexpectedEOF),
	//			durable.ErrorContains("throttl"),
	//		},
	//	}
	//
	// RetryableErrors applies to the strategy [NewRetryStrategy] builds
	// from this config. A hand-written [RetryStrategy] is not filtered; it
	// sees every failed attempt. To combine the two, have the hand-written
	// strategy delegate to the configured one, which then applies the
	// matchers, or apply an [ErrorMatcher] directly to [RetryAttempt.Err].
	RetryableErrors []ErrorMatcher
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
	errs = append(errs, validateErrorMatchers("RetryConfig", cfg.RetryableErrors)...)
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
	// Copy the matchers so a caller mutating its slice after construction
	// does not change the strategy.
	matchers := append([]ErrorMatcher(nil), cfg.RetryableErrors...)
	return func(a RetryAttempt) RetryDecision {
		if a.Attempt >= cfg.MaxAttempts {
			return RetryDecision{}
		}
		if !errorRetryable(a.Err, matchers) {
			return RetryDecision{}
		}
		base := math.Min(
			cfg.InitialDelay.Seconds()*math.Pow(cfg.BackoffRate, float64(a.Attempt-1)),
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
	return func(RetryAttempt) RetryDecision {
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

// LinearRetryConfig configures a linear backoff retry strategy created with
// [LinearBackoff] or [MustLinearBackoff]. The zero value of each field
// selects its documented default. The zero value of LinearRetryConfig
// produces the default linear strategy: 6 total attempts with delays of
// 1 s, 2 s, 3 s, 4 s, and 5 s, without jitter.
type LinearRetryConfig struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// MaxAttempts is the maximum number of total attempts, including the
	// first. The default is 6. It must not be negative.
	MaxAttempts int

	// InitialDelay is the delay before the first retry. The default is
	// 1 second. When set, it must be at least 1 second.
	InitialDelay time.Duration

	// Increment is added to the delay before each retry after the first.
	// The default is 1 second. It must not be negative. For a fixed
	// interval between attempts, use [NewRetryStrategy] with a
	// BackoffRate of 1 instead.
	Increment time.Duration

	// MaxDelay caps the delay between retries. The default is 5 minutes.
	// When set, it must be at least 1 second.
	MaxDelay time.Duration

	// Jitter is the jitter strategy applied to computed delays. The
	// default is [JitterNone], so the default sequence is exact. When
	// set, it must be one of the defined [JitterStrategy] constants.
	Jitter JitterStrategy

	// RetryableErrors restricts retries to errors that at least one
	// matcher reports as retryable. When empty, every error is retryable.
	// Entries must not be nil. See [RetryConfig.RetryableErrors].
	RetryableErrors []ErrorMatcher
}

// LinearBackoff returns a linear backoff retry strategy: the delay before
// retry n is InitialDelay + Increment × (n-1), capped at MaxDelay, with
// jitter applied, rounded to a whole number of seconds no less than one.
//
// The zero value of cfg produces 6 total attempts with delays of 1 s, 2 s,
// 3 s, 4 s, and 5 s, without jitter. As a worked example,
// LinearRetryConfig{InitialDelay: 2 * time.Second, Increment: 3 * time.Second,
// MaxDelay: 10 * time.Second} produces delays of 2 s, 5 s, 8 s, 10 s, and
// 10 s: the fourth and fifth retries would be 11 s and 14 s but are capped.
//
// It returns an error if cfg is invalid; see [LinearRetryConfig] for the
// constraints on each field. Zero-value fields are always valid and select
// their documented defaults.
func LinearBackoff(cfg LinearRetryConfig) (RetryStrategy, error) {
	if err := validateLinearRetryConfig(cfg); err != nil {
		return nil, err
	}
	return newLinearRetryStrategy(cfg), nil
}

// MustLinearBackoff is like [LinearBackoff] but panics if cfg is invalid.
// It is intended for initialization with hard-coded configurations, where
// invalid values are programming errors. MustLinearBackoff(LinearRetryConfig{})
// produces 6 total attempts with delays of 1 s, 2 s, 3 s, 4 s, and 5 s,
// without jitter.
func MustLinearBackoff(cfg LinearRetryConfig) RetryStrategy {
	strategy, err := LinearBackoff(cfg)
	if err != nil {
		panic(err)
	}
	return strategy
}

// validateLinearRetryConfig checks each LinearRetryConfig field against its
// documented constraints. Zero values are valid (they select defaults). It
// returns an [errors.Join] of one plain error per invalid field, or nil.
func validateLinearRetryConfig(cfg LinearRetryConfig) error {
	var errs []error
	if cfg.MaxAttempts < 0 {
		errs = append(errs, errors.New("durable: LinearRetryConfig.MaxAttempts must not be negative"))
	}
	if cfg.InitialDelay != 0 && cfg.InitialDelay < time.Second {
		errs = append(errs, errors.New("durable: LinearRetryConfig.InitialDelay must be at least 1 second when set"))
	}
	if cfg.Increment < 0 {
		errs = append(errs, errors.New("durable: LinearRetryConfig.Increment must not be negative"))
	}
	if cfg.MaxDelay != 0 && cfg.MaxDelay < time.Second {
		errs = append(errs, errors.New("durable: LinearRetryConfig.MaxDelay must be at least 1 second when set"))
	}
	switch cfg.Jitter {
	case "", JitterFull, JitterHalf, JitterNone:
	default:
		errs = append(errs, fmt.Errorf("durable: LinearRetryConfig.Jitter must be a defined JitterStrategy constant, got %q", cfg.Jitter))
	}
	errs = append(errs, validateErrorMatchers("LinearRetryConfig", cfg.RetryableErrors)...)
	return errors.Join(errs...)
}

// newLinearRetryStrategy builds the linear backoff strategy from a config
// already known to be valid, applying the documented default for each
// zero-value field.
func newLinearRetryStrategy(cfg LinearRetryConfig) RetryStrategy {
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 6
	}
	if cfg.InitialDelay == 0 {
		cfg.InitialDelay = time.Second
	}
	if cfg.Increment == 0 {
		cfg.Increment = time.Second
	}
	if cfg.MaxDelay == 0 {
		cfg.MaxDelay = 5 * time.Minute
	}
	if cfg.Jitter == "" {
		cfg.Jitter = JitterNone
	}
	matchers := append([]ErrorMatcher(nil), cfg.RetryableErrors...)
	return func(a RetryAttempt) RetryDecision {
		if a.Attempt >= cfg.MaxAttempts {
			return RetryDecision{}
		}
		if !errorRetryable(a.Err, matchers) {
			return RetryDecision{}
		}
		base := math.Min(
			cfg.InitialDelay.Seconds()+cfg.Increment.Seconds()*float64(a.Attempt-1),
			cfg.MaxDelay.Seconds(),
		)
		return RetryDecision{Retry: true, Delay: finalizeDelay(base, cfg.Jitter)}
	}
}
