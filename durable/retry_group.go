package durable

import (
	"errors"
	"fmt"
)

// RetryOption configures a [Retry] operation.
type RetryOption interface {
	applyRetryOption(*retryOptions)
}

// WithAttemptChildContext sets whether [Retry] runs each attempt in its own
// child context. The default is true. See [Retry] for what changes when
// attempts run directly in the caller's context instead.
func WithAttemptChildContext(enabled bool) RetryOption {
	return retryOptionFunc(func(o *retryOptions) { o.childContext = enabled })
}

// WithAttemptChildOptions supplies [ChildOption] values for the child
// context [Retry] creates for each attempt. [WithChildSerdes] selects the
// serializer for the attempt's result, [WithChildSummary] the summary of
// an oversized result, and [WithChildErrorMapper] the mapping of a failed
// attempt's [*ChildContextError] before the retry strategy sees it. The
// options are ignored when [WithAttemptChildContext] is false.
func WithAttemptChildOptions(opts ...ChildOption) RetryOption {
	return retryOptionFunc(func(o *retryOptions) { o.childOpts = append(o.childOpts, opts...) })
}

type retryOptions struct {
	childContext bool
	childOpts    []ChildOption
}

type retryOptionFunc func(*retryOptions)

func (f retryOptionFunc) applyRetryOption(o *retryOptions) { f(o) }

// Retry runs fn until it succeeds or strategy stops retrying, suspending
// the execution between attempts. It is retry for a group of durable
// operations: where [Step] retries one function that must not create
// durable operations, fn may call [Step], [Wait], [Invoke],
// [WaitForCallback], or any other operation, and a failed attempt re-runs
// the whole of fn from its beginning. attempt is the 1-based number of the
// attempt fn is running.
//
// By default each attempt runs in its own child context, named
// "<name>-attempt-<n>", so the operations of one attempt are recorded
// under that attempt and their identity does not depend on how far an
// earlier attempt got before it failed. A failed attempt returns a
// [*ChildContextError], and that error is what strategy receives as
// [RetryAttempt.Err]; its ErrorType names the error that escaped fn. The
// error is rebuilt from the recorded failure on the first invocation and
// on replay alike, so strategy sees the same input on both.
// [WithAttemptChildContext] turns the per-attempt child context off. Then
// fn runs in ctx, its operations are recorded at the top level, and
// strategy receives the error fn returned. [WithAttemptChildOptions]
// configures the per-attempt child context.
//
// After a failed attempt strategy decides whether to retry and after what
// delay. A retry waits for the delay with [Wait], in an operation named
// "<name>-backoff-<n>", so no compute is consumed while waiting: the
// invocation ends and the execution resumes in a new invocation. A zero
// delay selects [DefaultRetryDelay]; a negative delay is an error. When
// strategy stops retrying, Retry returns a [*RetryError] carrying the
// number of attempts made and the final attempt's error.
//
// A suspension from inside fn, for example from a [Wait] or a callback
// that has not resolved, is propagated unchanged. It is never a failed
// attempt and never reaches strategy; the attempt continues in a later
// invocation. On replay a completed attempt returns its checkpointed
// outcome without running fn, so a group that already succeeded returns
// its result and a group that already failed returns its error.
//
// strategy must be deterministic, as [RetryStrategy] requires: replay
// calls it again for every recorded failed attempt, and a different
// decision would diverge from the recorded operations. Pass "" as name
// for unnamed attempt and backoff operations.
func Retry[O any](ctx Context, name string, fn func(ctx Context, attempt int) (O, error), strategy RetryStrategy, opts ...RetryOption) (O, error) {
	var zero O
	if _, ok := ctx.(*execContext); !ok {
		return zero, fmt.Errorf("durable: Retry %q: Context was not created by the SDK", name)
	}
	if fn == nil {
		return zero, fmt.Errorf("durable: Retry %q: fn must not be nil", name)
	}
	if strategy == nil {
		return zero, fmt.Errorf("durable: Retry %q: strategy must not be nil", name)
	}

	options := retryOptions{childContext: true}
	for _, o := range opts {
		o.applyRetryOption(&options)
	}

	for attempt := 1; ; attempt++ {
		result, err := runRetryAttempt(ctx, name, attempt, fn, options)
		if err == nil {
			return result, nil
		}
		// Suspension is not a failed attempt: the attempt resumes in a
		// later invocation, so the strategy must not see it.
		if errors.Is(err, errSuspendExecution) || errors.Is(err, errCheckpointTerminated) {
			return zero, err
		}

		decision := strategy(RetryAttempt{Err: err, Attempt: attempt})
		if !decision.Retry {
			return zero, newRetryError(name, attempt, err, options.childContext)
		}
		delay := decision.Delay
		if delay == 0 {
			delay = DefaultRetryDelay
		}
		if _, derr := durationToSeconds(delay); derr != nil {
			return zero, fmt.Errorf("durable: Retry %q: retry delay: %w", name, derr)
		}
		if werr := Wait(ctx, retryOperationName(name, "backoff", attempt), delay); werr != nil {
			return zero, werr
		}
	}
}

// runRetryAttempt runs one attempt of a [Retry]: in its own child context
// when options ask for one, otherwise directly in ctx.
func runRetryAttempt[O any](ctx Context, name string, attempt int, fn func(Context, int) (O, error), options retryOptions) (O, error) {
	if !options.childContext {
		return fn(ctx, attempt)
	}
	return RunInChildContext(ctx, retryOperationName(name, "attempt", attempt), func(child Context) (O, error) {
		return fn(child, attempt)
	}, options.childOpts...)
}

// retryOperationName names one operation of a [Retry] group:
// "<name>-<kind>-<attempt>", or "" when the group is unnamed.
func retryOperationName(name, kind string, attempt int) string {
	if name == "" {
		return ""
	}
	return fmt.Sprintf("%s-%s-%d", name, kind, attempt)
}

// newRetryError builds the error [Retry] returns when its strategy stops
// retrying. err is the final attempt's error and stays reachable as Err.
// The record fields describe the error that escaped fn: when the attempt
// ran in a child context, err is the [*ChildContextError] that wraps that
// error, and the record is read through it so the RetryError names the
// escaping error rather than the attempt's child context.
func newRetryError(name string, attempts int, err error, childContext bool) *RetryError {
	rec := recordOf(err)
	if childErr, ok := err.(*ChildContextError); ok && childContext {
		rec = errorRecord{errType: childErr.ErrorType, message: childErr.Message, data: childErr.ErrorData, stackTrace: childErr.StackTrace}
	}
	return &RetryError{
		Name: name, Attempts: attempts,
		ErrorType: rec.errType, Message: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace,
		Err: err,
	}
}
