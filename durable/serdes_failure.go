package durable

import (
	"errors"
	"fmt"
)

// ErrRetryableSerdes matches a RetryableSerdesError with errors.Is.
var ErrRetryableSerdes = errors.New("durable: retryable serdes failure")

// RetryableSerdesError wraps a serdes failure the SDK must treat as
// transient. It fails only the current invocation; the service invokes the
// execution again from its last checkpoint, so a serdes fixed by a redeploy
// within the service's retry window lets the execution continue. Use it from
// a custom Serdes for a transient cause such as an offload store write that
// timed out. For a permanent failure return the error unchanged, which is
// permanent by default.
//
// The operation whose serdes returned the error records no outcome, and the
// step retry strategy is not consulted. A step under [AtLeastOncePerRetry]
// runs its body again on the next invocation. A step under
// [AtMostOncePerRetry] finds its attempt started and treats it as
// interrupted. The invocation ends with the error even when handler code
// catches it.
func RetryableSerdesError(err error) error {
	return &retryableSerdesError{err: err}
}

// retryableSerdesError is the error [RetryableSerdesError] returns.
type retryableSerdesError struct {
	err error
}

func (e *retryableSerdesError) Error() string {
	if e.err == nil {
		return ErrRetryableSerdes.Error()
	}
	return ErrRetryableSerdes.Error() + ": " + e.err.Error()
}

func (e *retryableSerdesError) Unwrap() error { return e.err }

func (e *retryableSerdesError) Is(target error) bool { return target == ErrRetryableSerdes }

// serdesInvocationEnd is the error an invocation ends with when an
// operation's serdes returned a [RetryableSerdesError]. It names the
// operation and the direction that failed, and wraps the serdes's error,
// so errors.Is still matches [ErrRetryableSerdes]. Its scope is
// [ErrorScopeInvocation]; see [failureScope].
type serdesInvocationEnd struct {
	operation string
	direction string
	err       error
}

func (e *serdesInvocationEnd) Error() string {
	return fmt.Sprintf("durable: serdes %s failed for operation %q, ending the invocation: %v", e.direction, e.operation, e.err)
}

func (e *serdesInvocationEnd) Unwrap() error { return e.err }

// isSerdesInvocationEnd reports whether err ends the invocation because a
// serdes returned a [RetryableSerdesError].
func isSerdesInvocationEnd(err error) bool {
	var end *serdesInvocationEnd
	return errors.As(err, &end)
}

// serdesFailure returns the error an operation reports when its serdes
// failed to convert a value. Every Marshal and Unmarshal call on an
// operation's serdes reports failure through this function, so the two
// outcomes below are decided in one place.
//
//  1. The serdes error matches [ErrRetryableSerdes]. The failure is
//     transient, so the operation must record nothing. serdesFailure halts
//     the checkpointer with a [serdesInvocationEnd] and returns it. The
//     halt refuses every later checkpoint of this invocation, including a
//     FAIL a parent child context or batch would write for it. After the
//     handler returns, the invocation ends with the error whatever the
//     handler returned, and the service invokes the execution again.
//  2. Any other serdes error is permanent. serdesFailure returns a
//     [*SerdesError]. The operation fails with it once and never retries
//     it. A step records it through [failStep]; every other operation
//     returns it to the caller, who may catch it.
func (ec *execContext) serdesFailure(operation, direction string, cause error) error {
	if !errors.Is(cause, ErrRetryableSerdes) {
		return newSerdesError(operation, direction, cause)
	}
	end := &serdesInvocationEnd{operation: operation, direction: direction, err: cause}
	if ec != nil && ec.checkpointer != nil {
		ec.checkpointer.halt(end)
	}
	return end
}

// decodeLiveResult decodes serialized with serdes into an O, the way replay
// decodes the recorded payload, so the value an operation returns live
// equals the value it returns on replay. On failure it returns the error
// [execContext.serdesFailure] reports for the unmarshal direction.
//
// An operation calls it before it checkpoints its success. A transient
// failure must end the invocation before any outcome is recorded, because
// a recorded success would stop the next invocation from running the body
// again. So the caller returns the error at once when
// [isSerdesInvocationEnd] reports it. A permanent failure fails the same
// way on replay, so the caller records the success and then returns the
// error.
func decodeLiveResult[O any](ec *execContext, serdes Serdes, id, operation string, serialized []byte) (O, error) {
	var out O
	if err := serdes.Unmarshal(ec.Context, ec.serdesCtx(id), serialized, &out); err != nil {
		var zero O
		return zero, ec.serdesFailure(operation, serdesDirectionUnmarshal, err)
	}
	return out, nil
}

// roundTripReplayedResult marshals result with serdes and returns the
// value [decodeLiveResult] decodes from those bytes, together with the
// bytes. Replay of a result too large to store runs the body again, so it
// holds the raw body value. The first run returned the decoded value. So
// this conversion applies the same round trip, and the value replay
// returns equals the value the first run returned.
func roundTripReplayedResult[O any](ec *execContext, serdes Serdes, id, operation string, result O) (O, []byte, error) {
	serialized, err := serdes.Marshal(ec.Context, ec.serdesCtx(id), result)
	if err != nil {
		var zero O
		return zero, nil, ec.serdesFailure(operation, serdesDirectionMarshal, err)
	}
	out, err := decodeLiveResult[O](ec, serdes, id, operation, serialized)
	if err != nil {
		return out, nil, err
	}
	return out, serialized, nil
}

// reportSerdesError passes err through [execContext.serdesFailure] when it
// is a [*SerdesError] built by a conversion helper that has no execution
// context, so a retryable serdes failure ends the invocation there too.
// Any other error is returned unchanged.
func (ec *execContext) reportSerdesError(err error) error {
	var se *SerdesError
	if errors.As(err, &se) && errors.Is(se.Err, ErrRetryableSerdes) {
		return ec.serdesFailure(se.Operation, se.Direction, se.Err)
	}
	return err
}
