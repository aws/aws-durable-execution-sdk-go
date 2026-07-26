// Package durable provides the AWS Lambda Durable Functions programming
// model for Go.
//
// A durable function executes as a series of checkpointed operations.
// Completed operation results are persisted, and after a suspension or
// interruption the function replays: previously completed operations return
// their stored results without re-executing, and execution continues from
// the first incomplete operation.
//
// Handlers receive a [Context] in place of the standard Lambda context and
// invoke durable operations through the package-level generic functions such
// as [Step], [Wait], [Invoke], [Map], and [Parallel]. Blocking variants
// return results directly. Async variants return a [Future] and run the
// operation body concurrently.
//
// # Determinism
//
// Replay pairs stored results with operations positionally, so code between
// checkpoints must be a pure function of the handler input and previously
// checkpointed results. Durable operations must be created in a
// deterministic order on each context. Use [Go] to fan out durable work
// instead of the go statement, and do not create durable operations while
// iterating a map (map iteration order is randomized; sort keys first).
// Nondeterminism inside a step body is safe: the step's checkpointed result
// is frozen once persisted.
//
// # Error Propagation
//
// Always return an error from a durable operation immediately, as ordinary
// Go code does. Do not ignore it, inspect it for a specific cause, or
// treat it as a business-level failure of a wait or invoke. A nil error
// means the operation completed (either live or on replay). A non-nil error
// means either the operation failed with an operational error (such as
// [*StepError] or [*InvokeError]) or that the invocation is suspending and
// the handler should unwind. In both cases the correct response is the
// same: return the error to the caller.
//
// If code swallows the error and continues, the invocation still suspends
// (it does not produce an incorrect result), but statements after the
// swallowed error execute, and subsequent durable operations on the same
// context refuse to proceed. This produces confusing behavior: the handler
// appears to run past the blocking point yet later operations fail. Return
// the error to avoid this.
//
//	// Correct: propagate the error immediately.
//	result, err := durable.Step(ctx, "charge", func(sc durable.StepContext) (Receipt, error) {
//		return chargeCard(sc, order)
//	})
//	if err != nil {
//		return OrderResult{}, err
//	}
//	// Use result only after confirming err == nil.
package durable
