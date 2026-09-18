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
// # Goroutine Ownership
//
// Every [Context] is owned by the goroutine it was created on: the handler
// goroutine owns the root Context, and a child goroutine started by [Go]
// or [RunInChildContextAsync] owns the child Context it receives. Durable
// operations on a Context must be invoked from its owning goroutine. Two
// goroutines claiming operations on one Context would claim them in a
// scheduling-dependent order, and replay would then pair stored results
// with the wrong operations.
//
// The rule is enforced at run time. Every durable operation checks the
// calling goroutine against the Context's owner before it claims an
// operation ID, and a call from any other goroutine fails with
// [ErrWrongGoroutine] without claiming an ID or recording a checkpoint. The
// check costs a few microseconds per operation (see the benchmark in the
// package tests); build with -tags durablenocheck to compile it out, at the
// price of leaving foreign-goroutine calls undetected.
//
// [Go] is the replay-safe way to run durable work concurrently. It claims
// the child's operation ID on the calling goroutine, so the order is
// deterministic, and then starts a goroutine that owns a fresh child
// Context. Use the child Context inside the function, never the parent:
//
//	fut := durable.Go(ctx, "work", func(child durable.Context) (T, error) {
//		return durable.Step(child, "step", func(durable.StepContext) (T, error) {
//			return doWork()
//		})
//	})
//	result, err := fut.Result()
//
// # Error Propagation
//
// Return an error from a durable operation immediately unless the handler
// intentionally handles it as a business-level outcome. A nil error means
// the operation completed, either live or on replay. A non-nil error is one
// of two things: a documented terminal failure of that operation, or a
// signal that the invocation is suspending and the handler should unwind.
//
// The two are distinguishable. Every terminal failure is matchable with
// [errors.As] against a public type such as [StepError] or [InvokeError],
// and all of them also match [OperationError]. A suspension signal matches
// none of them. An error that matches no public type therefore MUST be
// returned unchanged.
//
//	// Correct: handle a documented terminal failure, propagate anything else.
//	result, err := durable.Step(ctx, "charge", func(sc durable.StepContext) (Receipt, error) {
//		return chargeCard(sc, order)
//	})
//	var stepErr *durable.StepError
//	switch {
//	case err == nil:
//		// Use result.
//	case errors.As(err, &stepErr) && stepErr.ErrorType == "CardDeclinedError":
//		// A business-level decision belongs here.
//	default:
//		return OrderResult{}, err
//	}
//
// A typed failure never carries the original error value returned by the
// operation body. Its Err field is a stand-in rebuilt from the recorded
// ErrorType and Message, on the first invocation and on replay alike, so
// [errors.As] against the handler's own error types is always false.
// Match on the ErrorType field instead, as above. Structured data travels
// with the failure through [WithErrorData]. See [OperationError].
//
// [Wait] is stricter. Its error carries no business-level terminal result,
// so return it immediately in every case.
//
// If code swallows a suspension signal and continues, the invocation still
// suspends and does not produce an incorrect result, but statements after
// the swallowed error execute and subsequent durable operations on the same
// context refuse to proceed. The handler appears to run past the blocking
// point while later operations fail.
//
// # Struct Literals
//
// Exported configuration and result structs such as [RetryConfig],
// [ConditionConfig], [Branch], [BatchItem], and [Settled] begin with a
// blank zero-size field of type [0]func(). The field lets the SDK add fields
// to these structs later without breaking user code. It has four visible
// effects:
//
//   - An unkeyed literal such as Branch[string]{"name", fn} fails to compile
//     outside this package. Use keyed fields: Branch[string]{Name: "name",
//     Func: fn}.
//   - The struct is not comparable. Operators and functions that require a
//     comparable type, such as ==, [slices.Contains], and [slices.Index], do
//     not compile over it. The *Func variants such as [slices.ContainsFunc]
//     still work. This is intended, because adding a slice or func field
//     later would otherwise remove comparability and break callers.
//   - The fmt verb %+v prints the blank field as _:[].
//   - Copying, JSON encoding, [reflect.DeepEqual], [errors.Is], [errors.As],
//     and use as a map value are unaffected.
package durable
