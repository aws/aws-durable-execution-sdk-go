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
package durable
