# Release Notes

## Unreleased

### Breaking: operation errors expose the recorded failure; the cause is a stand-in

Every typed operation error (`StepError`, `InvokeError`, `CallbackError`,
`ChildContextError`, `WaitForConditionError`) and `OperationError` now
carry `ErrorType`, `Message`, `ErrorData`, and `StackTrace`, the fields
recorded for the failure. `Err` is a stand-in rebuilt from `ErrorType`
and `Message` on the first invocation as well as on replay. It never holds
the error value the operation body returned, so `errors.As` against a
handler's own error type is now false on every invocation instead of true
on the first and false on replay. Match on `ErrorType` instead:

```go
var stepErr *durable.StepError
if errors.As(err, &stepErr) && stepErr.ErrorType == "CardDeclinedError" { ... }
```

An SDK error that escapes a child context is rebuilt as that type from the
record, so `errors.As` against `*StepError` or `*CombinatorError` through a
`ChildContextError` works on both invocations. Sentinels
(`ErrCallbackTimedOut`, `ErrInvokeTimedOut`, `ErrExecutionStopped`,
`ErrExecutionCancelled`) still match through the stand-in.

`Error()` strings of typed errors now end in `<ErrorType>: <message>` on
the first invocation too, matching the replay form.

### New: `WithErrorData`

`durable.WithErrorData(err, data)` attaches a string payload to an error.
When the error escapes a step, a child context, a wait-for-condition
check, or the handler, the SDK records the payload as the failure's
`ErrorData`, and the typed error exposes it as `ErrorData` on both
invocations. Payloads over 256 KiB are truncated on a UTF-8 boundary.

### New: stack traces for step and handler failures; `WithStackTraces`

A failed step body or a failed handler now records a stack trace with
the failure. The trace is captured where user code hands the error to
the SDK: at the step body's return, at the handler's return, or inside
the recovery of a panic in either. It is written to the operation's
checkpoint and to the FAILED invocation response, and the typed
operation errors expose it as `StackTrace`. Each frame is one string of
the form `function file:line`, innermost frame first. At most
`MaxStackTraceFrames` (32) frames are kept.

Capture is on by default. `durable.WithStackTraces(false)` disables it,
so failures carry no `StackTrace` on any path. The wire `StackTrace`
field is a list of frame strings; earlier builds encoded a single
string, which no code path populated.

### New: callback failure subtypes

`CallbackExternalError` (the external system reported failure),
`CallbackTimeoutError` (with a `Heartbeat` field distinguishing a
heartbeat timeout from the overall timeout), and `CallbackSubmitterError`
(the `WaitForCallback` submitter step failed) embed `CallbackError`, so
`errors.As` against `*CallbackError` still matches all of them. Each has
its own wire `ErrorType`, shared with the other Durable Execution SDKs. A
failed submitter previously surfaced as a bare `StepError`; it is now a
`CallbackSubmitterError` whose `ErrorType` and `Message` are the
submitter's.

### `Settled` keeps the error type

`Settled[O]` (from `AllSettled`) now serializes the error's type and
`OperationError` fields alongside its message. After a checkpoint, an SDK
error is rebuilt as its type, so `errors.As` matches it; an unknown type
yields a stand-in carrying the name and message. Values written in the
older message-only form still deserialize. Detail fields outside
`OperationError` (such as `StepError.Attempts` or
`ResultTooLargeError.SizeBytes`) are zero after the round trip; a rebuilt
`NonDeterministicReplayError` or `ResultTooLargeError` keeps the recorded
text as its `Error()`, and a rebuilt `BatchCompletionError` recovers its
`Reason`.

Checkpoints that record a callback timeout under the older
`Callback.Timeout` or `Callback.Heartbeat` names still read back as a
`CallbackTimeoutError`; the older heartbeat name sets `Heartbeat`.

### Breaking: `WaitForCallback` takes `WaitForCallbackOption`

`WithSubmitterRetry` configures the submitter step, which only
`WaitForCallback` has. It now returns a `WaitForCallbackOption`, and
`WaitForCallback` accepts `...WaitForCallbackOption`. Every
`CallbackOption` (`WithCallbackTimeout`, `WithCallbackHeartbeatTimeout`,
`WithCallbackSerdes`) is also a `WaitForCallbackOption`, so calls that
pass options inline are unchanged. Passing `WithSubmitterRetry` to
`CreateCallback`, which previously compiled and was ignored, is now a
compile error. A `[]durable.CallbackOption` spread into `WaitForCallback`
must become a `[]durable.WaitForCallbackOption`.

`WithCallbackSerdes` passed to `WaitForCallback` now decodes the callback
payload; it was previously ignored on that path.

The documentation for `Deserializer` and `WithCallbackDeserializer`
stated that callbacks default to a raw-string passthrough. The
implementation has always decoded payloads with the handler-level
`Serdes` (default `encoding/json`), and the documentation now says so.
This is a deliberate difference from the other Durable Execution SDKs,
whose callbacks return the raw payload string by default: in Go the
callback result is typed, so the payload goes through the same decoding
as every other operation result.

### Breaking: `RetryStrategy` takes a `RetryAttempt` struct

`RetryStrategy` is now `func(RetryAttempt) RetryDecision` instead of
`func(err error, attempt int) RetryDecision`. `RetryAttempt` carries the
failing error as `Err`, the 1-based attempt number as `Attempt`, and the
time since the first attempt began as `Elapsed` (currently always zero;
the field is present so it can be filled later). A struct parameter lets
future releases add fields without breaking existing strategies.

Custom strategies change from

```go
func(err error, attempt int) durable.RetryDecision { ... }
```

to

```go
func(a durable.RetryAttempt) durable.RetryDecision { ... }
```

and read `a.Err` and `a.Attempt` in place of the former parameters.
Strategies built with `NewRetryStrategy`, `MustNewRetryStrategy`,
`ExponentialBackoff`, `LinearBackoff`, `MustLinearBackoff`, and `NoRetry`
are unaffected from the caller's side. Like `RetryConfig` and
`RetryDecision`, `RetryAttempt` must be constructed with keyed fields.

### Breaking: AWS SDK and aws-lambda-go types removed from the exported surface

The exported `durable` API now names only this SDK's own types and the
standard library. The AWS SDK for Go v2 and aws-lambda-go remain
implementation details behind one internal adapter each. Each removed or
changed symbol and its replacement:

- `ExecutionClient.GetDurableExecutionState` (taking
  `aws-sdk-go-v2/service/lambda` input and output structs) is replaced by
  `ExecutionClient.GetExecutionState`, declared in this SDK's own
  `GetExecutionStateInput` and `GetExecutionStateOutput` types.
- `ExecutionClient.CheckpointDurableExecution` is replaced by
  `ExecutionClient.Checkpoint`, declared in this SDK's own
  `CheckpointInput` and `CheckpointOutput` types.
- Because of the two changes above, a
  `github.com/aws/aws-sdk-go-v2/service/lambda.Client` no longer satisfies
  `ExecutionClient` directly. The SDK adapts to the AWS client internally;
  custom implementations (test fakes, proxies) now implement the interface
  in this SDK's own types, with no AWS SDK dependency. The supporting
  types (`Operation`, `OperationUpdate`, detail and option structs, and
  the `OperationType`, `OperationAction`, and `OperationStatus` values)
  are exported from the `durable` package.
- `Context.LambdaContext()` is removed. Use `Context.RequestID()` and
  `Context.InvokedFunctionARN()`. If the raw
  `*lambdacontext.LambdaContext` is genuinely needed, it is still
  reachable from the embedded `context.Context` via
  `lambdacontext.FromContext(ctx)`.
- `Wrap` now returns `func(context.Context, []byte) ([]byte, error)`
  instead of `aws-lambda-go/lambda.Handler`. `Start` is unchanged and
  remains the recommended entry point. Callers composing their own entry
  point adapt the returned function to their runtime shim's raw byte
  handler interface; see the `Wrap` documentation.
