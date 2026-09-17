# Release Notes

## Unreleased

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
