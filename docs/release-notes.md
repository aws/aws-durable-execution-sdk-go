# Release Notes

## Unreleased

### Changed: stack-trace frames use the base file name

A stack-trace frame the SDK records for a failure of user code now reads
`function base.go:line`. The file component is the base name of the source
file, so a frame no longer contains the directory the binary was built in,
and the frame is the same whether or not the binary was built with
`go build -trimpath`. The function component is unchanged. A trace an error
supplies through a `StackTrace() []string` method is still recorded
unchanged. Because a frame no longer carries build paths,
`WithStackTraces(false)` is needed only to keep checkpoints smaller.

### Added: `WithStepSubType` and `WithCallbackSubType`

`durable.WithStepSubType(subType)` is a `StepOption` for `Step` and
`StepAsync`. `durable.WithCallbackSubType(subType)` is a `CallbackOption`
for `CreateCallback` and `WaitForCallback`; under `WaitForCallback` it
labels the callback the operation creates, and the `WaitForCallback`
context keeps its own subtype. Each option sets the subtype the operation
records in its checkpoints and reports in `OperationHookInfo.SubType`, so
a plugin or a reader of the execution history can tell one kind of step
or callback from another without parsing names. Without the option a step
records `Step` and a callback records `Callback`, as before.

The value follows the `WithChildSubType` rules: 1 to 32 characters from
A-Z, a-z, 0-9, hyphen, and underscore, and an empty value selects the
default. Every subtype the SDK records for its own operations is
reserved, except the operation's own default. For a step this includes
`WaitForCondition`. A value outside these rules is a configuration error
the operation returns before it claims an operation ID or sends anything.

The subtype is part of the operation's identity on replay. An invocation
that supplies a different subtype for a checkpointed step or callback
returns a `*NonDeterministicExecutionError`, so changing a subtype breaks
the executions in flight.

### Changed: callback results are returned unchanged by default

A callback created with no serializer option now returns the bytes the
external system submitted, unchanged, as the JavaScript and Python SDKs
do. Before, the SDK decoded them as JSON with the handler-level serdes.
So `CreateCallback[string]` completed with `"approved"` now returns
`"approved"` with its quotes, and completed with `42` returns `42`
instead of failing. The default is the new `RawSerdes`, which supports a
result type of `string`, `[]byte`, or `json.RawMessage`; any other result
type fails with a `*SerdesError` that names the type. `WithSerdes` no
longer applies to callback results. To keep JSON decoding, pass
`WithCallbackSerdes(JSONSerdes)` on the operation or
`WithCallbackDeserializer` on the handler. This applies to
`CreateCallback` and `WaitForCallback`.

### Changed: `WaitForCallback` stores the submitted bytes

`WaitForCallback` now stores the bytes the external system submitted,
unchanged, at its child context, as the JavaScript SDK does. Before, it
stored the deserialized result re-encoded with the handler-level serdes.
The inner callback now always uses `RawSerdes`. The callback result
serializer runs once, at the `WaitForCallback` operation, on the first
run and on replay. It is the per-operation `WithCallbackSerdes`, else the
handler-level `WithCallbackDeserializer`, else `RawSerdes`. The returned
value is unchanged. A serializer that fails to deserialize now reports a
`*SerdesError` whose `Operation` is the `WaitForCallback` name.

### Added: `RawSerdes`

`RawSerdes` is a `Serdes` that stores bytes verbatim. `Marshal` returns
the bytes of a `string`, `[]byte`, or `json.RawMessage`, and `Unmarshal`
copies the stored bytes into a `*string`, `*[]byte`, or
`*json.RawMessage`. Any other type is rejected with an error that names
the type.

### Changed: `Wait` and `WaitAsync` require at least one second

`Wait` returns the error
`durable: Wait "<name>": duration must be at least 1 second` at the call
when the duration is under one second. `WaitAsync` returns a future that
fails with `durable: WaitAsync "<name>": duration must be at least 1 second`.
Neither records an operation for the rejected call. Before, a duration of
0 sent `WaitSeconds` 0, which the service rejects, and a duration from 1 ns
to 999 ms was rounded up to one second. A negative duration now fails with
the same error. A duration of one second or more is rounded up to whole
seconds, as before.

A `WaitDecision.Delay` of 0 is now raised to one second, so
`WaitForCondition` schedules every check at least one second later. Before,
it sent a delay of 0. A positive delay under one second still rounds up to
one second.

### Changed: `ConditionConfig.WaitStrategy` is required

`WaitForCondition` now returns the error
`durable: WaitForCondition "<name>": ConditionConfig.WaitStrategy must not be nil`
at the call when `ConditionConfig.WaitStrategy` is nil. It runs no check
and records no operation. Before, a nil `WaitStrategy` selected a default
strategy that never reported the condition met. That strategy ran the
check 60 times across 60 invocations and then failed the operation. The
default, `defaultConditionWaitStrategy`, is removed. To keep its schedule,
pass `durable.MustNewWaitStrategy(durable.WaitConfig[S]{ShouldContinue: ...})`.
`InitialState` stays optional, and `NewWaitStrategy`, `MustNewWaitStrategy`,
and the `WaitConfig` defaults are unchanged.

### Added: `ParallelMixed`

`ParallelMixed` runs named branches of different result types
concurrently. Each branch is a `*TypedBranch[T]` built with
`NewTypedBranch`, and `TypedBranch.Result` reads the branch value from the
returned `BatchResult[json.RawMessage]` as a `T`. Before, a typed fan-out
with `WithMaxConcurrency` and `WithCompletion` was available only as
`Parallel[O]`, whose branches share one result type. `Parallel[any]`
decoded a struct branch into a `map[string]interface{}` and an int branch
into a `float64`.

Every `BatchOption` applies as in `Parallel`. A `ParallelMixed` call
records the same operations and stores the same payloads as a `Parallel`
over the branch values. Each branch value is marshaled once by its serdes,
set with `WithTypedBranchSerdes` or else the batch item serdes, and those
bytes are stored unchanged, so they need not be JSON. `Result` returns the
branch's item error for a failed branch and a `*BranchNotCompletedError`
for a branch that never started or was abandoned when the batch completed
early. It returns an error when the result came from a different
`ParallelMixed` call. A `WithBatchSummary` function for `ParallelMixed`
takes a `BatchResult[json.RawMessage]`.

The `parallel-heterogeneous` example uses `ParallelMixed`, and its `timer`
branch now returns the error from `Wait`.

### Changed: every combinator failure is a `PromiseCombinatorError`

A failure of `All`, `Any`, `Race`, `Join`, or `Select` is now a
`*ChildContextError` whose `ErrorType` is `"PromiseCombinatorError"` and
whose cause matches `*CombinatorError`. Before, only `Any` failed this
way. `All`, `Race`, `Join`, and `Select` returned the failing future's
error, so their `ErrorType` was that error's type, such as `"StepError"`.
So one `errors.As(err, &combErr)` check now matches a failure of any
combinator, and the execution history records the same type for each.
The JavaScript SDK records the same type.

The message of the failure is the message of the decided failure: the
first error's message for `All`, `Race`, `Join`, and `Select`. For `Any`
it is `All promises were rejected`, which is the JavaScript SDK's
message. Before, `Any` reported `all futures failed (<n> errors)`.

A failed `Select` is now recorded as `FAILED` with `ErrorType`
`"PromiseCombinatorError"`. Before, it was recorded as `SUCCEEDED` with the
winner and its failure inside the result. The winner's name is recorded
as the failure's `ErrorData`, the JSON object `{"winner":"<name>"}`, so
`winner` still names the failing branch on the first invocation and on
replay.

`AllSettled` is unchanged: a failing future resolves `AllSettled`, and
its error is in `Settled[i].Err`.

`Race` with no futures now returns an error at once and records no
operation. Before, it suspended, and no future could ever end the
suspension. `Any` with no futures fails at once with
`All promises were rejected`.

### Changed: stored JSON keeps `<`, `>` and `&` literal

The SDK no longer HTML-escapes `<`, `>` and `&` in the JSON it stores.
Before, `JSONSerdes` and the other places that store JSON wrote them as
`\u003c`, `\u003e` and `\u0026`. Stored checkpoints and the execution
history now carry the characters literally. This covers every operation
result stored through `JSONSerdes`, the handler result, the `Map` and
`Parallel` records, the `AllSettled` outcomes, and the values and
envelopes of `FileSystemSerdes`. The output matches the JavaScript SDK
2.6.0 and the Python SDK, which both store the characters literally.
Replay reads checkpoints in either form, because the two forms decode to
the same value.

### Changed: the stored form of a `Map` or `Parallel` result

A `Map` or `Parallel` batch stores its result on its own context operation
when it succeeds. The stored form now matches the one the other Durable
Execution SDKs store. Replay reads only the new form. A checkpoint in the
old form is replayed by running the items again in order.

The full payload is `{"all":[<item>,...],"completionReason":"<REASON>"}`.
A succeeded item is `{"result":<value>,"index":<n>,"status":"SUCCEEDED"}`,
where `<value>` is the bytes the item serdes produced, stored as a JSON
value rather than as a JSON string. A failed item is
`{"error":<error object>,"index":<n>,"status":"FAILED"}`. An item
abandoned when the batch completed early is
`{"index":<n>,"status":"STARTED"}`. Items carry no name. Replay names each
item by its index, as the first run did. An item serdes whose output is
not a JSON value cannot be held in this payload, so the batch stores the
summary record below instead.

`BatchItemStatus` and `CompletionReason` now encode to JSON as their
`String()` values, such as `"SUCCEEDED"` and `"ALL_COMPLETED"`, and decode
from them. An unrecognized string decodes to the zero value. The integer
constants keep their values.

A failed item's error is stored as a nested error object with the fields
`ErrorType`, `ErrorMessage`, `ErrorData`, `StackTrace`, and `Cause`. The
SDK builds `Cause` by walking the error's `Unwrap` chain. Each failed item
in the returned `BatchResult` reports the error rebuilt from that object,
on the first run and on replay alike, so both report equal errors. An SDK
error type in the chain is rebuilt as that type, so `errors.As` finds it.
The fields the object does not hold are zero: `StepError.Attempts`,
`WaitForConditionError.Attempts`, `CallbackError.CallbackID`,
`SerdesError.Direction`, and the names of operations inside the item. Any
other error in the chain is rebuilt as a value that reports the recorded
type and message, so `errors.As` against a caller's own error type no
longer matches an item error, also in `NestingFlat` mode. Match on the
recorded type with `wireErrorType`-style checks such as
`ChildContextError.ErrorType`. The SDK sentinels `ErrCallbackTimedOut`,
`ErrInvokeTimedOut`, `ErrExecutionStopped`, and `ErrExecutionCancelled`
still match with `errors.Is`.

When the full payload is larger than 262144 bytes, the batch stores a
summary record and marks the checkpoint for child replay. A `Map` stores
`{"type":"MapResult","totalCount":<n>,"successCount":<n>,"failureCount":<n>,"completionReason":"<REASON>","status":"<STATUS>","itemStatuses":"<markers>"}`.
A `Parallel` stores `"type":"ParallelResult"` and adds
`"startedCount":<n>` after `failureCount`. `itemStatuses` holds one
character per admitted item in index order: `S` for succeeded, `F` for
failed, and `-` for abandoned. A `WithBatchSummary` string is stored under
an added `summary` key. Replay re-drives each `S` and `F` item from its own
recorded operations, reports each `-` item as started, and uses the
recorded completion reason without calling `ShouldComplete` again.

### Changed: `NonDeterministicReplayError` is now `NonDeterministicExecutionError`

A replay that finds a checkpointed operation whose type, subtype, or name
differs from the operation the handler now runs failed with a
`*durable.NonDeterministicReplayError`, recorded with the `ErrorType`
`NonDeterministicReplayError`. The other Durable Execution SDKs name this
failure `NonDeterministicExecutionError`. The Go type is now
`*durable.NonDeterministicExecutionError`, and its recorded `ErrorType` is
`NonDeterministicExecutionError`. `NonDeterministicReplayError` no longer
exists.

The detail fields now state the source of each value. `ActualType`,
`ActualSubType`, and `ActualName` are now `RecordedType`,
`RecordedSubType`, and `RecordedName`: the operation the checkpoint
recorded. `ExpectedType`, `ExpectedSubType`, and `ExpectedName` are now
`CurrentType`, `CurrentSubType`, and `CurrentName`: the operation the
handler now runs.

The mismatch message named only the operation the handler now runs, so a
name-only mismatch did not show the checkpointed name. It now states both
operations as a type, subtype, and name triple:

```
durable: non-deterministic replay at step "c4ca4238a0b92382": the checkpoint recorded operation (type STEP, subtype "Step", name "a"), but the handler now runs (type STEP, subtype "Step", name "b"). The handler code changed between deployments.
```

### Added: `BatchCompletionError` for a custom-failed batch with no failed item

A `ShouldComplete` decision that failed a `Map` or `Parallel` batch with
no failed item returned a `*durable.BatchError` recorded as `BatchError`.
It now returns the new `*durable.BatchCompletionError`, recorded with the
`ErrorType` `BatchCompletionError`. Its `Reason` is
`CompletionCustomFailed`. A batch with at least one failed item still
returns a `*durable.BatchError` recorded as `BatchError`. `errors.As`
against `*durable.OperationError` matches both, and `ErrorFromObject`
rebuilds both.

### Changed: `Map` and `Parallel` return an error only when the batch fails

`Map` and `Parallel` returned a `*BatchError` whenever an item failed,
even a failure within `ToleratedFailureCount` or
`ToleratedFailurePercentage`. So `if err != nil { return err }` turned a
tolerated failure into a handler failure.

They now return a `*BatchError` only when the reason is
`CompletionFailureToleranceExceeded` or `CompletionCustomFailed`. A
tolerated failure returns the populated result and a nil error.
`BatchResult.Status()` still reports `BatchItemFailed`, and `Failed()` and
`Errors()` list the failures.

### Fixed: a batch where every item ran reports `ALL_COMPLETED`

When the last item met `MinSuccessful`, the reason was
`CompletionMinSuccessfulReached`. It is now `CompletionAllCompleted`.
`CompletionMinSuccessfulReached` means the threshold completed the batch
before every item finished.

### Changed: `ShouldComplete` is called once before the first item

`CompletionConfig.ShouldComplete` was first called after the first item
finished, so at least one item always ran. It is now also called once
before any item starts, with a `BatchProgress` whose counts are 0 and whose
items are all `BatchItemNotStarted`. A `CompleteBatch` decision at that
call runs no item and returns an empty result with the reason
`CompletionCustomSucceeded` or `CompletionCustomFailed`. A callback that
completes on a count threshold returns `ContinueBatch` at that call, so it
runs as before, with one more call.

### Changed: unnamed `Map` items and `Parallel` branches get default names

A `Map` item with no `WithItemNamer`, and a `Parallel` branch with an
empty `Name`, were recorded with an empty name. So the history and the
`operationName` log field did not identify the item, and
`BatchResult.Item` and `BatchResult.Result` could not find it by name.

Such an item is now named from its zero-based index, as in the other
Durable Execution SDKs. A `Map` item is named `map-item-<index>`, and a
`Parallel` branch `parallel-branch-<index>`. A `WithItemNamer` that
returns `""` for an index falls back to `map-item-<index>`. A non-empty
configured name is used unchanged. The recorded names of unnamed items
change accordingly.

### Fixed: replay returns the same child-context and batch-item result as the first run

A child-context result whose serialized form exceeds 256 KiB is not
stored. Replay rebuilds it by running the body again. Replay returned the
new body value without passing it through the serdes, while the first run
returned the value `Unmarshal` produced. A `Map` item or `Parallel` branch
stored in the batch aggregate went through the item serdes twice in each
direction. So a serdes whose round trip changes the value, such as one
that compresses, encrypts, or normalizes, made replay return a different
value than the first run.

Every such path now applies the configured serdes exactly once per
direction and returns the same value on the first run and on replay. That
covers `RunInChildContext`, `RunInChildContextAsync`, and `Go` with a
result over 256 KiB, the combinators `All`, `AllSettled`, `Any`, `Race`,
`Join`, and `Select` over such a result, `Retry` whose attempt returns
such a result, and every `Map` item and `Parallel` branch. A result rebuilt
by running the body again is passed through `Marshal` and then
`Unmarshal`, as on the first run.

### Changed: an operation on an enclosing context is rejected

A step body or a `RunInChildContext` body could call an operation on the
enclosing context, for example the handler's `ctx` captured in a closure.
The SDK recorded that operation beside the step or the child instead of
under it. On replay the body did not run again, so the operation IDs no
longer lined up. The execution then either kept a wrong history or failed
with a `NonDeterministicReplayError`.

Such a call now fails at the call with the new `durable.ErrWrongContext`
when the body runs on the goroutine that owns the enclosing context. That
covers a step body, a `WaitForCondition` check, a `WaitForCallback`
submitter, and the body of a blocking `RunInChildContext`. An operation on
a child context kept from a body that already returned, such as a
sibling's context, fails the same way. A body that runs on its own
goroutine, the body of `Go`, `RunInChildContextAsync`, a `Map` item, or a
`Parallel` branch, already failed such a call with
`durable.ErrWrongGoroutine`, and still does. In both cases the rejected
operation claims no operation ID and records nothing. Match the error with
`errors.Is`. An asynchronous operation reports it through its `Future`.
Building with `-tags durablenocheck` removes the check.

### Changed: every log line is written exactly once, and `Result` takes a context

A line that ran for the first time after an awaited operation was dropped
as replayed. For example, a line after the last `Wait` of a handler was
written in no invocation, and `IsReplaying` returned true on it. The SDK
decided that code was live only when the code started an operation with
no checkpoint.

A context now also becomes live when its code receives the outcome of an
operation that the previous invocation did not have. That is an operation
the invocation payload lists in `UpdatedOperationIds`, or one that
completed during the current invocation. So every line is written exactly
once, in the invocation that first runs it, and `IsReplaying` reports
false there. A line is written twice only after a failed invocation: the
next invocation writes again the lines the failed one ran after it
received an outcome.

`Future.Result`, `Callback.Result`, and the `Awaitable` interface now take
the `durable.Context` of the code that reads the outcome. Write
`fut.Result(ctx)` instead of `fut.Result()`. Inside a `Go` branch, pass
the branch's context, also for a future the parent created.

The `durabletest` local runner now sets `UpdatedOperationIds` on each
invocation as the service does: the operations whose state changed since
the last successful invocation.

### Changed: the local test runner validates checkpoints and reports completions as the service does

The `durabletest` local runner now rejects every checkpoint update the
service rejects, with the service's error code and message. A step result
or `WaitForCondition` state over 262144 bytes, an error object over 262144
bytes, an `Invoke` input over 1048576 bytes, and a handler result over 6291456
bytes now fail the execution with a `CheckpointError`. Before, the runner
stored them, and the execution reached `SUCCEEDED` or `PENDING` locally
but failed against the service.

The runner also rejects a checkpointed wait of zero seconds, as the
service does. `durable.Wait` never sends one. It rejects a duration under
one second at the call, with a plain error, and records no operation. The
runner's check applies only to a malformed checkpoint that reaches the
client directly.

The runner now fails an execution whose handler answers `PENDING` with no
pending operation four times in a row. The error type is
`InvalidParameterValueException` and the message is `Cannot return PENDING
status with no pending operations.`. Before, `RunUntilComplete` returned
the `PENDING` result.

The runner now reports an operation's completion during an invocation. Its
in-memory client keeps a virtual clock, which starts with the execution
and which wall-clock time does not move. A wait or a step retry that
becomes due while a step or a child context runs is reported in the
response to the checkpoint request that reports that work finished, or to
a poll. A callback resolved while the handler runs is reported in the
response to the next checkpoint request. A registered function runs when
its invoke starts, and the invoke's START response reports its outcome. So
the handler continues in the same invocation, as it does under the
service. Before, these completions were reported only between invocations,
so tests needed more invocations than the service does.
`CompletePendingTimers` still completes every pending timer at once.
The operation and event timestamps in a `TestResult` come from the virtual
clock, so the wall-clock time a step body takes no longer appears in them.

### Changed: payload sizes are left to the service, as in the JavaScript SDK

The SDK no longer checks a step result, a `WaitForCondition` state, or an
`Invoke` input against a size limit of its own, and `ResultTooLargeError`
is removed. The check used 768000 bytes, which is the size limit of one
checkpoint call, not a limit on one payload. The JavaScript SDK has no such
check. The SDK now sends each payload as it is, and the service applies
its own limit for that kind of payload. The service rejects a payload over
its limit, and the rejection fails the execution with a `CheckpointError`,
even when the handler catches the operation's error.

Before, an `Invoke` input between 768000 and 1048576 bytes failed although
the service accepts it. A step result over 768000 bytes went through the
retry strategy, so the step function ran up to 6 times.

### Changed: a serdes failure is permanent and catchable, with a transient opt-in

A serdes failure is now permanent by default, and no retry strategy
retries it. A step whose result its serdes cannot marshal checkpoints a
terminal failure and returns a `*StepError` whose `ErrorType` is
`"SerdesError"`, so the step body runs once. Before, the step retry
strategy retried it, and under the default `ExponentialBackoff` the body
ran 6 times. Every other operation keeps returning a `*SerdesError`.

The new `RetryableSerdesError(err)` marks a serdes failure transient, and
`ErrRetryableSerdes` matches it with `errors.Is`. The operation records no
outcome, and the SDK ends only the current invocation, even when the
handler catches the error. The service invokes the execution again from
its last checkpoint. In a `Map` or `Parallel` item it ends the invocation
instead of producing a failed item.

A handler event that does not decode into the event type, or a handler
result that does not encode, now fails the execution with a `SerdesError`
whose `Operation` is `"execution"`. Before, the invocation ended with an
error, and every later invocation failed the same way until the execution
timed out.

### Changed: a checkpoint response without a token is classified by what the call carried

A checkpoint response without a `CheckpointToken` means the service will
accept no further checkpoints from the current invocation. The SDK now
handles it the way the JavaScript SDK 2.6.0 does.

- When the call carried the execution's terminal update, the execution
  finished, and the invocation reports the terminal outcome. A result too
  large for the response now reports `SUCCEEDED` in this case. Before, it
  reported `PENDING`.
- When the handler returns a result or an error while a branch's
  checkpoint call is in flight, the invocation waits for that call. If its
  response carries no token, the invocation reports `PENDING`. Before, it
  reported the handler's outcome.
- When a response without a token suspends the invocation, the SDK writes
  one WARN record through the handler's log handler: `Checkpoint response
  contained no CheckpointToken: the service will accept no further
  checkpoints from this invocation. Suspending; the execution continues on
  the next invocation.` Replay suppression never drops it. Before, the SDK
  wrote no record.

A response without a token to a poll suspends the invocation with
`PENDING`, as for any other call that does not carry the terminal update.

### Changed: the default client sets request timeouts, and checkpoint calls are not retried by the SDK

The default Lambda client now sets a 5 second connect timeout, a 50
second response timeout, and a 55 second total request timeout. Before, it
set only the AWS SDK's 30 second connect timeout, so a call that connected
and then stalled ran until the invocation deadline.

The SDK no longer retries a checkpoint call itself. It makes one call per
batch, and the client's own retryer is the only retry. The default
client's AWS standard retryer makes up to 3 attempts for server faults,
throttling, and connection errors. Before, the SDK also retried up to 3
times, so one checkpoint could make up to 9 requests. A failure that
remains after the client's retries ends the invocation, and the service
invokes the execution again. A custom `ExecutionClient` brings its own
retry; one that does not retry gets one attempt per invocation. In the
classification rules below, "retried" refers to this client retry.

### Fixed: a suspending invocation no longer responds SUCCEEDED

When an invocation starts to suspend, the SDK returns the suspension
error from every blocked operation and from `Future.Result`. Before this
fix, a handler that discarded that error and returned could make the
invocation respond `SUCCEEDED` while an operation it started, such as a
callback, was still pending. The invocation now responds `PENDING`.

### Fixed: checkpoint failures are classified by one table, and a rejected checkpoint fails the execution

A failed checkpoint or state-load call is now classified by these rules,
applied in order. The first rule that matches decides the outcome.

1. A `*ClientError` in the error chain: its stated scope. An unknown or
   zero scope is `ErrorScopeInvocation`.
2. An API error with code `KMSAccessDeniedException`,
   `KMSDisabledException`, `KMSInvalidStateException`, or
   `KMSNotFoundException`: `ErrorScopeExecution`. The execution fails.
   These codes arrive as server faults, so before this change they ended
   the invocation and the service invoked the execution again.
3. An `InvalidParameterValueException` whose message starts with
   `Invalid checkpoint token`: a stale token. The invocation ends with an
   error and the execution continues in the newer invocation. The prefix
   is now compared case-sensitively. A message that matches only without
   regard to case, such as `invalid checkpoint token: superseded`, is an
   ordinary rejected request and fails the execution.
4. `TooManyRequestsException`: `ErrorScopeInvocation`, retried.
5. Any other server fault: `ErrorScopeInvocation`, retried.
6. Any other client fault: `ErrorScopeExecution`. The execution fails.
7. An HTTP response with no modeled error code: status 429 or status 500
   and above is `ErrorScopeInvocation`; any other status is
   `ErrorScopeExecution`. A bare 429 is throttling, so it now ends the
   invocation for a re-invoke instead of failing the execution.
8. Anything else, such as a network error or a timeout:
   `ErrorScopeInvocation`, retried.

An execution-scoped checkpoint failure now fails the execution whether or
not handler code returns the error. The SDK stops checkpointing when the
failure arrives. After the handler returns, the invocation responds
`FAILED` with the `*CheckpointError`, whatever the handler returned.
Before, a handler that caught the `*CheckpointError` and returned a value
made the execution finish `SUCCEEDED`, although the operation whose
checkpoint the service rejected was never recorded. A step result over the
service's payload limit is one such rejection. `CheckpointError.Scope()`
and `CheckpointError.Retryable()` still report the classification.

A state load that fails with an execution-scoped `*ClientError` now records
the client error's own type and message in the `FAILED` response:
`ErrorType` is `ClientError`. Before, it recorded `ErrorType` `Error` and
the message `durable: load execution state: ...`.

### Added: ErrorTypeIs matcher for recorded wire ErrorType

[ErrorTypeIs] matches an error by its recorded wire ErrorType string. Use it
(with [ErrorContains] or [ErrorMatches]) to declare which errors are
retryable inside a default [Retry]: each attempt runs in a child context,
so the strategy sees a reconstructed [*ChildContextError] whose
[ErrorAs]/[ErrorIs] identity is gone but whose ErrorType and message
survive. [ErrorAs] and [ErrorIs] still match live errors from a plain
[Step], or from [Retry] with [WithAttemptChildContext] false.

### Changed: an operation that finishes during the invocation resumes it

An awaited operation that finishes while the same invocation is still
running now resumes the goroutine that awaits it. Before, the SDK ended
the invocation with `PENDING` as soon as a handler blocked on a wait, a
callback, a chained invoke, a step retry, or a condition check. The
service rejects a `PENDING` response when nothing is pending, so a
handler whose awaited operation finished while another step ran failed
the invocation, and four such invocations in a row failed the execution.

The rules are now these:

- A checkpoint response that reports an awaited wait, callback, or invoke
  terminal resumes the goroutine blocked on it with the operation's
  outcome: the value, or the error a replay of the same record returns.
  That includes an invoke whose START response reports that it failed.
- A response that reports a step retry or a condition check `READY` runs
  its next attempt in the same invocation.
- The invocation returns `PENDING` only when every goroutine that runs
  handler code is blocked on an operation or has returned, no checkpoint
  request is queued or in flight, no step attempt, condition check, or
  child-context completion is executing, and at least one blocked
  operation is still not finished. The SDK checks again after a 20 ms
  settle period and returns `PENDING` only if all of that still holds.
- An operation that is not terminal in the initial execution state follows
  the same rules.
- `OnOperationEnd` is dispatched once, in the invocation that observes the
  completion.
- A `Step` under `AtMostOncePerRetry` whose previous attempt was
  interrupted dispatches a replayed `OnOperationStart`, and
  `OnOperationEnd` once it records the terminal failure. Before, it
  dispatched neither.

While a goroutine is blocked on an operation that is not finished, the SDK
polls its status with a `CheckpointDurableExecution` request that carries
no updates:

- The first poll is at the operation's end time: the scheduled end of a
  wait, and the next attempt time of a step retry or a condition check. A
  callback and an invoke have no end time, so their first poll is 1 s
  after the goroutine blocks.
- After the n-th poll that finds no change, the next poll is min(n, 10)
  seconds later while n is at most 95, and 60 s later from n = 96 on.
- No poll is scheduled to fire less than 1 s before the invocation
  deadline, and polling stops once less than 1 s remains.
- A status change in any checkpoint response cancels the operation's
  pending poll.
- A handler that only awaits one operation, with nothing else running,
  suspends after the settle period and sends no poll.

A poll counts as a checkpoint call for
`durabletest.LocalRunner.OmitTokenOnCheckpoint`.

### Breaking: `durabletest` runners return an error instead of taking `*testing.T`

The runner methods no longer take a `*testing.T`. They return the result
and an error:

```go
func (r *LocalRunner[I, O]) Run(event I) (*TestResult, error)
func (r *LocalRunner[I, O]) RunUntilComplete(event I, opts ...RunnerOption) (*TestResult, error)
func (r *CloudRunner) Run(ctx context.Context, event any) (*TestResult, error)
func (r *CloudRunner) RunWithArn(ctx context.Context, executionArn string) (*TestResult, error)
```

So a handler can run under the local runner from a plain `main` package,
with no `testing` import. The `CloudRunner` methods pass `ctx` to the
Lambda `Invoke` call and to every poll, so a caller can cancel a run or
set a deadline on it.

The error is non-nil, and the result nil, only when the runner itself
fails. For `LocalRunner` that is an event that does not marshal to JSON,
an invocation error that indicates an SDK or runner bug, or a response
that does not parse. For `CloudRunner` it is a failed `Invoke` call, an
invoke response with no `DurableExecutionArn`, a failed or timed-out poll,
or the end of `ctx`. A handler that returns an error still produces a nil
error and a `Failed` result. A run blocked on a callback or invoke
produces a `Pending` result, and a run that reaches the invocation cap
sets `CapReached`.

`ResultAs` on a `Failed` result now includes `TestResult.Error.Type` and
`TestResult.Error.Message` in its error message.

`AssertGoldenSignature`, `AssertGoldenSignatureUnordered`,
`AssertSignatureContains`, and `AssertSignatureExcludes` take a
`testing.TB` instead of a `*testing.T`, so benchmarks and fuzz tests can
call them.

To migrate a test, drop the `t` argument, pass a context to the
`CloudRunner` methods, and fail the test on a runner error:

```go
result, err := runner.RunUntilComplete(input)
if err != nil {
	t.Fatal(err)
}
```

### Plugin instrumentation API: dispatch contract documented

The plugin instrumentation API stays experimental. `durable.Plugin`,
`durable.WithPlugins`, `durable.WithPluginChildOperationsDepth`, the hook
info types, and the status and outcome constants carry `EXPERIMENTAL`
markers in the godoc and may change or be removed without notice.

The `durable.Plugin` godoc now states the dispatch contract in one place.
For every hook it gives when the hook fires, whether it fires on replay,
its order relative to the other hooks, and the goroutine that dispatches
it, with the single-plugin and multi-plugin cases told apart. It also
states the concurrency contract. Hooks of concurrent operations run in
parallel, and so do the hooks of different plugins for one event, so every
hook must be safe for concurrent use. A package test drives every hook from
concurrent branches under the race detector.

### Added: `WithChildSubType`

`durable.WithChildSubType(subType)` is a `ChildOption` for
`RunInChildContext`, `RunInChildContextAsync`, and `Go`. It sets the
subtype the child context records in its checkpoints and reports in
`OperationHookInfo.SubType`, so a plugin or a reader of the execution
history can tell one kind of caller-defined grouping from another. Without
the option a child context records `OperationSubTypeRunInChildContext`, as
before.

The value must be 1 to 32 characters from A-Z, a-z, 0-9, hyphen, and
underscore; an empty value selects the default. The subtypes the SDK
records for its own operations (`Step`, `Wait`, `Callback`,
`ChainedInvoke`, `WaitForCallback`, `WaitForCondition`, `Map`,
`MapIteration`, `Parallel`, `ParallelBranch`) are reserved. A value outside
these rules is a configuration error the operation returns before it
claims an operation ID.

The subtype is part of the operation's identity on replay: an invocation
that supplies a different subtype for a checkpointed child context
returns a `*NonDeterministicReplayError`. Keep it constant across
invocations and deployments.

### Added: `WithChildVirtual`

`durable.WithChildVirtual()` is a `ChildOption` for `RunInChildContext`,
`RunInChildContextAsync`, and `Go`. It makes the child context virtual:
the child groups and names the operations inside it like any child
context, but nothing is checkpointed for the wrapper, so the execution
history holds no `ContextStarted`, `ContextSucceeded`, or `ContextFailed`
event for it. The operations inside record the nearest checkpointed
ancestor as their parent and are numbered under the child's position, so
adding or removing the option around existing operations changes their
identity on replay.

This is the standalone form of the virtual context `WithNesting(NestingFlat)`
already gives the items of a `Map` or `Parallel`; the two produce the same
checkpoint shape. Because the wrapper leaves no record, the child body runs
on every invocation that reaches it, the operations inside replay from
their own checkpoints, and a suspending operation inside it resumes in a
later invocation. The child starts in its parent's replay state and, like
the parent, switches to live execution at its first operation with no
checkpoint. The result is round-tripped through the child's `Serdes`
on every run and has no size limit. A failure is a `*ChildContextError`,
or the mapped error, on every run. Plugins observe a virtual child as they
observe a checkpointed one: a start before `WrapChildContextFn` wraps the
body and an end once the body has an outcome, on every invocation that
reaches it. Because nothing is recorded, the start and the end report
`IsReplay` true on every invocation; `WrapChildContextFn` receives
`IsReplay` false when the child executes live and true when it replays the
operations inside it. The
operations inside it report the enclosing context's `ParentID`, so the
child adds no level to the depth `WithPluginChildOperationsDepth` counts.
A virtual child context inside another virtual child context is a
configuration error; one inside a `NestingFlat` item is supported.

The `child-context-virtual` and `child-context-serdes-virtual` examples
now use the option; before, they showed concurrent child contexts.

### Added: `WithPluginChildOperationsDepth`

`WithPluginChildOperationsDepth(depth)` bounds the depth in the operation
tree of the operations reported to plugins. Operations deeper than `depth`
are omitted from every operation-level notification: the start, end, and
attempt hooks, the `WrapOperationAttemptFn` and `WrapChildContextFn` wrap
hooks, and the `Operations` and `UpdatedOperations` maps of the invocation
hooks. Omission affects notifications only; an omitted operation runs,
retries, and checkpoints exactly as a reported one. The default reports
every depth, as before.

Depth counts the operations between an operation and the root: an
operation claimed on the handler's `Context` has depth 0, an operation
inside a child context has the depth of that context's operation plus one,
a `Map` or `Parallel` item has the depth of its batch plus one, and the
operations inside the item one more. `NestingFlat` items and `WithChildVirtual`
children add no level.

`OperationHookInfo` gains `ChildrenOmitted`, set on the operations at the
configured depth, so a consumer can tell that the operations inside one
were withheld rather than absent. Every reported operation's parent is also
reported, so `ParentID` chains stay complete.

### Breaking: wrap hooks pass a context to `fn`

The `fn` argument of `Plugin.WrapInvocation`, `Plugin.WrapOperationAttemptFn`,
and `Plugin.WrapChildContextFn` changes from `func() (any, error)` to
`func(ctx context.Context) (any, error)`. The context a hook passes to `fn`
becomes the parent of the context the wrapped user code observes: the
handler's `Context` for `WrapInvocation`, the `StepContext` of a step body
or condition check for `WrapOperationAttemptFn`, and the child `Context` for
`WrapChildContextFn`. A plugin that derives a context (a tracing span, a
correlation value, a scoped logger) and passes it to `fn` makes it readable
inside user code; before this change the body captured its own context and
a plugin had no way to reach it.

To migrate, pass the `ctx` the hook received to `fn`:

```go
WrapOperationAttemptFn: func(ctx context.Context, info durable.AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
	return fn(ctx)
},
```

A hook that passes its `ctx` on unchanged sees no behavior change. `fn` must
still be called exactly once; a later call returns the first call's result
and ignores the context passed to it. A nil context stands for the `ctx` the
hook received. The wrapped work stays attached to the invocation's context:
when a hook passes `fn` any context other than the one it received, the user
code observes a context that is cancelled when either is cancelled, reports
the earlier of the two deadlines, and falls back to the invocation's context
for values the hook's context lacks, whether or not the hook's context
descends from the invocation's. That merged context is cancelled once `fn`
returns, so user code must not keep using it after the wrapped function has
returned.

### Added: the `insight` and `analysis` modules are released with the SDK

Each release tags the root module `vX.Y.Z` and the nested modules
`insight/vX.Y.Z` and `analysis/vX.Y.Z` at the same commit, so
`go get github.com/aws/aws-durable-execution-sdk-go/insight@vX.Y.Z` and
`go install github.com/aws/aws-durable-execution-sdk-go/analysis/cmd/durablelint@vX.Y.Z`
resolve. `insight/vX.Y.Z` declares a dependency on the root module
`vX.Y.Z`; keep the two at the same version. See "Releasing" in
`CONTRIBUTING.md`.

### Added: `ConfigureLogging` reconfigures the logger inside the handler

`ConfigureLogging(ctx, LogConfig{...})` replaces the `slog.Handler` behind
`Context.Logger()` and `StepContext.Logger()` for the rest of the
invocation, for a handler that must choose its logger from the event
payload or runtime configuration rather than at construction time. The
SDK attaches the same fields to the new handler as to the construction
time one: `requestId`, `executionArn`, `tenantId`, the operation scope
fields, and plugin fields from `EnrichLogContext`, and it wraps the new
handler with replay suppression.

Scope: the new settings apply to the calling context and to every child
context and branch derived from it after the call. A context derived
before the call keeps the settings it was derived with. Settings last for
the current invocation only; the next invocation starts from the
construction-time options again. The call claims no operation and writes
no checkpoint, so it may be made conditionally without affecting replay.
Like `ConfigureSerdes`, it must run on the goroutine that owns the context.

### Added: `ReplayLogMode` and `WithReplayLogMode` control replay suppression

Log records emitted while a context replays checkpointed operations are
dropped by default (`ReplayLogModeSuppress`). `ReplayLogModeEmit`,
selected with the `WithReplayLogMode` handler option or the
`LogConfig.ReplayLogMode` field of `ConfigureLogging`, emits them instead,
each with the field `replay: true`; live records carry no `replay` field.
The mode is for diagnosing a replay problem: expect every line written
before a suspension to appear again on each later invocation that replays
it. The top-level `replay` key is reserved in every mode: a plugin field or a
user attribute (from `Logger.With` or a record) under that name is dropped,
like the other SDK fields; the same name inside a user group is kept.

### Breaking: `Context.Logger()` returns `*slog.Logger`; `WithLogHandler` replaces `WithLogger`

The SDK logs through the standard library's `log/slog`. `Context.Logger()`
and `StepContext.Logger()` return a `*slog.Logger`, and the extension
point is an `slog.Handler` set with `WithLogHandler`. Each removed symbol
and its replacement:

- `Logger` (the four-method interface) is removed. Use `*slog.Logger`. Its
  `Debug`, `Info`, `Warn`, and `Error` methods take the same message and
  alternating key-value pairs, so most call sites compile unchanged. A
  level computed at runtime goes through `Logger.Log(ctx, level, msg, ...)`.
- `WithLogger(Logger)` is replaced by `WithLogHandler(slog.Handler)`. A
  logging library that ships an `slog.Handler` plugs in directly.
- `NopLogger` is replaced by `WithLogHandler(slog.DiscardHandler)`.
- `WriterLogger` and `NewWriterLogger(w)` are replaced by
  `WithLogHandler(slog.NewJSONHandler(w, nil))` or any other handler.

The default handler writes JSON to stderr with the field names the other
durable execution SDKs use, so one CloudWatch query works across
languages: `timestamp` (ISO 8601 UTC with millisecond precision and a Z
suffix), `level` (`DEBUG`, `INFO`, `WARN`, `ERROR`), `message`,
`requestId`, `executionArn`, `tenantId` (when the invocation has one), and
inside an operation scope `operationId` and `operationName` (when named).
A child context from `RunInChildContext`, `Go`, `Map`, `Parallel`, or
`WaitForCallback` is one scope; a step body, condition check, or callback
submitter is another and also carries `attempt`. Each scope carries its
own identifiers only, so a step inside a child context reports the step,
not the child. An attribute whose value is
an `error` is expanded into `errorType` and `errorMessage`, plus
`stackTrace` when the error carries recorded frames. slog's `time` and
`msg` keys do not appear.

The default handler's minimum level is read from `AWS_LAMBDA_LOG_LEVEL`
(`TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`, case insensitive).
Unset or unrecognised selects `INFO`; the previous default logger emitted
`DEBUG` records. Records below the level are dropped before formatting.
A supplied handler applies its own level.

The execution and operation identifiers reach a supplied handler through
its `WithAttrs` method, as structured attributes rather than text in the
message. Replay suppression wraps whichever handler is installed: while a
context replays checkpointed operations, its records are dropped before
they reach the handler, per branch as before.

The new example `logger-slog-handler` installs an application handler
with snake_case field names and a service field.

### Fixed: `Plugin.EnrichLogContext` fields now reach log records

The hook was declared but never called. Fields a plugin returns from
`EnrichLogContext` now become attributes of every record emitted through
`Context.Logger()` and `StepContext.Logger()`, in handler bodies, child
contexts, and step bodies alike. The hook runs once per emitted record,
after replay suppression, and receives the record's context.

Precedence, highest first: the SDK's own fields (`timestamp`, `level`,
`message`, `requestId`, `executionArn`, `tenantId`, `operationId`,
`operationName`, `attempt`), then attributes the user passed with the
record or attached through `slog.Logger.With`, then plugin fields. A plugin
field under a taken key is dropped. Keys are compared by qualified path:
plugin fields land under the groups the logger has opened with
`slog.Logger.WithGroup` and collide only with an attribute at that same
path, with the children of an empty-key group counting at the enclosing
path. A group opened after an attribute was attached at that same path is
also taken, so no plugin field is added under it. When several plugins implement the
hook, their maps are merged in registration order and a later plugin's
value replaces an earlier one's under the same key. Fields are added in
key order. A hook that panics contributes no fields and does not fail the
invocation. When no plugin implements the hook, no per-record work is done.

### Added: `BuildPreview` and `FileSystemSerdesConfig.GeneratePreview`

`BuildPreview(value, PreviewConfig)` returns a compact, redacted
`map[string]any` view of a value. `FileSystemSerdesConfig` gains
`GeneratePreview`, a hook the filesystem serdes calls for each value it
writes to a file; a non-nil result is stored in the checkpoint envelope as
the `preview` member next to the file reference. The operation log then
shows the preview without reading the file.

`PreviewConfig` selects a base mode, `PreviewIncludeAll` (the default) or
`PreviewExcludeAll`, and layers `Include`, `Exclude`, and `Mask` selector
lists on top. Exclude wins over the other two. A masked field is shown with
its value replaced by `MaskString` (default `***`). Each selector is a
`PreviewField` whose `Match` is `FieldMatchAnywhere` (the default, matching
the name at any depth) or `FieldMatchPath` (matching one exact dot-separated
path from the root). Fields are addressed by their JSON names.

The result is capped at `MaxPreviewBytes` (default 4096) of JSON. When the
cap leaves fields out, the preview carries `"$truncated": true`
(`PreviewTruncatedKey`) so truncation is visible. Traversal stops at
`MaxDepth` (default 32) nested objects and slices. Slices do not appear as
slices: the fields of each element merge under the slice's path. A value
that `encoding/json` cannot encode, including a cyclic one, yields a nil
preview.

A preview is advisory metadata. Masking or excluding a field in the preview
does not redact it from the offloaded file, which holds the value in full.
This is the SDK's only masking facility, and it applies to previews alone.

An envelope written without a preview reads back unchanged, and an envelope
with a preview reads back through a serdes that has no generator, so the
hook can be added to or removed from a deployed function while executions
are running.

```go
serdes := durable.NewFileSystemSerdes("/mnt/efs", durable.FileSystemSerdesConfig{
	GeneratePreview: func(v any) map[string]any {
		return durable.BuildPreview(v, durable.PreviewConfig{
			Mode:    durable.PreviewExcludeAll,
			Include: []durable.PreviewField{{Name: "id"}, {Name: "customer.email", Match: durable.FieldMatchPath}},
			Mask:    []durable.PreviewField{{Name: "ssn"}},
		})
	},
})
```

The examples `serde-preview-truncation` and `serde-preview-field-selection`
show both base modes end to end.

### Added: `FileSystemSerdesConfig.PathEncoding`; the default file layout is now readable

`FileSystemSerdesConfig` gains `PathEncoding`, which chooses where the
filesystem serdes writes each offloaded value under the base path.

`FileSystemPathEncodingURI` is the new default. Files go under
`<functionName>/<executionName>/<invocationId>/<operationID>.json`, taken
from the execution ARN, so a stored payload can be found by browsing the
mount. Every segment is percent-encoded: characters outside letters,
digits, `-`, `_`, `.`, and `~` become `%XX`, and a segment that would be
`.` or `..` has its dots encoded, so no identifier can name a path outside
its directory. An ARN without the durable-execution shape is encoded whole
into a single directory segment.

`FileSystemPathEncodingHash` is the layout earlier releases always used,
unchanged: the directory is the hex encoding of the first 16 bytes of the
ARN's SHA-256 digest and the file is `<operationID>.json`.

Effect on files written by earlier releases: they remain readable. The
checkpoint envelope stores the full path of each file, so `Unmarshal`
reads a file back whatever layout was in effect when it was written and
whatever layout the serdes is configured with now. Only the location of
new files changes. A file that an earlier release wrote for an execution
that is still running is not moved; a later write of the same operation
under the new default lands at the new location and the old file is left
in place. To keep writing new files at the earlier locations, set
`PathEncoding: durable.FileSystemPathEncodingHash`.

```go
serdes := durable.NewFileSystemSerdes("/mnt/efs", durable.FileSystemSerdesConfig{
	PathEncoding: durable.FileSystemPathEncodingHash,
})
```

When the `SerdesContext` carries no execution ARN or operation ID, the path
is derived from a hash of the value bytes under both layouts, as before.

### Added: `JSONSerdes` exports the default serializer

`JSONSerdes` is the `Serdes` the SDK uses when no serializer option is
supplied. It encodes with `encoding/json`, holds no state, and is safe for
concurrent use. A custom serdes that handles a few types itself can now
defer every other type to it instead of reimplementing the default:

```go
func (unixTimeSerdes) Marshal(ctx context.Context, meta durable.SerdesContext, v any) ([]byte, error) {
	if t, ok := v.(time.Time); ok {
		return []byte(strconv.FormatInt(t.UnixNano(), 10)), nil
	}
	return durable.JSONSerdes.Marshal(ctx, meta, v)
}
```

### Added: `ConfigureSerdes` replaces serializer defaults inside the handler

`ConfigureSerdes(ctx, SerdesConfig{...})` replaces the handler-level
default `Serdes` and callback `Deserializer` for the rest of the
invocation, for a handler that must pick its serializer from the event
payload rather than at construction time. A nil `SerdesConfig` field keeps
the current value. The new defaults apply to operations started on `ctx`
after the call and to child contexts and branches derived after the call;
per-operation options such as `WithStepSerdes` still take precedence.

The call must run identically on every invocation of an execution,
including replays: a checkpointed result is decoded with the serdes in
effect at replay time, so a configuration that differs between invocations
makes the checkpoint unreadable and the operation fails with a
`SerdesError`. Like every durable operation, `ConfigureSerdes` must be
called on the goroutine that owns `ctx`.

```go
func handler(ctx durable.Context, event OrderEvent) (OrderResult, error) {
	if event.Compressed {
		if err := durable.ConfigureSerdes(ctx, durable.SerdesConfig{Serdes: gzipSerdes{}}); err != nil {
			return OrderResult{}, err
		}
	}
	...
}
```

### Added: `SerdesOf` builds a `Serdes` from typed functions

`SerdesOf[T]` adapts a marshal function taking `T` and an unmarshal
function returning `T` to the untyped `Serdes` interface. The SDK performs
the type assertion once: `Marshal` with a value that is not `T`, or
`Unmarshal` with a target that is not `*T`, returns an error naming both
the expected and the actual type. `T` is inferred from the function
arguments.

```go
masked := durable.SerdesOf(
	func(_ context.Context, _ durable.SerdesContext, r Receipt) ([]byte, error) {
		r.Card = "****" + r.Card[len(r.Card)-4:]
		return json.Marshal(r)
	},
	func(_ context.Context, _ durable.SerdesContext, b []byte) (Receipt, error) {
		var r Receipt
		return r, json.Unmarshal(b, &r)
	},
)
receipt, err := durable.Step(ctx, "charge", chargeCard, durable.WithStepSerdes(masked))
```

The `Serdes` interface and every `With*Serdes` option are unchanged. A
`SerdesOf` serdes set handler-wide with `WithSerdes` serves every operation
result, so an operation whose result is not `T` fails with a `SerdesError`.

### Added: `NewWaitStrategy` builds a `WaitForCondition` wait strategy

`WaitConfig` and `NewWaitStrategy` are to `WaitForCondition` what
`RetryConfig` and `NewRetryStrategy` are to `Step`: declarative exponential
backoff instead of a hand-written function. `WaitConfig` takes
`MaxAttempts`, `InitialDelay`, `MaxDelay`, `BackoffRate`, and `Jitter` with
the same meanings as `RetryConfig`, plus a `ShouldContinue` predicate that
reports whether to keep polling given the state the latest check returned.
A met condition wins over exhaustion, so a condition met on the final
permitted attempt still succeeds. Reaching `MaxAttempts` with the condition
unmet fails the operation with a `*WaitForConditionError` rather than
returning the intermediate state. `MustNewWaitStrategy` panics on invalid
configuration; `NewWaitStrategy` returns the same error.

```go
status, err := durable.WaitForCondition(ctx, "await-delivery", check,
	durable.ConditionConfig[string]{
		InitialState: "IN_TRANSIT",
		WaitStrategy: durable.MustNewWaitStrategy(durable.WaitConfig[string]{
			MaxAttempts:    20,
			InitialDelay:   time.Minute,
			MaxDelay:       15 * time.Minute,
			ShouldContinue: func(s string) bool { return s != "DELIVERED" },
		}),
	})
```

`NewWaitStrategy` returns the new named type `WaitStrategy[S]`, whose
underlying type is the `ConditionConfig.WaitStrategy` field's function
type. The field's type is unchanged, so function literals, variables, and
caller-defined function types assign to it as before, and a
`WaitStrategy[S]` assigns to it too. The default used when `WaitStrategy`
is nil is unchanged, 5 s initial delay, rate 1.5, 5 minute cap, full
jitter, 60 attempts, and is now the strategy `WaitConfig[S]{}` builds.
Zero-value fields select those defaults.

### Added: `Retry` retries a group of durable operations

`Retry` runs a function that may contain any durable operations, such as an
invoke followed by a callback, and re-runs the whole function when it fails,
suspending the execution between attempts. It takes the same `RetryStrategy`
that `Step` takes, so `ExponentialBackoff`, `NewRetryStrategy`,
`LinearBackoff`, and hand-written strategies all apply. The function
receives the durable context to use and the 1-based attempt number.

```go
label, err := durable.Retry(ctx, "ship", func(c durable.Context, attempt int) (string, error) {
	id, err := durable.Invoke[string](c, "print-label", labelFunction, order)
	if err != nil {
		return "", err
	}
	return durable.WaitForCallback[string](c, "pickup", func(sc durable.StepContext, callbackID string) error {
		return notifyCarrier(callbackID, id)
	})
}, durable.MustNewRetryStrategy(durable.RetryConfig{MaxAttempts: 3}))
```

Each attempt runs in its own child context named `<name>-attempt-<n>`, so
the operations of one attempt are recorded under that attempt and replay is
deterministic whatever an earlier attempt did before it failed. A failed
attempt reaches the strategy as a `*ChildContextError` whose `ErrorType`
names the error that escaped the function. The backoff between attempts is
a `Wait` named `<name>-backoff-<n>`; a zero delay waits `DefaultRetryDelay`.
`WithAttemptChildContext(false)` runs attempts directly in the caller's
context instead, and `WithAttemptChildOptions` forwards `ChildOption`
values, such as `WithChildSerdes`, to the per-attempt child context.

When the strategy stops retrying, `Retry` returns a `*RetryError` carrying
`Attempts`, the final attempt's error as `Err`, and the escaping error's
`ErrorType` and `Message`. It is recorded under the wire name `RetryError`
and is matchable as an `*OperationError`. A suspension from inside the
function, for example a wait or an unresolved callback, propagates
unchanged and never counts as a failed attempt.

### Added: `RetryableErrors` restricts retries to matching errors

`RetryConfig` and `LinearRetryConfig` gain a `RetryableErrors []ErrorMatcher`
field. When it is empty, every error is retryable, as before. When it is
set, a failed attempt is retried only if at least one matcher accepts its
error; otherwise the step fails at that attempt with the attempts made so
far. This matches the other SDKs' `retryableErrors` and
`retryableErrorTypes` options, folded into one list for Go.

An `ErrorMatcher` is a `func(error) bool`. Four constructors cover the
common cases: `ErrorIs(target)` uses `errors.Is`, `ErrorAs[T]()` uses
`errors.As`, and `ErrorContains(substr)` and `ErrorMatches(re)` test the
error's message. Wrapped errors match under `ErrorIs` and `ErrorAs`.

```go
durable.MustNewRetryStrategy(durable.RetryConfig{
	MaxAttempts: 5,
	RetryableErrors: []durable.ErrorMatcher{
		durable.ErrorAs[*TransientError](),
		durable.ErrorIs(io.ErrUnexpectedEOF),
		durable.ErrorContains("throttl"),
	},
})
```

A nil entry is invalid configuration: `NewRetryStrategy` and
`LinearBackoff` return an error and the `Must` variants panic. `ErrorIs(nil)`
and `ErrorMatches(nil)` return a nil matcher so that mistake is caught the
same way. `RetryableErrors` applies only to strategies built from a config;
a hand-written `RetryStrategy` sees every failed attempt.

### Decision: `Map` does not pass the source collection to `fn`

The other Durable Execution SDKs call the map function with a fourth
argument, the collection being mapped. The Go `Map` keeps its callback at
`func(ctx Context, item I, index int) (O, error)`. This is a deliberate
difference. In Go the callback is a function literal at the call site
with the slice already in scope, and the standard library's slice
functions (`sort.Slice`, `slices.IndexFunc`) follow the same convention,
so closing over the input is the idiom:

```go
result, err := durable.Map(ctx, "diffs", readings,
	func(c durable.Context, r Reading, i int) (float64, error) {
		if i == 0 {
			return 0, nil
		}
		return r.Value - readings[i-1].Value, nil
	})
```

`WithItemNamer` already used this idiom, since a `BatchOption` is not
generic over the item type and its namer receives only the index. The
`Map` and `WithItemNamer` documentation now both show it. No signature
changed.

### Breaking: `Map` and `Parallel` are fail-fast by default and return a `BatchError`

Two behavioural changes to batch completion.

**The default completion policy is fail-fast.** A `Map` or `Parallel`
without `WithCompletion`, or with a zero `CompletionConfig{}`, ran every
item and reported success however many failed. It now completes on the
first item failure with `CompletionFailureToleranceExceeded`, and the
items not yet started are omitted from the result. This matches the other
SDKs. To keep the former behaviour, set a tolerance explicitly, for
example `CompletionConfig{ToleratedFailureCount: aws.Int(len(items))}`.
A config that sets only `MinSuccessful` tolerates every failure, as
before.

**Failed items are returned as `err`.** `Map` and `Parallel` returned a
non-nil error only for SDK-level failures; the item failures lived in
`BatchResult.Err()`, which the caller had to check separately. When at
least one item failed, they now return a `*BatchError` as `err` and still
return the populated `BatchResult`, so partial results remain available
for compensation. `BatchError` has `Name`, `Reason` (the batch's
`CompletionReason`), and `Errors` (the per-item errors in input order);
it unwraps to the item errors for `errors.Is` and `errors.As`, and matches
`*OperationError`. `BatchResult.Err()` and `BatchCompletionError` are
removed. A `BatchError` is returned whenever an item failed, so a failure
within a configured tolerance also produces one; its `Reason` is then
`CompletionAllCompleted` or `CompletionMinSuccessfulReached` rather than
`CompletionFailureToleranceExceeded`. Replace

```go
result, err := durable.Map(ctx, "reserve", items, fn, opts...)
if err != nil {
	return err
}
if err := result.Err(); err != nil {
	return err
}
```

with `if err != nil { return err }` alone, or, to keep using the partial
result:

```go
result, err := durable.Map(ctx, "reserve", items, fn, opts...)
var berr *durable.BatchError
switch {
case err == nil:
	// use result
case errors.As(err, &berr):
	// items failed; result is populated for compensation
default:
	return err // suspension or SDK failure: propagate unchanged
}
```

The batch's checkpoint is unchanged: the batch operation is recorded as
SUCCEEDED whether or not items failed, as in the other SDKs. Only the Go
return value changed.

Two smaller changes to `CompletionConfig`. `ToleratedFailurePercentage`
is now `*int`, like `ToleratedFailureCount`, so an explicit `0` (fail on
the first failure) is distinguishable from unset; replace
`ToleratedFailurePercentage: 25` with `aws.Int(25)`. The percentage
comparison is exact instead of rounding down through integer division:
one failure in three (33.3%) now exceeds a threshold of 33.

### New: `BatchResult` lookup by name and started-item accessors

`BatchResult` gains `Result(name)`, which returns the successful result of
the item or branch with that name and `ok == false` when there is no such
item or it did not succeed, and `Item(name)`, which returns the item or
`nil`. With duplicate names both use the first match in input order.
`Started()` and `StartedCount()` return the items that were started and
then abandoned when the batch completed early; `TotalCount` already
includes them, so `TotalCount == SuccessCount + FailureCount +
StartedCount`. All four are derived from the exported `Items` field.

`CompletionReason` gains `CompletionCustomSucceeded` and
`CompletionCustomFailed`, whose `String()` values are
`CUSTOM_COMPLETION_SUCCEEDED` and `CUSTOM_COMPLETION_FAILED`, matching the
other SDKs. The three existing numeric values are unchanged. `String()` on
a zero or unrecognized reason returns `UNKNOWN`. A custom decision is
authoritative for `BatchResult.Status()`: `CompletionCustomFailed` makes the
batch failed (and `Map`/`Parallel` return a `BatchError`) even with no
failed item, and `CompletionCustomSucceeded` makes it succeeded even with
failed items. A `BatchError` rebuilt from a checkpoint record recovers
either reason.

### New: `WithChildSummary` and `WithBatchSummary`

A child-context or batch result over the 256 KiB checkpoint limit is not
stored; the checkpoint records that the operation's children are kept and
replay rebuilds the result from them. That checkpoint carried no
description of the result. Two options now supply one.

`durable.WithChildSummary(fn)` is a `ChildOption` for `RunInChildContext`,
`RunInChildContextAsync`, and `Go`. When the serialized result exceeds the
limit, the SDK calls `fn` with the result and stores the returned string
as the checkpoint payload. A summary over 256 KiB is truncated on a UTF-8
boundary; an empty summary leaves the payload absent.

`durable.WithBatchSummary(fn)` is a `BatchOption` for `Map` and
`Parallel`. When the serialized `BatchResult` exceeds the limit, the SDK
calls `fn` with the result and stores the returned string in the batch's
checkpoint record under the `summary` key, next to the completion reason
and item statuses replay already used. The summary is truncated on a UTF-8
boundary until the record fits the limit, and omitted when no prefix fits.

Both functions are typed on the operation's result type; a function of
another type is a configuration error the operation returns before it
claims an operation ID. Both run only when the result is oversized, and
only on the invocation that produced it. The summary is advisory: the SDK
never reads it back, and replay correctness never depends on it. The
function must be deterministic and free of side effects.

### New: custom batch completion with `CompletionConfig.ShouldComplete`

`CompletionConfig` gains `ShouldComplete func(BatchProgress)
CompletionDecision`. The batch calls it after each item reaches a terminal
state with a `BatchProgress` snapshot: `TotalCount`, `CompletedCount`,
`SuccessCount`, `FailureCount`, and `Items`, one `BatchItemProgress` per
input index with `Index`, `Name`, and `Status`. `Status` is
`BatchItemNotStarted` (the new zero value, `String()` `NOT_STARTED`),
`BatchItemStarted` for an item in flight, or `BatchItemSucceeded` or
`BatchItemFailed`. The callback returns `ContinueBatch()` or
`CompleteBatch(outcome)`, where `outcome` is `CompletionOutcomeSucceeded`
or `CompletionOutcomeFailed`; completing sets the batch's reason to
`CompletionCustomSucceeded` or `CompletionCustomFailed`, and that reason
alone decides `Status()` and whether a `BatchError` is returned. Items in
flight at completion are reported `BatchItemStarted`, as for a threshold.

`ShouldComplete` is mutually exclusive with `MinSuccessful`,
`ToleratedFailureCount`, and `ToleratedFailurePercentage`: a config that
sets both makes `Map` or `Parallel` return an error before any item runs.
With `ShouldComplete` set there is no fail-fast; only the callback completes
the batch early. The callback must be deterministic. Its decision is
recorded with the batch, so replay of a completed batch does not call it
again. This holds for both nesting modes, including a batch whose result is
too large for one checkpoint. The zero `CompletionDecision` is the value
`ContinueBatch()` returns. A callback that panics, or that calls
`CompleteBatch` with an outcome other than `CompletionOutcomeSucceeded` or
`CompletionOutcomeFailed`, fails the batch operation with an error that is
not a `BatchError`.

A branch that a concurrent batch abandons at early completion now writes no
terminal checkpoint for its child context, even when its body finishes
before the batch returns. The child context stays `STARTED` in the log,
matching the `BatchItemStarted` status the result reports.

New example: `examples/parallel-should-complete`.

### Fixed: a retry decision without a delay waits one second

A `RetryStrategy` that returned `RetryDecision{Retry: true}` without
setting `Delay` scheduled the next attempt with a delay of zero seconds,
whose scheduling is unspecified. A zero `Delay` now selects the new
`DefaultRetryDelay` constant (one second). Explicit delays are sent as
before: a positive fractional delay rounds up to the next whole second, a
whole-second delay is sent unchanged, and a negative delay fails the step.

### Breaking: `LinearBackoff` grows linearly and takes a `LinearRetryConfig`

`LinearBackoff` and `MustLinearBackoff` produced a fixed delay between
attempts, with the attempt count fixed at 6 and jitter disabled. They now
implement linear growth, matching the other SDKs: the delay before retry n
is `InitialDelay + Increment × (n-1)`, capped at `MaxDelay`, with the
configured jitter applied and rounded to whole seconds no less than one.

Both take a `LinearRetryConfig` instead of a `time.Duration`. Its fields
are `MaxAttempts` (default 6), `InitialDelay` (default 1 s), `Increment`
(default 1 s), `MaxDelay` (default 5 min), and `Jitter` (default
`JitterNone`). The zero value produces 6 total attempts with delays of
1 s, 2 s, 3 s, 4 s, and 5 s. Replace

```go
durable.MustLinearBackoff(5 * time.Second)
```

with `durable.MustLinearBackoff(durable.LinearRetryConfig{InitialDelay: 5 *
time.Second})` for a sequence that starts at 5 s and grows by 1 s, or with

```go
durable.MustNewRetryStrategy(durable.RetryConfig{
	MaxAttempts:  6,
	InitialDelay: 5 * time.Second,
	BackoffRate:  1,
	Jitter:       durable.JitterNone,
})
```

to keep the former fixed 5 s interval.

`RetryConfig` now documents that its zero value is not
`ExponentialBackoff()`: `RetryConfig{}` is 3 attempts capped at 5 minutes,
`ExponentialBackoff()` is 6 attempts capped at 60 seconds. Neither changed.

### Fixed: suspension waits for a running asynchronous step

When the handler blocked on a pending operation (a `Wait`, a callback, an
invoke) while a step started by `StepAsync` or inside a `durable.Go`
branch was still running, the invocation responded PENDING at once. The
step's result was then refused by the terminated checkpointer, and the
next invocation ran the step again.

The invocation now waits until every running step attempt and
`WaitForCondition` check has recorded its outcome, and every child context
whose body has returned has recorded its completion, before it responds
PENDING. Child contexts include `Go` and `RunInChildContext` bodies,
`Map` and `Parallel` items and the batch itself, and `WaitForCallback`.
Only that work is waited for: a branch that is itself blocked on
a pending operation, or that is running code between durable operations,
delays the response by at most a short settle period (20 ms), during which
a step it starts is still waited for. The wait ends early when the
invocation's context ends.

### Fixed: a stale checkpoint token no longer fails the execution

The service rejects a checkpoint whose token a newer invocation has
superseded (`InvalidParameterValueException` with a message starting
`Invalid checkpoint token`). The SDK previously classified this as an
execution-scoped failure and responded FAILED. It now classifies it as
invocation-scoped, stops checkpointing at once, and ends the invocation
with an error. The execution continues in the invocation that holds the
fresh token.

`CheckpointError.Retryable()` is false for this rejection even though
`Scope()` is `ErrorScopeInvocation`: the token never becomes valid again,
so the SDK does not retry the call. For every other invocation-scoped
failure `Retryable()` is still true.

The invocation ends with the error even when handler code ignores the
step's error and returns a value.

### New: a checkpoint response without a token ends the invocation PENDING

A checkpoint response that carries no `CheckpointToken` means the service
will accept no further checkpoints from the current invocation. The SDK
previously treated it as a plain error, which failed the execution. It now
stops checkpointing and ends the invocation with `Status: PENDING`, as for
any other suspension; operations that had not checkpointed replay on the
next invocation. The invocation responds PENDING even when handler code
ignores the step's error and returns a value.

`durabletest.LocalRunner.OmitTokenOnCheckpoint(n)` makes the n-th
checkpoint call return no token, so a handler's behavior on this path can
be tested locally.

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
text as its `Error()`, and a rebuilt `BatchError` recovers its
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

### New: `ErrorScope` and `ClientError`; `CheckpointError.Scope`

A failed `ExecutionClient` call now has a scope. `ErrorScopeInvocation`
means the current invocation cannot continue but the execution can resume
in a later one; `ErrorScopeExecution` means the execution must fail.
`CheckpointError.Scope()` exposes it. `Retryable()` and
`IsCheckpointRetryable` are unchanged: `Retryable()` is true when the scope
is `ErrorScopeInvocation`, except for a stale checkpoint token (see above).

A custom `ExecutionClient` states the scope by returning a
`*durable.ClientError` (from `Checkpoint` or `GetExecutionState`). The
SDK honors it wherever the error surfaces: an execution-scoped failure is
not retried and fails the execution with a FAILED response; an
invocation-scoped failure is retried and, if it escapes the handler, ends
the invocation with an error so the execution resumes later. The same
rule applies to a `ClientError` returned by handler or plugin code and to
the checkpoint the SDK makes for a result too large to return inline. A
`ClientError` with the zero or an unknown `Scope` is read as
`ErrorScopeInvocation`, the safer default. Errors without a `ClientError`
in their chain are classified from their AWS SDK shape as before.

Behavior change: an invocation-scoped `CheckpointError` or `ClientError`
that the handler passes through now ends the invocation with an error
instead of a FAILED response, and an execution-scoped failure of the
oversized-result checkpoint now responds FAILED instead of ending the
invocation with an error. Handler code that wants the execution to fail
returns its own error.

### New: `WithChildErrorMapper`

`durable.WithChildErrorMapper(mapper)` is a `ChildOption` for
`RunInChildContext`, `RunInChildContextAsync`, and `Go`. When the child
body fails, the `*ChildContextError` the SDK would otherwise return is
passed to `mapper`, and `mapper`'s result is returned instead. A nil
result is ignored and the `*ChildContextError` is returned unchanged.

The mapper runs on the first invocation and again on every replay, with
the same input each time: a `*ChildContextError` built from the recorded
failure. The checkpoint records that failure, the mapper's input, not the
mapper's result; a result of the handler's own type cannot be rebuilt from
a record, so re-mapping the recorded input is what reproduces the mapped
error on replay. The mapper must be deterministic. It needs no knowledge
of its own output type.

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
