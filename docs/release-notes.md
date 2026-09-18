# Release Notes

## Unreleased

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
