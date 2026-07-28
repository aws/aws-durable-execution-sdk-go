# Checkpoint-Replay Architecture

This document describes how the AWS Durable Execution SDK for Go implements
checkpoint-replay to give Lambda functions durable, resumable execution
semantics. It is intended for contributors working on the SDK internals.

## Overview

Checkpoint-replay lets a Lambda function survive invocation boundaries. The
function executes forward, checkpointing operation results as it goes. When
the function is re-invoked (after a wait, a callback, or a timeout), the
runtime replays already-checkpointed operations by returning their stored
results instantly, then resumes live execution from the first operation that
has no checkpoint. From the user handler's perspective, the function appears
to run continuously from start to finish.

The contract is simple: between checkpoints, handler code must be a pure
function of its input and the checkpointed results it has consumed. Any
side effect that should not repeat on replay belongs inside a `Step`.

## Invocation Lifecycle

Each Lambda invocation follows this sequence:

1. **Parse payload.** The Lambda runtime invokes the durable handler with a
   JSON payload containing the execution ARN, a checkpoint token, and an
   embedded page of previously checkpointed operations.

2. **Assemble state.** The embedded page is parsed into an in-memory
   operation map. If a pagination marker is present, additional pages are
   fetched from the backend until the full operation log is loaded.

3. **Determine mode.** If more than one operation exists in the state (the
   root EXECUTION operation plus at least one user operation), the context
   starts in **replay mode**. Otherwise it starts in **live execution mode**.

4. **Run the handler.** The user handler runs on its own goroutine. Each
   durable operation it calls either returns a replayed result (replay mode)
   or executes live and checkpoints its outcome (execution mode).

5. **Settle.** The invocation ends in one of three ways:
   - **SUCCEEDED** -- the handler returns a value.
   - **FAILED** -- the handler returns an error.
   - **PENDING** -- the suspend signal fires because an operation requires
     an external event (wait, callback, invoke).

## Operation ID Minting

Every durable operation needs a stable identifier that is identical across
invocations. IDs are **positional**: a counter scoped to the context that
owns the operation.

- The root context mints `"1"`, `"2"`, `"3"`, ...
- A child context whose own ID is `"2"` mints `"2-1"`, `"2-2"`, ...
- Nesting composes: a grandchild under `"2-1"` mints `"2-1-1"`, `"2-1-2"`, ...

Because IDs derive from program order alone, they are deterministic across
re-invocations as long as the handler code is unchanged. The `opIDs` struct
is confined to a single goroutine and is not safe for concurrent use, which
enforces deterministic ordering at the type level.

## ID Hashing

Positional IDs are human-readable but can grow long in deeply nested
executions. The wire protocol uses a compact form: the first 16 hex
characters of the MD5 digest of the positional ID.

```go
sum := md5.Sum([]byte(positionalID))
wireID := hex.EncodeToString(sum[:])[:16]
```

MD5 is used as an identifier encoding, not for cryptographic security. The
16 hex characters carry 64 bits of entropy, making collisions negligible at
realistic operation counts (thousands per execution).

All state lookups accept a positional ID and hash it internally, so callers
never deal with wire IDs directly.

## Replay Detection

The execution context decides its initial mode based on the assembled state:

```go
mode := modeExecution
if len(state.operations) > 1 {
    mode = modeReplay
}
```

The state always contains at least one operation (the root `EXECUTION`
record). If additional operations exist, prior work has been checkpointed
and the context enters replay.

Before each operation, `refreshReplayMode` checks whether the next
positional ID has a checkpoint entry. If it does, the operation returns the
stored result. If not, the context transitions to live execution mode for
that operation and all subsequent ones.

## Suspension

The `suspendSignal` coordinates suspension across the invocation. It has two
independent mechanisms: a pending commitment, which decides the invocation
result, and active-branch accounting, which decides when in-flight futures
are settled. A third piece, checkpointer termination, takes over when the
invocation responds: it bounds what an orphaned branch can record once the
result is decided.

### Pending commitment

When a durable operation determines that it cannot proceed (a timer has not
elapsed, a callback has not arrived, an invoked function has not returned),
it records a pending commitment and returns `errSuspendExecution`. Once a
commitment stands, the invocation responds with `PENDING` regardless of what
the handler returns, so user code cannot swallow the error and report a
bogus success. The handler checks `committed()` for this.

A commitment made by an operation running under a batch branch that its
parent `Map` or `Parallel` may abandon is recorded against that subtree's
abandon handle rather than unconditionally. When the parent completes early
and abandons its outstanding branches, it retires those commitments after
draining every worker, so work the batch explicitly abandoned does not keep
the invocation `PENDING`. Retirement cascades to handles minted by nested
batches. Every other commitment is unconditional and is never retired.

### Active-branch accounting

Concurrent work registers as an active branch before its goroutine starts
and deregisters when the goroutine finishes. A branch that can still make
progress keeps running and checkpointing even while a sibling is blocked.
The signal fires only when the last active branch deregisters while a
commitment stands. Firing does two things:

1. Closes an internal channel so the handler's select notices immediately.
2. Settles all registered in-flight futures with `errSuspendExecution` so
   goroutines blocked on `Future.Result()` unwind without hanging.

So the commitment and the firing are separate events, and `committed()` can
be true well before `fired()` is. Code that needs to know the invocation
result is already decided uses `committed()`; code that needs to know
futures have been settled uses `fired()`. A context also carries its own
`blocked` flag, set when an operation on that context commits, which stops
further claims on that context without affecting its siblings.

The invocation responds with `PENDING`. The backend will re-invoke the
function when the blocking condition resolves.

### Checkpointer termination

The two mechanisms above decide the result and settle futures, but neither
constrains a branch that is still running user code when the invocation
responds. The handler never joins outstanding branches: when it observes a
standing commitment (either the handler goroutine returned while
`committed()` is true, or the suspend signal fired), it terminates the
checkpointer and responds with `PENDING` immediately. Because no goroutine
is joined, the `PENDING` response cannot stall behind a slow or blocked
branch.

Termination is a single atomic flag store on the checkpointer, so it never
blocks behind an in-flight checkpoint API call holding the checkpointer's
mutex. The checkpoint method consults the flag at three points:

1. Before acquiring the mutex, as a lock-free fast-path refusal.
2. After acquiring the mutex and again between retry attempts, in case
   termination arrived while the caller was waiting or backing off.
3. After a successful API call returns, so a checkpoint that was in flight
   when termination was signaled is refused before rotating the token.

Point 3 means a checkpoint whose API call was already in flight at the
moment of termination can succeed at the backend (the bytes land remotely)
but the checkpointer refuses to commit the result locally. From the
handler's perspective the branch was refused; from the backend's perspective
the state was written. This is safe because the next invocation replays from
the full backend state, so remotely written data is picked up on resume
rather than lost. The guarantee is therefore that no subsequent checkpoint
attempt succeeds locally after termination, not that no in-flight bytes can
reach the backend.

#### Translation of the terminated error

Operations translate the internal `errCheckpointTerminated` into
`errSuspendExecution` at the call site where a checkpoint failure is
observed. `Step`, `Wait`, `Invoke`, callbacks, and `RunInChildContext` all
perform this translation explicitly: when the checkpointer refuses, the
operation returns `errSuspendExecution`, settling the branch's future and
releasing its branch token without recording further state.

`Map`, `Parallel`, and `WaitForCondition` have a different structure: their
internal checkpoint calls return errors up through helper functions that do
not translate `errCheckpointTerminated`. A checkpoint failure in a batch
item or condition attempt propagates as a non-suspension error through the
batch coordinator or condition loop. In the concurrent batch path, such an
error becomes a fatal error that stops the batch after its in-flight workers
drain. A `durable.Go` child context that hosts one of these operations can
therefore observe `errCheckpointTerminated` as its function's return error
rather than `errSuspendExecution`. This does not escape to user code as a
failure: when the child function returns, the child context wrapper checks
for both `errSuspendExecution` and `errCheckpointTerminated` before any
error wrapping and settles the child's future with `errSuspendExecution`,
so the parent awaiting the future observes a suspension, never a
`ChildContextError`. `ChildContextError` wraps only errors that are neither
suspension nor termination. However, inside the child function itself, a
caller that inspects the raw error from `Map` or `WaitForCondition` sees
the internal error rather than `errSuspendExecution`.

In all cases the terminated branch cannot record further state. On the next
invocation the branch re-executes from its last committed checkpoint. The
behavioral effect is identical to a suspension: progress is deferred, not
lost.

## Combinator Suspension-Drain Semantics

`All`, `AllSettled`, `Any`, and `Race` join a slice of futures into one
checkpointed aggregate result. Each runs inside a child context (via
`RunInChildContext`), so on replay the stored aggregate is returned without
re-awaiting the futures.

Within one invocation a future can settle three ways: success, a terminal
error, or suspension (`errSuspendExecution`, meaning that branch is blocked
until a later invocation). A suspension is not an outcome; it means the
branch has no outcome yet. The combinators therefore treat it differently
from terminal settlements, in two phases.

### Before any suspension is observed

Each combinator keeps its usual first-settlement semantics:

- **All** fails fast on the first terminal error without awaiting the
  remaining futures, matching `Promise.all` fail-fast rejection.
- **Any** returns the first success without awaiting the remaining futures,
  matching `Promise.any`.
- **Race** returns the first terminal outcome (success or error) without
  awaiting the remaining futures, matching `Promise.race`.
- **AllSettled** has no early exit; it awaits every future by definition.

### Once a suspension is observed

Every combinator switches to draining: it awaits all remaining futures so
each sibling branch reaches its own blocking point and checkpoints its
progress, then propagates the suspension. Suspension takes precedence over
terminal outcomes observed after it, because the suspended branch completes
only on a later invocation and the aggregate cannot be final until then.
Terminal outcomes seen during the drain are not lost: on the resume
invocation the futures replay and those outcomes are returned then.

Without the drain, a combinator returning at the first suspension would cut
off sibling branches before they reached their blocking points, so their
progress would never be checkpointed and would be re-executed from scratch
on the next invocation.

### Edge cases

Edge cases mirror the JavaScript promise combinators:

- `All` and `AllSettled` on empty input return an empty slice immediately.
- `Any` on empty input fails immediately with a `*CombinatorError`.
- `Race` on empty input suspends, since no future will ever settle.

## Goroutine Ownership

Each `Context` has an owning goroutine. When built with the `durablecheck`
build tag, durable operations validate that the calling goroutine matches
the owner before proceeding. A mismatch returns `ErrWrongGoroutine`
immediately. Without the tag, the check is a no-op for production use.

This rule exists to enforce deterministic ID minting. If two goroutines
raced to call operations on the same context, the minting order would be
non-deterministic and replay would break. The ownership check turns this
class of bug into a fast, clear failure rather than a subtle replay
divergence.

Goroutine identity is parsed from the `runtime.Stack` header (`goroutine N
[running]:`). If the ID cannot be determined (future Go runtime changes),
the check is disabled rather than rejecting correct programs.

## Futures

`Future[O]` is the result handle for asynchronous durable operations. It
has three key properties:

1. **Settle-once.** `sync.Once` guarantees that only the first call to
   `settle` takes effect. Racing between normal completion and suspension
   settlement is safe by construction.

2. **Re-readable.** The done channel is closed on settlement. Any number of
   goroutines can call `Result()` after settlement and receive the same
   value without blocking.

3. **Deferred suspension.** Callback futures attach a `preResult` hook. The
   suspend signal fires only when `Result()` is actually called, allowing
   intervening operations (like a submitter step) to execute in the same
   invocation before the function suspends.

## Cross-Goroutine Coordination

The `durable.Go` function (an alias for `RunInChildContextAsync`) runs a
subflow in its own child context on a new goroutine. It is the replay-safe
substitute for the language-level `go` statement inside durable functions:

```go
fut := durable.Go(ctx, "work", func(child durable.Context) (T, error) {
    // This runs on a new goroutine with its own Context.
    return doWork(child)
})
result, err := fut.Result()
```

The sequence is:

1. The operation ID is claimed synchronously on the calling goroutine,
   preserving deterministic minting order.
2. A future is created and registered with the suspend signal before the
   goroutine launches.
3. A new goroutine starts. It captures its own `goroutineOwner` and
   receives a child context scoped to that goroutine.
4. The child context has its own ID minter (prefixed with the parent
   operation ID), so operations inside the child are isolated from the
   parent's ID sequence.

This design means multiple `durable.Go` calls from one goroutine produce
deterministic IDs regardless of goroutine scheduling order.

### Orphaned child contexts after termination

When the handler unwinds and the checkpointer is terminated, a
`durable.Go` child context that is still mid-flight becomes an orphaned
branch. At its next checkpoint attempt the checkpointer refuses with
`errCheckpointTerminated`. Operations that translate the error (`Step`,
`Wait`, `Invoke`, callbacks, `RunInChildContext`) convert it to
`errSuspendExecution`, which settles the child's future and releases its
branch token. Operations that do not translate (`Map`, `Parallel`,
`WaitForCondition`) propagate the internal error up through the child
function, but the child context wrapper that hosts the branch recognizes
`errCheckpointTerminated` alongside `errSuspendExecution` and settles the
future with `errSuspendExecution` rather than wrapping it in a
`ChildContextError`. The net effect is the same on either path: the
orphaned branch stops, its future settles as a suspension, and its branch
token is released.

Progress the orphan recorded before termination (checkpoints whose API
calls completed and whose tokens rotated before the flag was set) is
preserved in the backend state. Progress it would have recorded after
termination is deferred: the branch re-executes from its last committed
checkpoint on the next invocation.

## Determinism Contract

The replay guarantee rests on a single invariant:

> Between checkpoints, the handler must be a **pure function** of its input
> and the checkpointed results it has consumed.

In practice this means:

- **No side effects between operations.** Reading the clock, calling an
  external API, or generating random values between durable operations will
  produce different results on replay. Use `ExecutionStartTime` when a
  stable timestamp is enough. Put non-deterministic work inside a `Step` so
  it executes once and its result is reused on replay.

- **No conditional branching on mutable external state.** If an `if`
  statement between operations reads from a database or environment
  variable, the branch taken on replay may differ from the original
  execution, causing operation IDs to diverge.

- **Deterministic iteration order.** Iterating over a Go map between
  operations may yield different ordering on replay. Use slices or sorted
  keys when the iteration drives durable operation calls.

Violations of the determinism contract do not crash the program at the
point of divergence. Instead, they manifest as a replay consistency error
when the runtime detects that the expected operation type or name at a given
ID does not match the checkpoint.
