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
   fetched until the full operation log is loaded.

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

The `suspendSignal` coordinates suspension across the invocation. It has
three mechanisms: a pending commitment, which decides the invocation
result; active-branch accounting, which decides when in-flight futures
are settled; and executing-span accounting, which decides when a blocked
handler may respond. A further piece, checkpointer termination, takes
over when the handler's outcome is decided: it bounds what an orphaned
branch can record once the result is decided.

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
draining its item workers, so work the batch explicitly abandoned does not
keep the invocation `PENDING`. Every other commitment is unconditional and
is never retired.

Each handle carries a pointer to the handle of the enclosing subtree, fixed
when the batch mints it. A subtree counts as abandoned when its own handle
or any handle on that chain is set. The chain is reachable from every
context that holds the handle, so a handle's lineage lasts exactly as long
as some branch under it can still act; no registry has to be kept current
as batches start and finish. Retirement sets the retiring handle and
removes every commitment whose handle reaches it through the chain, so it
covers commitments made inside nested batches at any depth.

Draining the item workers does not drain every goroutine under the subtree.
A nested `Map` or `Parallel` runs inside an item worker, so its own workers
finish before that item worker returns. A branch launched by `durable.Go`
inside an item is not joined by the batch: it holds its own branch token and
settles its own future when its operation returns, so it can reach a
blocking operation after the batch has retired the handle. Such a branch
may also outlive a nested batch that has already returned, or start a
nested batch of its own after the retirement. In every case the handle it
carries reaches the retired handle through the chain, and `commitPending`
against an abandoned handle is a no-op: nothing is recorded, the signal is
not fired, and the operation unwinds with `errSuspendExecution` as it would
on an abandoned branch. A commitment made before retirement is removed by
it; one made after is dropped. Either way no commitment stands, so the
invocation outcome does not depend on which side of the retirement the
branch's operation lands on.

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

The invocation responds with `PENDING`. The function is re-invoked when
the blocking condition resolves.

### Executing-span accounting

Active-branch accounting decides when futures are settled; it does not
decide when the invocation may respond. A branch that is running a step
body when the handler blocks holds its token, but a branch that is blocked
on a plain channel between operations holds its token too. Waiting for
every branch would let the second kind stall the response indefinitely,
so the response condition uses a separate count.

The signal counts executing spans. A step attempt is a span from its
`START` checkpoint through the checkpoint of its outcome, and a
`WaitForCondition` cycle over the same range. A child context is a span
from the return of its body through the checkpoint of its completion.
This covers `RunInChildContext`, `RunInChildContextAsync`, and `Go`; a
`Map` or `Parallel` item, whether it runs on its own worker goroutine or
on the batch's goroutine; the batch parent itself, from the last item's
report through the checkpoint of the parent's completion; and a
`WaitForCallback` context. Each site increments the count when the span
begins and decrements it when the outcome has been recorded. A child
context's body between operations is not counted. Every increment also
advances a generation stamp.

When the handler goroutine unwinds with `errSuspendExecution`, the handler
calls `awaitDrain` before it returns `PENDING`. `awaitDrain` returns when
one of three conditions holds:

1. No branch other than the handler goroutine is registered. Every span
   runs on a registered branch, so none can begin.
2. No span is executing, and none began during a settle period (20 ms by
   default). One observation of a zero count is not stable: a step that
   has just recorded its outcome returns into its child context, which
   begins its own span at once. So the count and the generation stamp are
   read again after the settle period, and the wait starts over if either
   changed. The last branch deregistering during the period ends it early.
3. The invocation's context ends. A long span cannot hold the response
   past the Lambda deadline; its later checkpoint is then refused by the
   terminated checkpointer, as for any orphaned branch.

Spans that are executing finish and record their outcomes in this
invocation; the next invocation replays them instead of running them
again. A span that begins after `awaitDrain` returns is refused at its
next checkpoint.

The wait applies only to that exit. A handler that returns a result or an
error while a commitment stands responds at once and never joins
outstanding branches (see below).

### Unfinished operations inside a recorded context

A child context whose result is already recorded can be re-executed on
replay when that result was too large to store (replay-children mode). Its
operations replay from their checkpoints. An operation that had not
completed when the context's result was recorded has no terminal
checkpoint. It must not run again: re-executing it would repeat its side
effects, and it cannot produce a result in this invocation. It also makes
no pending commitment: the context's result is already decided, so an
unfinished operation inside it must not force the invocation to `PENDING`.

An asynchronous form of such an operation returns a future that settles
only if the invocation suspends. A synchronous await parks the calling
goroutine (`parkUnfinishedReplay`). The park ends on the first of three
events:

1. The suspend signal fires because other branches committed to `PENDING`
   and deregistered. The caller unwinds with `errSuspendExecution`.
2. A fixed deadline (`unfinishedReplayParkTimeout`, one second) elapses.
3. The Lambda context ends.

Events 2 and 3 share one outcome, decided by whether a pending commitment
exists at that moment. If one does, the invocation responds `PENDING`
whatever the handler returns, so the caller unwinds with
`errSuspendExecution`. If none does, the caller unwinds with a
`*NonDeterministicReplayError` that names the operation and its checkpoint
status.

Event 2 is the termination guarantee. When the parking goroutine is the
last active branch, no commitment exists, so nothing can fire the signal;
without the deadline the park would last until the Lambda deadline, the
invocation would end `PENDING`, and the next invocation would repeat the
same wait. A deterministic handler cannot reach this state: the live run
returned from the context without awaiting the operation, so a replay that
awaits it has diverged from the recorded execution. The deadline turns that
divergence into a diagnosable failure of the execution, delivered within
one second of reaching the park.

Event 3 applies the same commitment check so a Lambda context with less
than one second remaining cannot defeat the bound. If the context's end
unwound the park as a suspension regardless, the invocation would respond
`PENDING` with no diagnostic and the next invocation would park again.

The park releases the goroutine's branch token only when the parking
context registered that token itself. A child context created on its
parent's goroutine inherits the parent's token without owning it, so a park
inside such a child leaves the parent, including the root handler, counted
as active. Releasing an inherited token would deregister a goroutine that
is still running; for the root handler it would also let a commitment made
after the handler returned change the invocation's outcome, which the
deferred release in `Invoke` exists to prevent. An asynchronous operation's
branch owns its token and releases it before parking, so a sibling's
commitment can still fire the signal while the branch is parked.

### Checkpointer termination

The two mechanisms above decide the result and settle futures, but neither
constrains a branch that is still running user code when the invocation
responds. The handler never joins outstanding branches. When the handler
goroutine returns a result or an error while a commitment stands, or the
suspend signal fires, it responds with `PENDING` immediately. When the
handler goroutine unwinds with `errSuspendExecution`, it first waits for
executing spans to record their outcomes (executing-span accounting,
above), then responds; branches running code outside a span are still not
joined beyond the settle period, so that response cannot stall behind a
branch blocked between operations.

The checkpointer is terminated on every exit from the handler, not only
on suspension. A single `defer` in `runHandler` performs the termination,
so a handler that returns a result, returns an error, or panics terminates
the checkpointer exactly as a suspension does, and no exit path added later
can miss it. Termination happens the moment the handler's outcome is
decided. Result serialization, `WrapInvocation` post-processing, and the
`OnInvocationEnd` hooks all run after it, so no branch can record state for
this invocation once the handler has finished, whatever runs between that
point and the response.

One write follows termination: the invocation's own record of an oversized
result. A result too large for the response envelope is persisted through
a checkpoint on the root execution operation after the handler returns.
That write goes through `checkpointFinal`, a path reserved for the
invocation goroutine. Termination refuses branch checkpoints; it does not
refuse the final write. The final write travels through the same flusher
as every other request, so it is sent after any branch call already in
flight and with the token that call rotated to. It is refused only when
the service has stopped accepting this invocation's checkpoints (a response
without a token, or a stale-token rejection), and then with the same error
a branch checkpoint would receive.

A deferred cleanup in `Invoke` releases the root handler's branch token
once the response is decided. The handler goroutine releases that token
itself only when it unwinds with `errSuspendExecution`, so a handler
blocked on a pending operation counts as blocked while the outcome is
undecided. On a successful, failed, or panicking return the token is held
until the response is decided. A branch that commits to `PENDING` after the
handler has returned therefore cannot fire the signal and change the
outcome. Releasing the token afterwards lets the signal fire once the last
orphaned branch deregisters, which settles any future a goroutine is still
blocked on.

Termination is a single atomic flag store on the checkpointer, so it never
blocks behind an in-flight checkpoint API call holding the checkpointer's
mutex. The checkpoint method consults the flag at three points:

1. Before acquiring the mutex, as a lock-free fast-path refusal.
2. After acquiring the mutex and again between retry attempts, in case
   termination arrived while the caller was waiting or backing off.
3. After a successful API call returns, so a checkpoint that was in flight
   when termination was signaled is refused to its callers.

Point 3 means a checkpoint call that was already in flight at the moment of
termination may still be recorded durably, even though its callers are
refused. From the branch's perspective the checkpoint was refused; the
recorded execution state may nonetheless include the update. This is safe
because the next invocation replays from the full recorded state, so an
update recorded during termination is picked up on resume rather than lost.
The token the call rotated to is kept locally, because the service holds
it: the invocation's final write, if any, must carry that token. The
guarantee is therefore that no branch checkpoint attempt succeeds after
termination, not that an in-flight call cannot be recorded.

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

### Pending callbacks cannot win

A pending callback future settles only when the invocation suspends, and
its first `Result` call is what records the pending commitment (deferred
suspension, below). `Any` and `Race` therefore set such futures aside and
join only the futures that can settle terminally in the current
invocation. If a terminal winner emerges, the pending callbacks are never
awaited, so a losing callback cannot force the invocation to `PENDING`
while the winner is returned. Only when suspension is the decided outcome
— a joined branch suspended, or every remaining future is a pending
callback — are the deferred futures awaited, recording the commitment.
`All` and `AllSettled` await every future by definition, so a pending
callback in their input always suspends the aggregate.

### Edge cases

Edge cases mirror the JavaScript promise combinators:

- `All` and `AllSettled` on empty input return an empty slice immediately.
- `Any` on empty input fails immediately with a `*CombinatorError`.
- `Race` on empty input suspends, since no future will ever settle.

### Select

`Select` is `Race` over named branches. It runs each `Branch` in its own
child context and applies the same first-terminal-wins and drain rules to
the resulting futures. Its checkpointed aggregate holds the winning
branch's name together with the settled outcome (value or error), so replay
returns the same winner and the same error even when another branch would
finish first if the branches ran again. A failed winner does not fail the
`Select` operation's own record: the operation is recorded as `SUCCEEDED`
with the rejected outcome inside its result, and the error returned to the
caller is the branch's `*ChildContextError`, rebuilt from that record on
replay. Empty input and duplicate branch names are rejected before any
operation is claimed.

## Goroutine Ownership

Each `Context` has an owning goroutine. Every durable operation validates
that the calling goroutine matches the owner before it claims an operation
ID. A mismatch returns `ErrWrongGoroutine` immediately, and the rejected
call claims no ID and records no checkpoint. The check runs in every
default build. The `durablenocheck` build tag compiles it out; that build
does not detect foreign-goroutine calls.

This rule exists to enforce deterministic ID minting. If two goroutines
raced to call operations on the same context, the minting order would be
non-deterministic and replay would break. The ownership check turns this
class of bug into a fast, clear failure rather than a subtle replay
divergence.

Goroutine identity is parsed from the `runtime.Stack` header (`goroutine N
[running]:`). If the ID cannot be determined (future Go runtime changes),
the check is disabled rather than rejecting correct programs. The check
costs a few microseconds per operation, growing with stack depth, because
`runtime.Stack` walks the whole calling stack; `BenchmarkClaimOperation`
in the `durable` package records the numbers. A checkpoint request follows
every claim, so the check is a small fraction of an operation's cost.

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

When the invocation ends and the checkpointer is terminated, a
`durable.Go` child context that is still mid-flight becomes an orphaned
branch. This holds for every way the invocation can end: the handler
suspended, returned a result, returned an error, or panicked. At its next
checkpoint attempt the checkpointer refuses with
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

After a `PENDING` response, the orphan re-executes from its last committed
checkpoint on the next invocation, so its progress is deferred, not lost.
After a `SUCCEEDED` or `FAILED` response the execution is finished and the
orphan is never replayed. Its recorded progress stays in the execution
state; its unrecorded progress is never recorded.

Termination stops the orphan at its next durable boundary, not inside user
code. Termination refuses checkpoints; it does not preempt a goroutine. A
`Step` whose `START` checkpoint completed before termination has already
entered its user body. That body runs to completion. The step's next
checkpoint, the `SUCCEED` or `FAIL` that would record the body's result, is
the one refused. So an orphan's already-started user code may continue
after the invocation has responded, until it reaches its next checkpoint.
Any side effect that user code performs happens even though the result is
never recorded. A handler that needs a branch's work to complete, or needs
its side effects confined to the invocation, must await the branch's future
before returning.

Termination matters most in a reused execution environment. The
environment is frozen when the invocation responds and thawed for a later
invocation, so an orphan that was blocked when the handler returned resumes
during that unrelated invocation. If it was blocked before a checkpoint,
that checkpoint is refused locally and the goroutine exits without a
network call. If it was blocked inside a step body, the body continues and
the checkpoint after it is refused. In neither case does the orphan write
to the execution with a token the service no longer accepts.

Progress the orphan recorded before termination (checkpoints whose API
calls completed before the flag was set) is preserved in the recorded
execution state. Progress it would have recorded after termination is not
recorded in this invocation.

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
