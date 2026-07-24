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

The `suspendSignal` coordinates suspension across the invocation. When a
durable operation determines that it cannot proceed (a timer has not
elapsed, a callback has not arrived, an invoked function has not returned),
it fires the suspend signal.

Firing the signal does three things:

1. Closes an internal channel so the handler's select notices immediately.
2. Settles all registered in-flight futures with `errSuspendExecution` so
   goroutines blocked on `Future.Result()` unwind without hanging.
3. Records the outcome as final. The handler goroutine's later return value
   is ignored once suspension is decided.

The invocation responds with `PENDING`. The backend will re-invoke the
function when the blocking condition resolves.

## Goroutine Ownership

Each `Context` has an owning goroutine. Durable operations validate that
the calling goroutine matches the owner before proceeding. A mismatch
returns `ErrWrongGoroutine` immediately.

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

The `Go` function (aliased from `RunInChildContextAsync`) is the
replay-safe substitute for Go's `go` statement:

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

This design means multiple `Go` calls from one goroutine produce
deterministic IDs regardless of goroutine scheduling order.

## Determinism Contract

The replay guarantee rests on a single invariant:

> Between checkpoints, the handler must be a **pure function** of its input
> and the checkpointed results it has consumed.

In practice this means:

- **No side effects between operations.** Reading the clock, calling an
  external API, or generating random values between durable operations will
  produce different results on replay. Use `CurrentTime` for replay-safe
  timestamps. Put non-deterministic work inside a `Step` so it executes
  once and its result is reused on replay.

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
