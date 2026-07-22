# Checkpoint/Replay State Machine — Cross-SDK Findings & Go Design

This supersedes the "next steps" section of the original scaffold doc with concrete findings from reading the actual Java and TypeScript SDK source (not just their public docs), and translates the mechanism into a Go-native design.

## Cross-SDK findings

### 1. Step ID generation (all three SDKs agree)

Simple incrementing counter per context, not a complex hash:
```
createStepId(): counter++; return prefix ? `${prefix}-${counter}` : `${counter}`
```
A child context is given `stepId` as its own prefix, so its children become `"1-1"`, `"1-2"`, etc. IDs are then opaquely hashed (`hashId()`) before being sent to the backend, but the *logical* ID scheme visible to SDK code is the plain dash-joined counter path. **Go port: identical scheme**, `fmt.Sprintf("%s-%d", prefix, counter)` with an atomic or mutex-guarded counter per context (contexts can have concurrent children, see §4).

### 2. Operation log structure (all three SDKs agree — this is the fixed wire contract)

Flat map: `stepId (hashed) -> Operation`. Each `Operation` has `Id`, `Type` (STEP/WAIT/CALLBACK/INVOKE/EXECUTION/...), `Status` (STARTED/SUCCEEDED/FAILED/PENDING/CANCELLED/STOPPED/TIMED_OUT), and type-specific detail payloads (e.g. `StepDetails.Result`, `StepDetails.Error`, `StepDetails.NextAttemptTimestamp`, `StepDetails.Attempt`). This matches what's already in `pkg/durable/types/wire.go` on this branch; only refinement needed is adding `PENDING`/`CANCELLED`/`STOPPED`/`TIMED_OUT` to `OperationStatus` and per-type detail structs.

### 3. Replay-skip logic (all three SDKs agree)

Before running an operation, look up its (not-yet-claimed) step ID in the operation log:
- Found + `SUCCEEDED` → deserialize `Result`, return without running user code.
- Found + `FAILED` → reconstruct and return/throw the stored error without running user code.
- Found + `PENDING`/`STARTED` (mid-retry) → wait for the retry timer / poll, then proceed.
- Not found → this is "the first not-yet-completed operation": run it for real, checkpoint start, execute, checkpoint result.

JS implements this via `peekStepId()` (counter+1, not yet incremented) + `getStepData()`. Java implements it identically via `ExecutionManager.getOperationAndUpdateReplayState()`. **Go port: identical** — a `Context.nextOperation(id string) (*Operation, bool)` lookup before running any operation body.

### 4. Suspension mechanism — the one piece that does NOT translate directly, and where Java is the right reference for Go

**JS (single-threaded, event loop):** `Promise.race([handlerPromise, terminationPromise])` in `with-durable-execution.ts`. The `TerminationManager` holds one single-resolution promise. A `CheckpointManager` runs a state machine per pending operation (`IDLE_NOT_AWAITED` → `IDLE_AWAITED` → `RETRY_WAITING`/etc.) and decides "should we terminate" based on: checkpoint queue empty + not processing + no operation `EXECUTING` + at least one operation `RETRY_WAITING`/`IDLE_AWAITED` with nothing else pending. It schedules termination after a **cooldown timer** (`CHECKPOINT_TERMINATION_COOLDOWN_MS`) that gets cancelled if conditions change (e.g. another concurrent branch is still running). When it fires, `terminationManager.terminate()` resolves the race, and the handler promise is left permanently unresolved (GC'd when the Lambda process is frozen/recycled).

**Java (threaded — this is the directly portable model for Go):**
- `ExecutionManager` keeps a `Set<String> activeThreads`. A thread is "active" when it can make forward progress.
- Every operation's `get()` calls `waitForOperationCompletion()`: if the operation isn't done, attach a completion callback that will **re-register** the calling thread, then **deregister** the calling thread, then block on `completionFuture.join()`.
- `deregisterActiveThread()` checks: if `activeThreads` becomes empty, no thread anywhere can make progress → `suspendExecution()` completes an `executionExceptionFuture` with `SuspendExecutionException`.
- The top-level driver races `CompletableFuture.anyOf(handlerFuture, executionExceptionFuture)`. If suspension wins, return `PENDING` and abandon the handler thread (it's blocked forever on a `join()` that will never complete in this Lambda invocation — fine, because the whole process is about to be frozen/recycled).
- Steps and child contexts run on their **own thread** (via a user-configurable `ExecutorService`), registered *before* being submitted (to avoid a race where the parent deregisters before the child registers). SDK-internal coordination (checkpoint batching, wait/callback polling) runs on a **separate internal daemon-thread pool**, isolated from user code.
- Checkpoint completion (`onCheckpointComplete`) is what resolves an operation's `completionFuture`, which is what triggers the `thenRun` re-registration, which is what makes the blocked thread's `join()` return.

**Go translation:** Go's goroutines + channels + `select` map onto this almost mechanically:
| Java | Go |
|---|---|
| `Set<String> activeThreads` + counting | `sync.WaitGroup`-like counter guarded by a mutex, or an atomic counter, checked on every decrement |
| `CompletableFuture<T> completionFuture` per operation | `chan struct{}` (closed = done) or `chan Result[T]` per operation |
| `CompletableFuture.anyOf(handlerFuture, executionExceptionFuture)` | `select { case <-handlerDone: ... case <-suspendCh: ... }` |
| Thread registration/deregistration around blocking `get()` | Explicit `ctx.deregister()` / `ctx.register()` calls around a channel receive, exactly mirroring Java's `synchronized(completionFuture)` critical section to avoid the same race |
| User executor vs internal executor | Two goroutine "pools" is unnecessary in Go (goroutines are cheap, no pool needed) — but the *isolation principle* still matters: user step code and SDK coordination (polling, checkpoint batching) should run on logically separate goroutines so a blocked/slow step body can't wedge SDK bookkeeping. Achieve this by never running SDK coordination logic *inside* a step's goroutine. |
| `SuspendExecutionException` | A sentinel error or a dedicated `suspended` result type returned up the call stack instead of panicking — Go idiom favors explicit returns over exception-like control flow |

This is the model to implement. It is a closer structural match to Go than the JS event-loop/promise-race model, exactly because Java and Go are both natively multi-threaded.

### 5. Checkpoint batching (JS is most detailed here; Java agrees on the concept)

Both batch multiple pending operation updates into one API call, bounded by payload size (JS: 750KB / 250 items) and optionally by a small delay (Java: `checkpointDelay`, default 0 = flush ASAP). JS uses an explicit queue + `setImmediate`-driven drain loop; Java uses a `CheckpointBatcher` with the same size/latency tradeoff. **Go port:** a queue (slice) protected by a mutex, drained by a single dedicated goroutine woken via a buffered signal channel — directly analogous to JS's queue + `setImmediate` and Java's dedicated internal executor thread. Same size bound (750KB payload, configurable item cap) should be preserved since it's presumably a backend-enforced limit, not an SDK preference.

### 6. Retry / step semantics (all three SDKs agree)

- `AtLeastOncePerRetry` (default): checkpoint happens *after* the step body runs. On replay mid-retry, if status is `STARTED` (not yet `SUCCEEDED`/`FAILED`), the step body is simply re-run.
- `AtMostOncePerRetry`: checkpoint `START` happens *before* the step body runs (synchronously awaited). If the process dies between the `START` checkpoint and completion, replay sees `STARTED` and raises `StepInterruptedException`/`StepInterruptedError` instead of re-running — the retry strategy is then consulted on that synthetic error like any other failure.
- Retry delay: computed by the configured `RetryStrategy`/`retryStrategy` function; checkpointed as `NextAttemptTimestamp` (or equivalent) so replay can determine "has the delay elapsed" without depending on wall-clock state in memory.

This already matches the current scaffold's `types.StepSemantics` design; no changes needed there beyond wiring it into the real state machine.

## Go implementation plan

Given the above, the Go runtime will be built around:

1. **`execmgr` (new internal package):** the Go analog of Java's `ExecutionManager` — holds the operation log (map keyed by step ID), the active-goroutine counter, the suspend channel, and the checkpoint queue. This is the piece that did not exist in the original scaffold at all.
2. **`context/durablecontext.go`:** concrete implementation of `types.DurableContext`, holding a step counter + prefix (per §1) and a reference to the shared `execmgr`. Every `operations.*` function currently calls into `errNotImplemented`; they'll instead call a method on this concrete context that does replay-lookup (§3) then either returns the cached result or runs the operation body and coordinates suspension (§4) via the concrete context's `execmgr`.
3. **`checkpoint/manager.go`:** the batching queue (§5) plus the `Client` calls, replacing the currently-empty `Client` interface usage in `durable.go`.
4. **`durable.go` `WithDurableExecution`:** replaced with the actual race: start the handler on its own goroutine, `select` on handler-done vs. suspend-signal, matching Java's `anyOf(handlerFuture, executionExceptionFuture)`.

This is being implemented incrementally on the `golang` branch; see commit history for the breakdown into reviewable pieces.

## Real-world verification (2026-07-16)

Deployed `examples/simple-step-go` as an actual Lambda container-image durable function in a real AWS account and ran it end to end. This resolved two open questions empirically rather than by inference, and surfaced concrete bugs no amount of documentation review would have caught:

**Confirmed:**
- Go works as a container-image runtime for durable functions - no infrastructure-level block. `create-function --durable-config` accepts it; checkpointing and completion behave normally.
- The real `DurableExecutionInvocationInput` wire shape, captured directly from CloudWatch Logs: `DurableExecutionArn`, `CheckpointToken`, `InitialExecutionState.Operations[]` (with the root EXECUTION operation carrying `ExecutionDetails.InputPayload` and a top-level `StartTimestamp`), and `UpdatedOperationIds` - present even on the execution's true first invocation.
- The real `CheckpointDurableExecutionResponse` shape, captured from an actual successful checkpoint call: `Operation` uses per-type nested `*Details` structs (`StepDetails.Attempt`, `StepDetails.Result`, etc.), confirmed against both the official AWS API Reference and live traffic.
- A full `SUCCEEDED` durable execution, driven entirely by this SDK's `operations.Step`/`durable.WithDurableExecution`, checkpointing against the real backend.

**Bugs found and fixed via this process:**
1. Wire types (`types/wire.go`) were rebuilt against the official AWS API Reference (`API_CheckpointDurableExecution.html`, `API_GetDurableExecutionState.html`) after CLI help text alone led to an incorrect flat-fields model.
2. Timestamps use **two different encodings** on the real wire: unix-millis integers on `DurableExecutionInvocationInput` (invocation input) vs. ISO 8601 strings on `CheckpointDurableExecutionResponse` (checkpoint output) - a genuinely non-obvious inconsistency, fixed with a `types.Time` wrapper that accepts either, with regression tests (`wire_test.go`).
3. A hand-rolled SigV4 client (`sigv4lambda`) was built and deployed, but produced a real 403 signature mismatch not worth further debugging time - abandoned in favor of a CLI-shim `checkpoint.Client` (`awscli`) to keep verifying the SDK's actual logic. Neither is the intended production client; both exist only because the AWS SDK for Go v2 could not be fetched in this development environment (no outbound network access to the Go module proxy).

**Still unverified:**
- The exact accepted string values for `DurableExecutionOutput.Status` beyond `"SUCCEEDED"` (confirmed working) - `"FAILED"` was accepted by the backend in earlier test invocations (no "Invalid Status" rejection), but `"PENDING"`'s wire behavior (actual suspension via Wait/Callback) has not yet been exercised against the real backend.
- `GetDurableExecutionState`'s exact response shape from a live call (only `GetDurableExecutionHistory` and a live `CheckpointDurableExecutionResponse` have been directly observed).
