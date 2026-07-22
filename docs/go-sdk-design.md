# AWS Durable Execution SDK for Go — Design Scaffold

Status: **partially functional.** `Step` and `Wait` are backed by a real, tested checkpoint/replay state machine (see docs/checkpoint-replay-design.md). `RunInChildContext`, `Map`, `Parallel`, `CreateCallback`, `WaitForCallback`, `WaitForCondition`, and `Invoke` are still scaffold stubs returning "not yet implemented" errors. No production AWS SDK-backed backend client exists yet - callers must supply their own `checkpoint.Client`.

## Why this exists

The Durable Execution SDK currently ships for JavaScript/TypeScript, Python, and Java (see the [SDK docs](https://docs.aws.amazon.com/durable-execution/)). This scaffold explores what an official Go SDK's public API should look like before committing to a full implementation.

## Design basis

- **Execution semantics are fixed by the backend and are not a language-specific choice.** Checkpoint/replay behavior, hierarchical step IDs, and the wire protocol (`pkg/durable/types/wire.go`) must stay consistent with the JS/Python/Java SDKs.
- **The API surface is modeled on the Java SDK's operation set**, not ported syntactically from either JS or Java. Java's execution model (compiled, thread-based, error-return via checked exceptions) maps more directly onto Go's goroutine/error-return model than TypeScript's async/await + event-loop model does. Concretely, this scaffold covers the same operations Java exposes on `DurableContext`: `Step`, `Wait`, `CreateCallback`, `WaitForCallback`, `WaitForCondition`, `Invoke`, `RunInChildContext`, `Map`, `Parallel`.
- **The API idiom is Go, not a transliteration of either source SDK:**
  - Generic, package-level functions (`operations.Step[T](dc, id, fn, opts...)`) instead of methods on a generic context class.
  - Functional options (`operations.WithStepRetryStrategy[T](...)`) instead of config-object builders.
  - Two context types — `types.DurableContext` (handler/child/map/parallel) and `types.StepContext` (inside step callbacks) — enforce at the type level that durable operations can't be called from inside a step body.
  - `dc.Context()` / `sc.Context()` expose the underlying `context.Context` for cancellation and for passing to AWS SDK/HTTP calls, matching standard Go I/O conventions.
  - `error` return values throughout, no panics for control flow.

## What exists now (updated after checkpoint/replay implementation)

```
pkg/durable/
├── durable.go              # WithDurableExecution: real suspend-or-complete race (see checkpoint-replay-design.md §4)
├── determinism.go          # CurrentTime: reads the checkpointed root Execution operation's start timestamp
├── types/
│   ├── types.go            # DurableContext, StepContext, Logger, Duration, Serdes, etc.
│   └── wire.go             # Wire-protocol types (Operation, OperationUpdate, per-type Details structs)
├── execmgr/                # NEW: active-goroutine counting + suspend/wake coordination (Go analog of Java's ExecutionManager)
├── checkpoint/             # NEW: batching queue + Client dispatch (Go analog of JS CheckpointManager / Java CheckpointBatcher)
├── context/
│   └── dcontext.go         # NEW: concrete DurableContext/StepContext, step-ID generation, replay lookup
├── operations/
│   ├── step.go              # Step - REAL implementation: replay-skip, retry, AtMostOnce/AtLeastOnce semantics
│   ├── wait.go               # Wait - REAL implementation: checkpoint + suspend-coordinated block
│   ├── wait_for_condition.go # WaitForCondition - still scaffold (errNotImplemented)
│   ├── callback.go           # CreateCallback, WaitForCallback - still scaffold
│   ├── invoke.go             # Invoke, RunInChildContext - still scaffold
│   ├── batch.go              # Map, Parallel, All, AllSettled, Any, Race - still scaffold
│   └── errors.go
└── utils/
    ├── serdes.go             # Default JSON Serdes
    ├── logger.go             # DefaultLogger, NopLogger
    └── retry.go              # RetryStrategyConfig, CreateRetryStrategy, Presets
```

Step and Wait are backed by a real, tested checkpoint/replay state machine (`execmgr` + `checkpoint` + `context`), validated by unit tests covering: happy-path execution, replay-skip (a completed step's body is not re-run), retry-then-succeed, exhausted-retries failure, and Wait-based suspension - including a dedicated `execmgr` test that verifies true suspension (no active goroutines) and wake-on-completion in isolation from any backend. All tests pass under `go test -race`.

See docs/checkpoint-replay-design.md for the cross-SDK research (Java + TypeScript source, not just docs) this implementation is based on, and its "Go implementation plan" section for the mapping from Java's threaded suspension model to Go's goroutines/channels.

## What is explicitly NOT done yet

- **RunInChildContext, Map, Parallel, and their combinators (All/AllSettled/Any/Race)** are still scaffold stubs. These are the highest-risk remaining pieces per the community Go attempt's experience (multiple bug-fix passes needed specifically here) and per the Java design doc's description of per-branch goroutine registration ordering (register on the *parent* thread before spawning, to avoid a race where the parent deregisters before the child registers - this ordering constraint carries over directly to Go and needs the same care).
- **CreateCallback / WaitForCallback / WaitForCondition** are still scaffold stubs. WaitForCallback's suspend/resume shape is structurally the same as Wait's (checkpoint + block via `execmgr.WaitForOperation`), so it should be the next piece implemented; WaitForCondition additionally needs a polling loop against the backend.
- **Invoke** is still a scaffold stub.
- **No AWS SDK-backed `checkpoint.Client` implementation** - `Config.Client` must currently be supplied by the caller (a real implementation, or a test fake like the one used in `durable_test.go`).
- **Retry-delay suspension is not yet implemented** - `retryOrFail` currently re-executes a step immediately after checkpointing a `Retry` action rather than suspending the whole invocation for long delays the way `Wait` does. This is flagged explicitly in `step.go`'s comments as a follow-up requiring the same cooperative-suspend mechanism `Wait` uses, generalized to retry timers.
- **Checkpoint batching delay / `CheckpointStrategyBatched` / `CheckpointStrategyOptimistic`** are accepted as config values but not wired to distinct runtime behavior yet - the current `checkpoint.Manager` always batches eagerly (drains as soon as signaled).
- No local testing SDK equivalent (`LocalDurableTestRunner` in Java/JS), no examples, no CI.
- No OpenTelemetry plugin.
- No hierarchical replay validation (the JS/Java SDKs raise a `NonDeterministicExecutionException`-equivalent if replay encounters an operation type/order mismatch from the checkpoint log - not yet implemented here).

## Suggested next steps

1. Implement `WaitForCallback` (structurally close to `Wait`) and `CreateCallback`, then `RunInChildContext` (needed as a building block for `Map`/`Parallel`).
2. Implement `Map`/`Parallel` with careful attention to the goroutine registration-ordering constraint noted above - write concurrency-focused tests (`-race`) before considering this done, given the community Go attempt's specific history of bugs here.
3. Implement retry-delay suspension so long backoff delays don't block a Lambda invocation in-process.
4. Implement a real AWS SDK-backed `checkpoint.Client`.
5. Add a local testing SDK (equivalent to `LocalDurableTestRunner`) before writing example applications.
