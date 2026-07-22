# TypeScript SDK Examples vs. Go SDK Example Coverage — Comparison

**What this document is:** a point-in-time inventory of the TypeScript/JavaScript AWS Durable
Execution SDK's `aws-durable-execution-sdk-js-examples` package (the public repo
[`aws/aws-durable-execution-sdk-js`](https://github.com/aws/aws-durable-execution-sdk-js),
`packages/aws-durable-execution-sdk-js-examples/src/examples/`), cross-referenced against this
repo's own `examples/*-go` directories as they exist today.

**When produced:** 2026-07-17, by reading the TS repo's example source/test files directly via
the GitHub API/raw content endpoints (public, no auth), and by reading every Go example's
`handler_test.go` (and sibling test files) in full.

**This is a snapshot, not a live-synced comparison.** The TS repo will keep adding examples
(it has ~30 top-level example groups today, several with 10+ sub-variants each — e.g.
`wait-for-callback` alone has 15 variant subdirectories, `map` has 11, `parallel` has 14), and
this Go repo's example catalog will keep growing too. Treat the "Go SDK Equivalent Exists?"
column as accurate as of the commit state described in `docs/remaining-work.md` at the time this
document was originally written (10 Go examples: `simple-step-go`, `run-in-child-context-go`,
`wait-for-callback-go`, `wait-for-condition-go`, `chained-invoke-go`, `map-parallel-go`,
`retry-go`, `error-handling-go`, `logging-go`, `custom-config-go`); two more were added in later
sessions closing specific gaps this document identified - `completion-config-go` (the
`CompletionConfig`/Map-Parallel gap) and `large-payload-go` (the large-payload/Serdes-overflow
gap, gap 6/8 below) - bringing the current total to 12; two more were added in a later session
(`docs/remaining-work.md` §10 task 28) closing this document's `parallel-processing` and
`map-with-condition-and-callback` gaps - `parallel-processing-go` and
`map-with-condition-and-callback-go` - bringing the current total to 14. That same session also
closed the `hello-world`/`generic-types` catalog entries as permanent, explicit "not applicable"
decisions rather than examples (see the "No Go equivalent at all" section below). Re-derive this comparison rather than
trusting it blindly if significant time has passed.

This document does **not** replace or modify `docs/remaining-work.md` — it is a new, standalone
reference focused specifically on test-scenario- and assertion-level granularity that the
existing tracker doesn't spell out in table form.

## How the TS examples package works (confirmed from source)

The TS SDK's examples package (`ADDING_EXAMPLES.md`, `SDK_COVERAGE.md`) is explicitly the SDK's
own integration test suite, not documentation-only sample code — this matches what
`docs/remaining-work.md`'s completion-criteria section already says, and this session's reading
of the actual source confirms it precisely:

- Every example is `{example-name}.ts` (the handler) + `{example-name}.test.ts` (the test) +
  an optional `{example-name}.history.json` golden file, either standalone
  (`src/examples/{name}/`) or grouped into nested variants
  (`src/examples/{group}/{variant}/{group}-{variant}.ts`).
- Tests are written via a shared `createTests({ handler, tests })` helper
  (`../../utils/test-helper`), which hands the test body a `runner` and a
  `{ assertEventSignatures, isCloud, isTimeSkipping, functionNameMap }` helper object.
- **`createTests` runs every example against a `LocalDurableTestRunner` by default, and against
  a `CloudDurableTestRunner` when `NODE_ENV=integration` is set** — exactly the env-var-driven
  dual-runner pattern `docs/remaining-work.md` describes citing this repo's own documentation.
  The cloud path additionally requires a `FUNCTION_NAME_MAP` env var (mapping example names to
  real deployed Lambda ARNs) and drives through the CI workflow
  `.github/workflows/integration-tests.yml`, which builds, deploys via SAM, then runs
  `npm run test:integration`.
- **`assertEventSignatures(execution)` is mandatory in every single test** — the package's own
  tooling enforces this (`"assertEventSignature was not called for test [name]"` is a real,
  documented error). It compares the actual checkpointed operation event sequence against a
  committed `.history.json` golden file, regenerated via `GENERATE_HISTORY=true npm test`. This
  is the direct ancestor of this Go repo's own `testing.AssertEventSignatures` /
  `testdata/*.history.json` mechanism (`docs/remaining-work.md` §7 task 17c).
- Multiple history files per example are supported via a second string argument
  (`assertEventSignatures(execution, "success")` → `{name}-success.history.json`), used when one
  example has several meaningfully distinct scenarios (matching this Go repo's own convention of
  one golden file per scenario, established in the same task 17c writeup).
- A separate SDK-coverage harness (`jest.config.sdk-coverage.js`, `scripts/copy-sdk-source.js`)
  runs the whole example suite with the core SDK's own source copied in and instrumented, to
  measure how much of the core SDK the example suite exercises — this Go repo's task 17d ("SDK
  coverage-via-examples") is the direct, currently-unstarted analog.

## Full list of TS example groups found (`src/examples/`)

30 top-level entries, several containing many nested variant subdirectories (variant counts
in parentheses; `shared` is test infrastructure, not an example):

`block-example`, `child-operations-invalid-depth`, `child-operations-preservation`,
`comprehensive-operations`, `concurrent`, `context-validation`, `create-callback`,
`error-determinism`, `force-checkpointing`, `handler-error`, `hello-world`, `invoke` (3:
`simple`, `tenant-id`, `tenant-target`), `large-payload`, `logger-test` (5: `after-callback`,
`after-wait`, `log-levels`, `powertools-logger`, `simple-powertools-logger`),
`map-completion-config-issue`, `map` (11: `basic`, `empty`, `error-preservation`,
`error-type-preservation-replay`, `failure-threshold-exceeded-count`,
`failure-threshold-exceeded-percentage`, `high-concurrency-invoke`, `large-scale`,
`min-successful`, `tolerated-failure-count`, `tolerated-failure-percentage`,
`virtual-context`), `multiple-waits`, `no-replay-execution`, `non-durable`, `otel`, `parallel`
(14: `basic`, `custom-summary-generator`, `empty`, `error-preservation`,
`failure-threshold-exceeded-count`, `failure-threshold-exceeded-percentage`, `heterogeneous`,
`invoke`, `min-successful-with-callback`, `min-successful-with-passing-threshold`,
`min-successful`, `should-complete`, `tolerated-failure-count`,
`tolerated-failure-percentage`, `virtual-context`, `wait`), `promise`, `retry-exhaustion`,
`run-in-child-context` (8: `basic`, `checkpoint-size-limit`, `error-data-propagation`,
`large-data`, `serdes-large-payload`, `serdes-virtual`, `serdes`, `with-failing-step`),
`serde`, `shared` (test infra, not an example), `simple-execution`, `step` (7:
`attempt-fallback`, `basic`, `interrupted-no-retry`, `named`, `step-error-determinism`,
`steps-with-retry`, `with-retry`), `undefined-results`, `wait-for-callback` (15: `anonymous`,
`basic`, `child-context`, `error-instance-failure`, `error-instance-submitter`,
`error-instance-timeout`, `failing-submitter`, `failures`, `heartbeat-sends`, `mixed-ops`,
`multiple-invocations`, `nested`, `quick-completion`, `serdes`,
`submitter-failure-catchable`, `submitter-retry-success`, `timeout`), `wait-for-condition`,
`wait`, `with-retry` (2: `callback`, `invoke`).

That is roughly 90+ individual test files once every variant is counted — far larger than the
Go SDK's 10-example catalog. The tables below focus on the TS groups that have a direct
conceptual counterpart among the Go SDK's 10 examples (Step, RunInChildContext,
WaitForCallback, WaitForCondition, Invoke, Map/Parallel, Retry, error handling, logging, custom
config), since those are what the Go side can actually be compared against; the remaining TS
groups (`otel`, `promise`, `hello-world`, `serde` at the top level, `non-durable`,
`concurrent`, `undefined-results`, `context-validation`, `multiple-waits`,
`no-replay-execution`, `block-example`, `child-operations-*`, `comprehensive-operations`,
`map-completion-config-issue`, `force-checkpointing`, `retry-exhaustion`,
`simple-execution`) are called out separately at the end as pure gaps with no Go equivalent at
all, since the Go SDK has no examples in those specific niches. (`large-payload` is NOT in this
list, unlike when this document was originally written - it now has its own dedicated row in
the "Serialization / config" table below, closed by `examples/large-payload-go` in a later
session.)

---

## Step

| TS Example | Feature Demonstrated | Test Scenario(s) | Assertion Style | Local + Cloud Coverage | Go Equivalent? | Go Notes |
|---|---|---|---|---|---|---|
| `step/basic` (`step-basic.ts`/`.test.ts`) | Single `context.step()` call | Happy path only | Result equality (`toStrictEqual`) + explicit `OperationType`/`OperationStatus` checks on the step operation + `assertEventSignatures` (golden file) | Both, via `createTests` | Yes | `simple-step-go`'s `TestHandler_CreatesGreeting` — result equality, `GetType`/`GetStatus` checks, `StepResult[T]`, plus `AssertEventSignatures` golden file. Near-identical assertion shape. |
| `step/named` | Named steps (explicit step name vs. auto-generated) | Happy path | Result + signature | Both | Yes | All Go examples use named steps by convention (`operations.Step(dc, "create-greeting", ...)`); no dedicated "named vs. anonymous" example exists on the Go side, but the capability itself is exercised everywhere. |
| `step/with-retry`, `step/steps-with-retry` | `context.step()` with a configured retry strategy, incl. a realistic DynamoDB-polling scenario mixing `step` + `wait` | 5 distinct cases: succeeds first try; succeeds after polling (wait between polls); succeeds after a transient failure is retried; **fails after exhausting retries** (asserts structured `StepError` shape with nested original `Error`); fails when item never found after max polls | Result equality **and** structured field checks on `StepDetails` (`.result`, `.error` with exact `errorType`/`errorMessage` shape) **and** `WaitDetails.waitSeconds`. No `assertEventSignatures` call visible in this particular file (uses `LocalDurableTestRunner` directly, not the `createTests` wrapper) | Local only in this file (direct `LocalDurableTestRunner`, no `NODE_ENV=integration` branch) | Partial | `retry-go`'s `handler_test.go` covers succeeds-after-retry, no-retry-needed, and retries-exhausted (`*operations.StepFailedError`) — matches 3 of the 5 TS scenarios. Go has no polling-with-wait-between-attempts scenario in `retry-go` itself (that pattern is closer to `wait-for-condition-go`, which is a distinct Go example). Go's exhausted-retry test asserts `StepDetails.Attempt` count and a non-empty error string, not a byte-for-byte structured error shape comparison — a materially looser assertion than the TS `toEqual({errorMessage, errorType, ...})` exact-shape check. |
| `step/attempt-fallback` | Reading `sc.Attempt()`/fallback behavior on retry attempts | Not read in full this session | — | — | Partial | `retry-go`'s handler comment notes `sc.Attempt()` is correctly available inside `fn` and logged, but there's no dedicated assertion isolating fallback-by-attempt-number behavior the way this TS example's name implies. |
| `step/interrupted-no-retry` | Step interrupted (e.g. Lambda timeout) vs. a real retryable failure — no retry attempted for non-retryable interruptions | Not read in full this session | — | — | No | No Go example distinguishes "interrupted" (infra-level) from "failed" (application-level) step outcomes. This is a real, distinct gap — the Go SDK's `StepFailedError` hierarchy doesn't currently model an "interrupted, don't retry" classification separately from a normal retryable failure. |
| `step/step-error-determinism` | Replay-determinism of step error types/messages across invocations | Not read in full this session | Likely golden-file/signature-based given the name | — | Partial | Go's `error-handling-go/nondeterministic_test.go` covers the broader non-deterministic-replay case (different operation type at the same step ID), but not specifically "does a step's error TYPE survive replay round-trip identically" in isolation. |

## RunInChildContext

| TS Example | Feature Demonstrated | Test Scenario(s) | Assertion Style | Local + Cloud Coverage | Go Equivalent? | Go Notes |
|---|---|---|---|---|---|---|
| `run-in-child-context/basic` | Grouping steps under an isolated child context | Happy path (based on directory/file naming; not fetched in full this session due to a 404 on the exact filename guess) | Presumed result + signature per package convention | Both | Yes | `run-in-child-context-go`'s `TestHandler_ProcessesOrderThroughChildContext` — asserts on the CONTEXT operation type/status, its own checkpointed result, the 2 nested child STEP operations by name/type/status via `GetChildOperations`, the sibling root-level step's independence, **and** `AssertEventSignatures` (the first Go example with real nesting, per that test's own doc comment). This is comprehensive, arguably deeper than a typical single "happy path" TS test. |
| `run-in-child-context/with-failing-step` | A step inside the child context fails, propagating failure to the whole context | — | — | — | Yes | **Closed this session.** `run-in-child-context-go`'s new `TestHandler_PaymentStepFails` (`handler_test.go`) adds a second, distinct trigger (a negative `Amount`, alongside the pre-existing zero-amount/`TestHandler_InvalidAmount` falsy-non-error case, which was deliberately left unchanged rather than silently repurposed — see handler.go's own doc for the reasoning) that makes the `charge-card` step return a genuine Go error (`errCardChargeDeclined`, via `utils.Presets.NoRetry()` for a deterministic single-attempt failure), asserting: the whole execution FAILS; the nested `charge-card` STEP is checkpointed FAILED with the real error recorded; the ENCLOSING `payment` CONTEXT/RUN_IN_CHILD_CONTEXT operation is ALSO checkpointed FAILED (not just its child); the handler's own `errors.As(err, &operations.ChildContextFailedError{})` inspection (mirroring `error-handling-go`'s identical pattern for `StepFailedError`) recovers `ID`/`Reconstructed`, surfaced through `TestResult.GetError()`'s string-based message (confirmed to be the only available inspection surface from the testing package's public API — `TestResult`/`Operation` expose no live Go error value to run `errors.As` against directly, only flattened `ErrorMessage` strings, per `result.go`/`runner.go`); and a dedicated `AssertEventSignatures` golden file (`testdata/TestHandler_PaymentStepFails.history.json`) pinning the new failure shape (`CONTEXT/RUN_IN_CHILD_CONTEXT FAILED` nesting one `SUCCEEDED` step and one `FAILED` step, with no sibling `create-shipping-label` at all). **Also verified against a real deployed Lambda function** (`run-in-child-context-go-example`, newly deployed this session — see `docs/remaining-work.md`'s example-related entries for the full deployment/verification writeup): a real invoke with a negative amount produced a genuine terminal `Status: FAILED` via `GetDurableExecution`, and `GetDurableExecutionHistory` showed the exact expected real-backend shape — `StepFailed` for `charge-card`, `ContextFailed` for the enclosing `payment` CONTEXT (both `Id`/`ParentId` real 64-character SHA-256 hex hashes with correct parent/child linkage) — the first FAILURE-path real-cloud verification in this document; every prior cloud verification had been a SUCCESS-path confirmation only. |
| `run-in-child-context/error-data-propagation` | Original error data/type preserved when surfaced from a child context | — | — | — | Partial | Go's `operations.ChildContextFailedError` (`Reconstructed`/`Original` fields, per `docs/remaining-work.md` §4 task 10) models exactly this distinction in the SDK itself, but no *example* test specifically exercises and asserts on it end-to-end the way this TS example does. |
| `run-in-child-context/large-data`, `checkpoint-size-limit`, `serdes-large-payload` | Large payload handling inside a child context, incl. checkpoint size limits | — | — | — | Partial | `docs/remaining-work.md` §6 task 16 covers large-payload handling at the SDK level (`operations.checkResultSize` / `ResultTooLargeError`), and `examples/large-payload-go` (closed in a later session - see that document's task 16 update) now demonstrates it via plain `Step` calls, not scoped to a `RunInChildContext`. No Go example demonstrates the SPECIFIC combination this TS group names (a large result checkpointed from WITHIN a child context specifically) - `checkResultSize` is called uniformly across step.go/invoke.go/batch.go/wait_for_condition.go regardless of nesting, so the underlying mechanism should already behave identically inside a child context, but this has not been demonstrated by a dedicated example. |
| `run-in-child-context/serdes`, `serdes-virtual` | Custom Serdes scoped to a child context | — | — | — | Yes | **Closed this session (docs/ts-sdk-examples-comparison.md gap 8/8).** `custom-config-go` demonstrates a custom `Serdes` scoped to a single `Step` (`WithStepSerdes`); `run-in-child-context-go`'s new `OrderEvent.UseCustomChildSerdes:true` scenario (`customSerdesHandlerScenario`, `handler.go`) now additionally applies the SAME custom `Serdes` implementation (`screamingSnakeCaseSerdes`, duplicated verbatim from `custom-config-go` rather than reinvented) to a `RunInChildContext` call's OWN checkpointed result via `WithChildSerdes` — a genuinely different operation scope, not a different serialization scheme. `TestHandler_ReconciliationUsesCustomChildSerdes` asserts on the RAW checkpointed CONTEXT payload string (`GetContextDetails().Result`) to prove `WithChildSerdes` genuinely ran (SCREAMING_SNAKE_CASE keys `"RECONCILED_TOTAL"`/`"AUDIT_NOTE"`), and additionally asserts the NESTED step's own checkpointed payload remains the plain default format — proving the custom Serdes is scoped to the CONTEXT, not leaking into everything nested inside it. Has a committed `AssertEventSignatures` golden file. **Verified against a real deployed Lambda function**: the already-deployed `run-in-child-context-go-example` function was rebuilt/redeployed (version 4) and invoked with `useCustomChildSerdes:true`; `GetDurableExecution` reached genuine terminal `SUCCEEDED`, `GetDurableExecutionHistory` showed the real `ContextStarted`/`ContextSucceeded` (`RUN_IN_CHILD_CONTEXT`) shape nesting a real `StepSucceeded`, and — since `GetDurableExecutionHistory`'s own Result/Payload fields came back `Truncated:true`, matching every prior real-cloud verification in this document — CloudWatch Logs confirmed the actual checkpointed payload content was genuinely `{"AUDIT_NOTE":"reconciled order recon-cloud-2","RECONCILED_TOTAL":999.01}`, i.e. really SCREAMING_SNAKE_CASE on the real backend, not just locally. `run-in-child-context-go`'s pre-existing `serdes-virtual` analog (a "virtual context" concept) remains unmatched — see the separate `run-in-child-context/virtual` row below, which is still an open question about what that TS concept even means at the SDK level. |
| `run-in-child-context/virtual` | "Virtual context" (a context-like grouping without its own checkpoint?) | — | — | — | No | No Go concept or example matches this; would need investigation into what "virtual context" means in the TS SDK before assessing whether Go has an analog at the SDK level at all. |

## WaitForCallback

| TS Example | Feature Demonstrated | Test Scenario(s) | Assertion Style | Local + Cloud Coverage | Go Equivalent? | Go Notes |
|---|---|---|---|---|---|---|
| `wait-for-callback/basic` | Callback create → submit → await → resolve | 3 cases in one file: **succeeds** (`sendCallbackSuccess`), **fails** (`sendCallbackFailure`), **times out** (no callback sent within `timeoutSeconds`) | Result/error equality + `assertEventSignatures(execution, "success"\|"failure"\|"timed-out")` — **3 separate named golden files per scenario in a single test file** | Both (`InvocationType.Event` + `createTests`) | Partial | `wait-for-callback-go`'s `TestHandler_ApprovedExpense`/`TestHandler_RejectedExpense` cover success and failure with their own golden files (`.history.json`), matching the TS success/failure split closely, including asserting on the nested `CALLBACK` and submitter `STEP` operations by name/status. **Go has no timeout scenario at all** — `docs/remaining-work.md` itself states heartbeat/timeout enforcement (`WithWaitForCallbackTimeout`) is accepted but NOT enforced in the SDK yet, so a timeout test isn't possible until that lands. This is a confirmed, real gap, not an oversight in the example. |
| `wait-for-callback/timeout` | Dedicated timeout-focused example (separate from the 3-in-1 basic test) | `CallbackTimeoutError` on timeout | Exact structured error-object equality (`errorType: "CallbackTimeoutError"`) | Both | No | Same root cause as above — no Go timeout support yet. |
| `wait-for-callback/heartbeat-sends` | Heartbeat mechanism to keep a long-running callback alive | — | — | — | No | `docs/remaining-work.md` confirms no heartbeat helper exists in Go's testing package either (`SendCallbackSuccess`/`Failure` exist; no heartbeat equivalent), consistent with no heartbeat-timeout SDK support. |
| `wait-for-callback/failing-submitter`, `submitter-failure-catchable`, `submitter-retry-success` | The **submitter step itself** (not the callback) fails/retries — Step-style retry semantics applied to the submitter | — | — | — | Partial | Go's `WaitForCallback` composition includes "a submitter Step (with full Step-style retry)" per `docs/remaining-work.md`'s own description of the flowchart, so the SDK-level capability exists, but no Go example test specifically drives a failing/retrying submitter step in isolation. |
| `wait-for-callback/error-instance-failure`, `error-instance-submitter`, `error-instance-timeout` | Preserving real `Error` instance shape/type across the 3 different failure modes | — | — | — | Partial (submitter/failure) / No (timeout) | Same timeout gap as above; the failure/submitter error-instance cases are partially covered by Go's structured `CallbackFailedError`, but not with dedicated instance-preservation assertions. |
| `wait-for-callback/child-context`, `nested`, `mixed-ops` | Callback used inside a child context / nested / alongside other operation types | — | — | — | Partial | Go's `map-parallel-go` demonstrates callbacks composed with other operations only implicitly (it doesn't use callbacks at all, actually — it uses Step/Map/Parallel). No Go example nests a callback inside a `RunInChildContext` or mixes it with Map/Parallel branches directly. |
| `wait-for-callback/multiple-invocations`, `quick-completion`, `anonymous`, `serdes`, `failures` | Various edge cases: multiple suspend/resume round-trips, immediate resolution, anonymous (unnamed) callbacks, custom Serdes on the callback payload, generic failure variants | — | — | — | No (mostly) | Go's `wait-for-callback-go` covers exactly 2 scenarios (approved/rejected) against 1 suspend/resume cycle. None of these edge variants (multi-invocation chains, custom Serdes on callback payload, unnamed callbacks) have Go equivalents. |

## WaitForCondition

| TS Example | Feature Demonstrated | Test Scenario(s) | Assertion Style | Local + Cloud Coverage | Go Equivalent? | Go Notes |
|---|---|---|---|---|---|---|
| `wait-for-condition` (single example, no variants) | Polling until a condition is met | 1 test: "should invoke step three times before succeeding" | Result equality (`toStrictEqual(3)`) + `assertEventSignatures` | Both | Yes | `wait-for-condition-go`'s `TestHandler_PollsUntilJobCompletes` — result equality (`FinalStatus`, `PollCount == 3`, matching the TS "three times" scenario almost exactly), operation type/status checks, and `AssertEventSignatures`. This is a close, direct match — likely the single best 1:1 parity pair in the whole comparison. TS has only ONE test scenario for this operation (no variant subdirectories at all, unlike `map`/`parallel`/`wait-for-callback`), so Go is not missing any TS-side scenario here. |

**`map-with-condition-and-callback` (a `docs/remaining-work.md` §10 task 23 catalog-tracked name,
not a literal enumerated TS example group) — closed this session (task 28).** No existing Go
example combined `operations.Map`, `operations.WaitForCondition`, AND
`operations.WaitForCallback` in one workflow before this: `map-parallel-go` combines `Map`+
`Parallel` with no `Wait*` operations; `wait-for-callback-go`/`wait-for-condition-go` each
demonstrate exactly one `Wait*` operation in isolation with no `Map`; `completion-config-go`
demonstrates `Map`'s `CompletionConfig` alone. `examples/map-with-condition-and-callback-go` now
demonstrates all three composed: a batch of orders priced via `Map`, one specific order's own Map
iteration additionally polling carrier status via a NESTED `WaitForCondition` (a different nesting
depth from `wait-for-condition-go`'s top-level-only call), and the WHOLE batch's completion gated
on a single `WaitForCallback` human-approval scoped to the aggregate result (not per-order, unlike
`wait-for-callback-go`'s own single-order approval scope). Two tests
(`TestHandler_ApprovedBatchWithCarrierPolling`, `TestHandler_RejectedBatch`), both with committed
golden files, verified locally (`-race -count=30`) — **implemented and locally verified only;
real-cloud deployment deferred this session**, per task 28's own honest scope note (prioritized
in favor of fully deploying `parallel-processing-go` given this session's added scope). See task
28's writeup in `docs/remaining-work.md` for the full detail.

## Invoke

| TS Example | Feature Demonstrated | Test Scenario(s) | Assertion Style | Local + Cloud Coverage | Go Equivalent? | Go Notes |
|---|---|---|---|---|---|---|
| `invoke/simple` | Basic cross-function invoke | Happy path (presumed; not fetched in full) | Presumed result + signature | Both | Partial/Yes | `chained-invoke-go` covers available/unavailable/service-error (3 scenarios, each with its own golden file) via a **mocked** target function (`RegisterDurableFunction`), matching the TS local-runner pattern. TS's `simple` variant likely covers just the happy path in isolation, so Go's 3-scenario spread is arguably broader for the core operation, though Go's mock-based approach for the target function is standard practice for both SDKs' local runners. |
| `invoke/tenant-id`, `tenant-target` | Multi-tenant invoke targeting/routing | — | — | — | No | No Go example or SDK-level concept of tenant-scoped invoke targets exists; this looks like a product feature specific to some multi-tenant invoke routing capability not yet ported to Go at all (worth flagging to the Go SDK owners as an open question — is this an operation-level feature or an application-level convention in TS?). |
| `parallel/invoke`, `map/high-concurrency-invoke` | Invoke composed inside Parallel/Map branches at scale | — | — | — | No | `map-parallel-go` uses only `Step` inside its Map iterations and Parallel branches, never `Invoke`. No Go example combines Invoke with Map/Parallel. |
| `with-retry/invoke` | Retry strategy applied around an Invoke call | — | — | — | Yes | **Closed this session.** `operations.Invoke` itself has NO retry-strategy option in the real API (independently re-confirmed by reading `pkg/durable/operations/invoke.go`/`types.ChainedInvokeOptions` in full — no backend-level retry field exists either, only `FunctionName`/`TenantID`), so "retry scoped to Invoke" is necessarily a CALLER-written manual retry loop, not an SDK primitive — matching the TS SDK's own likely scoping for this example. `examples/chained-invoke-go`'s new `RetryingInventoryCheckHandler` (`handler.go`) demonstrates exactly this: each attempt calls `operations.Invoke` at a genuinely new step ID (`check-inventory-attempt-N`), replay-safely, up to `MaxInventoryCheckAttempts` (3), recovering the specific `*operations.InvokeFailedError` type via `errors.As` to distinguish a genuine resolved failure (retry) from the SDK's own unexported suspension sentinel (propagate immediately, don't retry — see `docs/remaining-work.md` §10 task 26 for why this distinction, invisible in every prior example, had to be made explicit here). Tested locally (succeeds-after-N-failures, retries-exhausted-fails-the-execution, succeeds-first-try, each with a distinct `AssertEventSignatures` golden file) AND verified against REAL infrastructure: a genuine second Lambda function (`inventory-check-target`, newly deployed) was made to fail twice then succeed, and the real `GetDurableExecutionHistory` confirmed 3 distinct, real SHA-256-hashed `CHAINED_INVOKE` operations (2 FAILED, 1 SUCCEEDED) — a genuine multi-attempt retry against real cross-function invocation, not a single-success-only fallback. |

## Map / Parallel

| TS Example | Feature Demonstrated | Test Scenario(s) | Assertion Style | Local + Cloud Coverage | Go Equivalent? | Go Notes |
|---|---|---|---|---|---|---|
| `map/basic` | Fan-out over a collection | 2 tests in 1 file: correct child-operation count (`getChildOperations()` length), correct aggregated result array | `toHaveLength` on child ops + `toStrictEqual` on result array + `assertEventSignatures` (only on the second test in the file — note the first test in this file does NOT call `assertEventSignatures`, a minor inconsistency in the TS repo itself worth noting) | Both | Yes | `map-parallel-go`'s `TestHandler_PricesAndVerifiesConcurrently` — asserts on the outer `CONTEXT/MAP` operation, individual `MAP_ITERATION` children by name/status, result array length/values, plus `AssertEventSignatures`. Broader than TS's 2-assertion `map/basic` test. |
| `map/empty` | Zero-item Map | — | — | — | Yes | `map-parallel-go`'s `TestHandler_EmptyOrderBatch` — direct match, including its own dedicated golden file per the Go repo's "one golden file per distinct scenario" convention (matching TS's own `map/empty` being a separate example rather than a branch of `map/basic`). |
| `map/min-successful`, `tolerated-failure-count`, `tolerated-failure-percentage`, `failure-threshold-exceeded-count`, `failure-threshold-exceeded-percentage` | `CompletionConfig` evaluation (5 distinct variants) | — | — | — | Yes | `docs/remaining-work.md` confirms `CompletionConfig` (`MinSuccessful`/`ToleratedFailureCount`/`ToleratedFailurePercentage`) is fully implemented at the SDK level (§1 task 8) and even has a dedicated unit test for max-concurrency (§3 task 9's `TestLocalTestRunner_Parallel_MaxConcurrency`). **This gap is now closed**: `examples/completion-config-go` (added in a later session) exercises `WithMapCompletionConfig`'s `ToleratedFailureCount` threshold directly via `TestHandler_ToleratesFailuresWithinThreshold` (some items fail, the threshold absorbs them, the outer `CONTEXT/MAP` operation and the whole execution SUCCEED) and `TestHandler_ExceedsToleratedFailureThreshold` (same failures, a lower threshold correctly FAILS the batch), both asserted via `AssertEventSignatures` golden files, with the tolerated-failures-succeed scenario additionally verified against a real deployed Lambda function (see `docs/remaining-work.md` §10 task 23's note for the function ARN and what the real invoke confirmed). This narrows TS's 5-variant spread down to Go's single `ToleratedFailureCount`-based demonstration — `MinSuccessful` and `ToleratedFailurePercentage` remain unexampled in Go even though both are implemented at the SDK level, so this is a partial, not complete, close of the gap. |
| `map/error-preservation`, `error-type-preservation-replay` | Original error type/data preserved from a failed Map item, incl. across replay | — | — | — | Partial | SDK-level: Go's `BatchItemFailedError` (`Reconstructed`/`Original`) models this. Example-level: no Go example test drives a failing Map item and asserts on the preserved error shape. |
| `map/large-scale`, `high-concurrency-invoke` | Scale/concurrency stress scenarios | — | — | — | Partial | Go's own internal test suite has heavy concurrency stress testing for Map/Parallel (per `docs/remaining-work.md`'s task 17c "map-parallel-go concurrency-stability finding" — `-count=50`, varied `GOMAXPROCS`), but that's example-*test*-suite-level stress testing of the SAME small 3-order/2-branch example, not a large-scale (many items) demonstration example in its own right the way TS's `large-scale` is named to suggest. |
| `map/virtual-context` | "Virtual context" applied to Map iterations | — | — | — | No | Same open question as `run-in-child-context/virtual` above. |
| `parallel/basic` | Fan-out over independent named branches | 1 test: result defined + 3 child operations | `toBeDefined` + `toHaveLength(3)` + `assertEventSignatures` | Both | Yes | `map-parallel-go`'s `TestHandler_PricesAndVerifiesConcurrently` also covers the `Parallel`/`All` half (2 branches, not 3, but structurally the same pattern) — asserts branch-by-name status plus the aggregated boolean results, a **more specific** assertion than TS's `basic` test (which only checks branch count + that a result exists, not the individual branch values). **Closed this session (docs/remaining-work.md §10 task 28):** `examples/parallel-processing-go` now additionally provides a FOCUSED, standalone `Parallel`/`All` reference with no `Map` in the mix at all — `map-parallel-go`'s own `Parallel` usage is always alongside an unrelated `Map` fan-out, so there was previously no example matching a real `parallel-processing`/`parallel/basic` catalog entry's minimal, single-purpose shape. 4 independent branches (vs. `map-parallel-go`'s 2), plus a genuine branch-failure scenario (`TestHandler_FraudCheckFailsWholeBatch`) asserting `operations.All`'s `Promise.all`-style all-must-succeed semantics and that sibling branches still complete independently. Verified locally (`-race -count=30`, golden files) AND against a real deployed Lambda function (`parallel-processing-go-example`) — both a SUCCESS invoke (4/4 branches succeed, real SHA-256 IDs, correct nesting) and a FAILURE invoke (fraud-check branch fails, its enclosing `PARALLEL_BRANCH` context fails, siblings remain `ContextSucceeded`, the outer `PARALLEL` context fails) were invoked for real, polled to genuine terminal status, and cross-checked against `GetDurableExecutionHistory` and CloudWatch Logs. See task 28's writeup for the full verification detail. |
| `parallel/empty` | Zero-branch Parallel | — | — | — | Yes | `map-parallel-go`'s `TestHandler_EmptyVerificationBranches` — a new `SkipVerifications` event flag drives `operations.All` with a zero-length branch slice (isolated from the pre-existing `TestHandler_EmptyOrderBatch`'s zero-*item* Map case: this test still prices a real order via Map). Asserts the outer `CONTEXT/PARALLEL` operation SUCCEEDS with zero `PARALLEL_BRANCH` children (`GetChildOperations` returns an empty slice) and a dedicated golden file. Real-cloud-verified: `map-parallel-go-example` (version 3) invoked with `{"orderIds":["order-1"],"skipVerifications":true}`, real `GetDurableExecutionHistory` shows `ContextStarted`(PARALLEL) immediately followed by `ContextSucceeded`(PARALLEL) with zero `PARALLEL_BRANCH` events in between. A regression-check re-invoke of the same version with the original 3-order/2-branch payload confirmed `SUCCEEDED`, unaffected. |
| `parallel/min-successful`, `min-successful-with-callback`, `min-successful-with-passing-threshold`, `tolerated-failure-count`, `tolerated-failure-percentage`, `failure-threshold-exceeded-count`, `failure-threshold-exceeded-percentage` | `CompletionConfig` on Parallel (7 variants, incl. one combined with a callback branch) | — | — | — | No | `examples/completion-config-go` (see the `map/min-successful` row above) demonstrates `WithMapCompletionConfig` on `Map`, not `WithParallelCompletionConfig` on `Parallel` specifically — `operations.Parallel`'s identical `CompletionConfig` support (same `batchCompletion` evaluation, per `batch.go`) remains fully unexampled. Still 0 Go coverage for the Parallel-specific variants, including the callback-combined one. |
| `parallel/should-complete` | Presumably "should complete despite N failures within tolerance" | — | — | — | No | Same. |
| `parallel/heterogeneous` | Branches with differing return types | — | — | — | Partial | Go's `operations.All` signature in `map-parallel-go`'s handler uses `[]func(types.DurableContext) (bool, error)` — a homogeneous branch-result-type slice. Go's type system (no variadic heterogeneous tuple support the way TS's array-of-`Promise<any>` allows) makes a direct heterogeneous-branches example harder to express idiomatically; worth flagging as a language-level design question rather than a simple missing-example gap. |
| `parallel/invoke`, `wait`, `error-preservation`, `virtual-context`, `custom-summary-generator` | Parallel combined with Invoke/Wait; error preservation; virtual context; custom result-summary generation | — | — | — | No | None have Go equivalents. `custom-summary-generator` in particular — a hook for customizing how a large `BatchResult` gets summarized above the 256KB auto-summarization threshold (`docs/remaining-work.md` §6's "Large-payload handling" row references this exact mechanism) — has no Go example despite the underlying `BatchResult` auto-summarization behavior being referenced as confirmed/real in that document. |

## Retry (general, cross-cutting)

| TS Example | Feature Demonstrated | Test Scenario(s) | Assertion Style | Local + Cloud Coverage | Go Equivalent? | Go Notes |
|---|---|---|---|---|---|---|
| `step/steps-with-retry`, `with-retry` | Retry strategies/presets on Step | See Step table above | Structured field checks | Both | Partial | Covered above; Go's `retry-go` is close but lacks the polling-with-wait-between-attempts composite scenario. |
| `with-retry/callback`, `with-retry/invoke` | Retry strategy applied around Callback/Invoke specifically (not just Step) | — | — | — | Partial | **`with-retry/invoke` closed this session** — see the Invoke table above for the full writeup (`RetryingInventoryCheckHandler`, a manual caller-written retry loop around `operations.Invoke`, verified both locally and against a real second deployed Lambda function). `with-retry/callback` remains open: Go's retry-strategy configuration surface (`WithStepRetryStrategy`, `WithConditionRetryStrategy`) is Step/WaitForCondition-scoped in the examples; no Go example configures retry behavior for a Callback submitter as its own dedicated demonstration (though the WaitForCallback submitter DOES retry internally per the SDK's composition — just not exampled/tested as a distinct retry-focused scenario). |
| `retry-exhaustion` (top-level, standalone) | Dedicated retry-exhaustion example, separate from step/steps-with-retry | — | — | — | Yes (via error-handling-go / retry-go) | Go's `retry-go/handler_test.go`'s `TestHandler_ExhaustsRetries` and `error-handling-go/handler_test.go`'s `TestHandler_ChargeExhaustsRetries` both cover this distinctly, with `Attempt`-count field assertions. Reasonable coverage, though split across two examples rather than one dedicated one. |

## Error handling / determinism

| TS Example | Feature Demonstrated | Test Scenario(s) | Assertion Style | Local + Cloud Coverage | Go Equivalent? | Go Notes |
|---|---|---|---|---|---|---|
| `handler-error` | Handler itself throws before any operation runs | 1 test: exact structured error equality (`errorMessage`, `errorType: "Error"`, `stackTrace: undefined`) **and** `expect(result.getOperations()).toHaveLength(0)` (proves NO operations were checkpointed before the failure) | Structured error-object equality + operation-count check + `assertEventSignatures` | Both | Yes | **Closed this session.** `examples/simple-step-go`'s new `ValidatingHandler` (`handler.go`) - a deliberately separate handler value from the pre-existing `handler`, kept unchanged (see that function's own doc comment for the reasoning: it's this repo's most-referenced minimal example, and bolting a validation branch onto it in place would have changed what its own existing, widely-referenced test demonstrates) - returns a genuine Go error (`errMissingMessage`) immediately when `Message == ""`, before `operations.Step` is ever called. `TestHandler_FailsValidationBeforeAnyOperation` (`handler_test.go`) asserts: the whole execution FAILS; and - the TS example's own core assertion, matched in substance - zero NON-EXECUTION operations were ever checkpointed (`result.GetOperations()`, filtered to exclude the root EXECUTION operation the runner always seeds before the handler starts - a real, narrow API-shape divergence from the TS SDK's own `getOperations()`, which excludes the analogous root operation by convention; documented in the test itself, since this Go SDK's `TestResult.GetOperations()` doesn't yet make that exclusion explicit the way `testing.EventSignatures` already does). A trivial (empty-array) `AssertEventSignatures` golden file was still committed for consistency with every other terminal-state test in this repo, even though there is nothing to diff beyond emptiness for a zero-operation scenario - it retains real regression value (a future change that starts checkpointing something before the validation error would flip it from empty to non-empty). **Also verified against a real deployed Lambda function** (`simple-step-go-example`, redeployed this session specifically to add this scenario - see this document's own deployment-tracking notes for the exact versions/ARNs): a real invoke with an empty `message` field produced a genuine terminal `Status: FAILED` via `GetDurableExecution`, and `GetDurableExecutionHistory` showed only `ExecutionStarted`/`InvocationCompleted`/`ExecutionFailed` bookend events with **zero** operation-level events (no `ContextStarted`/`StepStarted`/etc.) in between - the real-backend confirmation of "zero operations checkpointed," cross-checked against the deployed binary's own CloudWatch logs showing `DURABLE_OUTPUT ... {"Status":"FAILED","ErrorMessage":"validation failed: \"message\" field is required and must be non-empty"}`. A control invocation with valid input against the same deployed version confirmed the pre-existing happy-path Step behavior is unaffected, and additionally confirmed `simple-step-go-example` had been running a STALE image from before this repo's ParentID-population fix (`e2b542d`) and SHA-256 operation-ID-hashing fix (`8be9ff8`) - the function's `LastModified` (`2026-07-16T21:21:37Z`) predated both commits (`2026-07-17T19:07:18Z`/`2026-07-17T20:56:05Z` UTC) by about a day; the redeployed version's control-invocation history now shows a real 64-character lowercase-hex SHA-256 `Id` for the `create-greeting` step, confirming the hashing fix is now live for this function too (this handler is a single flat, root-level `Step` with no nesting, so there is no `ParentId` to observe either way - its absence here is expected, not a regression of the ParentID fix, which only ever applies to an operation with an enclosing parent context). |
| `error-determinism` (top-level) | General error-determinism across replay | — | — | — | Partial | Go's `error-handling-go/nondeterministic_test.go` is the closer, more specific analog to `step/step-error-determinism`; the top-level `error-determinism` TS example's exact scope wasn't read this session, so this mapping is tentative. |
| `map/error-type-preservation-replay` | Error type preserved specifically across a replay (not just within one invocation) | — | — | — | Partial | Go's `TestNonDeterministicReplay_RedeployedHandlerChangesOperationType` (in `error-handling-go`) demonstrates the broader "replay + a checkpointed operation log from before" mechanics, but is testing operation-TYPE mismatch detection, not specifically error-type preservation through a Map item's replay. Different (related) concern. |
| `context-validation`, `no-replay-execution`, `child-operations-invalid-depth`, `child-operations-preservation` | Various SDK-internal validation/edge-case scenarios | — | — | — | No | No Go equivalents; these look like TS-SDK-internal-robustness examples (validating context nesting depth limits, behavior when replay is disabled, etc.) rather than durable-execution-feature demonstrations per se. |

## Logging

| TS Example | Feature Demonstrated | Test Scenario(s) | Assertion Style | Local + Cloud Coverage | Go Equivalent? | Go Notes |
|---|---|---|---|---|---|---|
| `logger-test/after-callback`, `after-wait` | Log suppression/emission specifically around callback and wait suspension points | — | — | — | Partial | Go's `logging-go/loggingconfig_test.go` tests suppression around **Step** boundaries specifically (two sequential steps, full replay-skip), not around a Callback or Wait suspension point. `docs/remaining-work.md` §5 task 13 confirms the suppression mechanism is operation-agnostic at the SDK level (`dcontext.replayState` is shared across all context types), so the underlying capability likely already works correctly for Wait/Callback too — but no Go example test proves it for those operation types specifically. |
| `logger-test/log-levels` | Distinguishing Debug/Info/Warn/Error suppression behavior per level | — | — | — | No | Go's `recordingLogger` test double (`logging-go`) only records message strings via a generic `record()` call from all 4 level methods (`Debug`/`Info`/`Warn`/`Error`), and no Go test distinguishes suppression behavior BY level — suppression in the Go SDK design is a single frontier-crossing flag applied uniformly regardless of level, so this may not even be a meaningful distinction to test (worth confirming against the TS SDK's actual semantics before treating this as a real gap vs. a non-issue). |
| `logger-test/powertools-logger`, `simple-powertools-logger` | Integration with AWS Lambda Powertools' logger | — | — | — | No | No Go equivalent; Powertools for Go exists as a separate ecosystem library, and no Go example demonstrates wiring a `types.Logger` adapter to it. Reasonable, low-priority gap given Powertools-for-Go's smaller adoption relative to the TS ecosystem. |

## Serialization / config

| TS Example | Feature Demonstrated | Test Scenario(s) | Assertion Style | Local + Cloud Coverage | Go Equivalent? | Go Notes |
|---|---|---|---|---|---|---|
| `serde` (top-level) | Custom Serdes on a Step result | — | — | — | Yes | `custom-config-go`'s `TestHandler_ChecksSnapshotUsesCustomSerdes` — asserts on the RAW checkpointed wire payload string to prove the custom Serdes genuinely ran (not just that the Go value round-trips), a rigorous assertion style. Direct match in spirit. |
| `wait-for-callback/serdes`, `run-in-child-context/serdes`/`serdes-virtual`/`serdes-large-payload` | Custom Serdes scoped to Callback/ChildContext payloads specifically, incl. large-payload overflow handling | — | — | — | Partial | **Closed this session for ChildContext (gap 8/8)** — see the RunInChildContext table above for the full writeup: `run-in-child-context-go`'s new `UseCustomChildSerdes:true` scenario applies `screamingSnakeCaseSerdes` (the SAME implementation `custom-config-go` uses at Step scope) to a `RunInChildContext` call via `WithChildSerdes`, verified locally (raw-payload assertion, golden file) and against a real deployed Lambda function (CloudWatch-confirmed SCREAMING_SNAKE_CASE checkpointed payload). `wait-for-callback/serdes` (custom Serdes on a Callback payload) remains open — no Go example applies a custom Serdes to a `CreateCallback`/`WaitForCallback` payload. `serdes-large-payload` also remains open — no Go example demonstrates the large-payload/overflow Serdes pattern `docs/remaining-work.md` §6 task 16 discusses (the `FileSystemSerDes`/`OVERFLOW` mode mentioned there is explicitly out of scope for that task and remains unbuilt in Go). |
| `undefined-results` | Handling of `undefined`/no-op step results | — | — | — | No (N/A?) | Go has no direct `undefined` equivalent (Go's type system requires an explicit zero value), so this may not be a meaningful gap so much as a language-semantics difference — worth a quick confirmation rather than treating as a straightforward missing example. |
| `large-payload` (top-level) | Large payload handling at the top level (not scoped to a specific operation) | — | — | — | Yes | **Closed this session.** Corresponds to the real SDK-level gap `docs/remaining-work.md` §6 task 16 already documents in detail (the claimed automatic backend offload mechanism doesn't exist; Go implemented a conservative client-side size-limit rejection instead - `operations.ResultTooLargeError`). `examples/large-payload-go` now demonstrates that conservative behavior directly: a Step whose result exceeds the 750KB threshold fails clearly with `*operations.ResultTooLargeError` (asserted via the handler's own `errors.As`-based inspection, matching this repo's established convention), and a second Step demonstrates the RECOMMENDED alternative - staging the same-sized data externally (simulated via an in-memory map standing in for real S3/DynamoDB - explicitly documented as simulated, not proof of real external-storage integration) and returning a small reference instead, which succeeds normally. **This demonstrates the CONSERVATIVE client-side-rejection pattern this SDK actually has - it does NOT demonstrate, and does not claim to demonstrate, the automatic-offload mechanism the JS/Python reference SDKs document (`FileSystemSerDes`/`OVERFLOW`), which remains unbuilt in Go and is explicitly out of scope for this example.** Verified against a real deployed Lambda function (`large-payload-go-example`): both a cold and a warm invocation of the oversized-result path were polled to genuine terminal `FAILED` status via `GetDurableExecution`, with `GetDurableExecutionHistory` showing ZERO operation-level events for either (confirming no checkpoint carrying the step - let alone the 800KB payload - was ever sent), and CloudWatch Logs' own `platform.report` duration metric confirming the rejection completes in ~95ms on a warm container - over 10x faster than the reference-pattern invocation's real checkpoint round-trip (~1000ms) - direct, measured evidence the rejection happens client-side, BEFORE any network call, not after a slow round-trip. The reference-pattern path was separately invoked and polled to genuine terminal `SUCCEEDED`, with `GetDurableExecutionHistory` showing `StepSucceeded` and the checkpointed result confirmed (via CloudWatch Logs' `DURABLE_OUTPUT` line) to be the small reference string, not the large payload. See `docs/remaining-work.md` §6 task 16's "Update, later session" note for the full writeup. |
| `force-checkpointing` | Explicit/forced checkpoint flush behavior | — | — | — | No | No Go equivalent concept found in the SDK surface reviewed; would need further investigation into whether Go's checkpoint-batching (`checkpoint.Manager`) exposes anything analogous to a "force checkpoint now" call at all. |

## No Go equivalent at all (pure gaps — TS-only groups)

These top-level TS example groups have **no conceptual counterpart** among any of the 10 Go
examples, and in most cases no equivalent capability was found described in
`docs/remaining-work.md` either:

- `otel` — matches `docs/remaining-work.md`'s own explicit statement that the OTel plugin is
  deprioritized and not planned (§5 task 15) — an intentional, acknowledged gap, not an oversight.
- `promise` — TS's `context.promise.*` (`all`/`allSettled`/`any`/`race`) combinator examples;
  Go's `All`/`AllSettled`/`Any`/`Race` ARE implemented at the SDK level
  (`docs/remaining-work.md` §1's table, task 6), but **no Go example dedicated to demonstrating
  the combinators themselves** exists — `map-parallel-go` uses `operations.All` internally as
  an implementation detail of one handler, not as a named, focused demonstration of the
  combinator family the way TS's `promise` group appears to be.
- `hello-world` — **closed this session as a permanent, explicit "not applicable" decision, not an open gap.** The TS SDK's simplest, most minimal onboarding example (single step, per the main README's own description: "basic patterns like hello-world and steps"). Re-read `examples/simple-step-go/handler.go` fresh (docs/remaining-work.md §10 task 28): its original `handler` function is a single `operations.Step` call producing a greeting — exactly as minimal as a `hello-world` example would be, and re-reading it fresh did not change this assessment. A separate `hello-world-go` module would be a byte-for-byte structural duplicate of `simple-step-go` (same module layout, same single-Step handler shape) with no distinct naming/discoverability value beyond what `simple-step-go` already provides — it would not teach a reader anything `simple-step-go` doesn't already teach, just under a different directory name. **Decision: do not create a `hello-world-go` example.** `simple-step-go` is considered the permanent Go equivalent of this catalog entry.
- `generic-types` (a Java SDK catalog entry, not a TS example group — included here since
  `docs/remaining-work.md` §10 task 23 tracked it alongside the TS-derived entries) —
  **closed this session as a permanent, explicit "not applicable to Go" decision, with a
  clear technical justification, not an open gap.** The Java SDK's `generic-types` example
  exists specifically to demonstrate working around Java's TYPE ERASURE: Java generics are
  checked at compile time but STRIPPED to raw types at runtime, so a generic operation whose
  result type needs to be reconstructed from a serialized payload at runtime (e.g. deserializing
  a checkpointed `List<MyType>`) cannot recover `MyType` from a bare `Class<T>` token alone —
  Java's ecosystem convention for this is a runtime `TypeToken`-style reified-type descriptor,
  and the Java catalog's `generic-types` example demonstrates using one correctly with the SDK's
  checkpointing.

  Go's generics are REIFIED, not erased: they are checked AND monomorphized/specialized at
  compile time, and a generic function's type parameter is fully known at every call site with
  zero runtime ambiguity to resolve. Verified this directly from source this session
  (docs/remaining-work.md §10 task 28), not assumed: every generic operation in
  `pkg/durable/operations` (`Step[T]`, `WithStepSerdes[T]`, `Map[TIn,TOut]`,
  `Parallel[TOut]`/`All[TOut]`/`AllSettled[TOut]`, `WaitForCallback[T]`,
  `WaitForCondition[TState]`, `WithChildSerdes[T]`) is a genuine, compiler-checked Go generic
  function — callers never supply a runtime type descriptor anywhere, and Go's own type
  inference fills in the type parameter from each call's own argument types in every existing
  example. `types.Serdes`'s interface methods are untyped (`Serialize(value any, ...)
  (string, error)` / `Deserialize(pointer string, ...) (any, error)`), but this is NOT Java's
  erasure problem in disguise: a custom `Serdes` implementation (confirmed by reading
  `examples/custom-config-go/handler.go`'s `screamingSnakeCaseSerdes` and
  `examples/large-payload-go/handler.go` in full) never needs to know or branch on its generic
  type parameter `T` at all, because the CALLING generic function (`Step[T]`/`WithStepSerdes[T]`)
  already statically knows `T` from its own type parameter, resolved entirely at compile time,
  before `Serdes.Deserialize` is ever invoked — there is no point in the call chain where a
  runtime type token would be needed to recover type information, because nothing was ever
  erased in the first place. **Decision: do not create a `generic-types-go` example.** No
  genuine Go-specific generics gotcha was found that isn't already demonstrated by
  `custom-config-go`/`large-payload-go`'s existing use of a custom `Serdes` with a generic
  operation.
- `non-durable` — presumably demonstrating what happens when non-durable/non-deterministic code
  is used incorrectly outside a Step. No Go equivalent; would pair naturally with the
  deprioritized lint-analyzer idea in `docs/remaining-work.md` §9 task 19.
- `concurrent`, `multiple-waits` — general concurrent-operation and multiple-sequential-Wait
  demonstrations; no Go equivalents beyond what `map-parallel-go` incidentally covers via Map/
  Parallel concurrency.
- `context-validation`, `no-replay-execution`, `child-operations-invalid-depth`,
  `child-operations-preservation`, `comprehensive-operations` — SDK-internal robustness/
  validation scenarios; no Go equivalents, and likely lower priority than feature-coverage gaps.
- `map-completion-config-issue` — sounds like a regression-test example tied to a specific past
  bug; not applicable to Go without knowing the specific TS issue it guards against.
- `simple-execution` — likely another minimal/introductory example; not distinctly gapped beyond
  what `simple-step-go` already covers.

---

## Summary of the biggest coverage gaps

1. **`CompletionConfig` (Map/Parallel min-successful / tolerated-failure) had zero Go example
   coverage — now partially closed.** `examples/completion-config-go` (added in a later
   session) demonstrates `WithMapCompletionConfig`'s `ToleratedFailureCount` threshold on
   `Map`, with both a tolerated-failures-succeed scenario (verified locally AND against a real
   deployed Lambda function) and a threshold-exceeded-fails scenario (local only). Still
   remaining: `MinSuccessful`/`ToleratedFailurePercentage` on `Map`, and all of `Parallel`'s
   `CompletionConfig` variants (including the callback-combined one) remain unexampled in Go —
   TS's 12 combined variants across `map/*`/`parallel/*` are now down to roughly 1 covered in
   Go, not 0, but the majority of the original gap is still open.
2. **WaitForCallback timeout/heartbeat has zero Go example coverage, and can't be added yet** —
   this isn't an example-writing gap, it's a real SDK-level gap `docs/remaining-work.md` itself
   flags (`WithWaitForCallbackTimeout` accepted but not enforced). TS has 3+ dedicated
   timeout/heartbeat variants; Go can't test what isn't implemented.
3. **Closed this session: a genuine failing/erroring nested operation propagating up
   through `RunInChildContext`, now demonstrated and verified against a real deployed
   Lambda.** The pre-existing `run-in-child-context-go` test (`TestHandler_InvalidAmount`)
   is unchanged and still documents that IT specifically does not exercise a real failure
   path (only a falsy-but-non-error result) — that was a deliberate choice, not something
   fixed by silently repurposing it, since its own doc comment explicitly relies on the
   current behavior. Instead, a new, separately-named test (`TestHandler_PaymentStepFails`)
   and a second failure trigger (a negative `Amount`, making `charge-card` return a genuine
   error) were added alongside it, closing the exact hole TS's
   `run-in-child-context/with-failing-step` fills: `errors.As`-based inspection of
   `*operations.ChildContextFailedError`, assertions that both the nested STEP and the
   ENCLOSING CONTEXT/RUN_IN_CHILD_CONTEXT operation are checkpointed FAILED, a dedicated
   golden-file signature, and a real-cloud invoke against a newly-deployed
   `run-in-child-context-go-example` Lambda function confirming the identical FAILED/
   StepFailed/ContextFailed shape with real SHA-256-hashed operation IDs. See the
   RunInChildContext table above and `docs/remaining-work.md` for the full writeup.
4. **Retry-strategy example scoped to Invoke closed this session; Callback's own remains
   open.** `with-retry/invoke` now has a Go counterpart (`examples/chained-invoke-go`'s
   `RetryingInventoryCheckHandler`, a manual caller-written retry loop around
   `operations.Invoke` — the only kind of "retry scoped to Invoke" the real API supports,
   since `Invoke` itself has no retry-strategy option at all), verified both locally and
   against a real second deployed Lambda function exercising genuine multi-attempt
   failure/retry (see the Invoke table above and `docs/remaining-work.md` §10 task 26 for
   the full writeup). `with-retry/callback` still has no Go counterpart — Go's
   `retry-go` only demonstrates `Step`-level retry strategies, and no example configures
   retry behavior for a Callback submitter as its own dedicated demonstration.
5. **Closed this session: structured-error assertion rigor tightened across every existing
   example test that previously only checked for a non-empty error string.** A thorough grep
   across every `examples/*/handler_test.go`/`examples/*/*_test.go` for the
   `!ok || msg == ""`-shaped pattern found 8 call sites total, not just the one
   (`retry-go`'s exhausted-retry case) this gap's original description named. 3 of the 8 turned
   out to already be followed by a genuinely tighter check further down the same test function
   (`error-handling-go`'s `TestHandler_ChargeExhaustsRetries` and
   `nondeterministic_test.go`'s `TestNonDeterministicReplay_RedeployedHandlerChangesOperationType`,
   plus `run-in-child-context-go`'s `TestHandler_PaymentStepFails`) - these were left unchanged,
   since the loose check there is only a preliminary non-nil guard before a real structured
   assertion, not the test's only error check. The remaining 5 were genuinely loose (no
   follow-up tightening anywhere in the test) and were tightened this session:
   - `chained-invoke-go`'s `TestHandler_RetryingInventoryCheck_ExhaustsRetries` - now asserts the
     exact wrapped `*operations.InvokeFailedError` message format (the handler's "exhausted N
     attempts" wrapper around the last attempt's own `'invoke "name" (id id): <cause>'` text)
     and the last attempt's checkpointed `Operation.GetError().ErrorMessage` directly.
   - `chained-invoke-go`'s `TestHandler_InventoryServiceError` - now asserts the exact
     `*operations.InvokeFailedError` message (`'order order-3: checking inventory: invoke
     "check-inventory" (id id): inventory service unavailable'`) and the checkpointed
     `ChainedInvokeDetails.Error.ErrorMessage` directly, both via `Operation.GetError()`.
   - `completion-config-go`'s `TestHandler_ExceedsToleratedFailureThreshold` - now asserts the
     exact `*operations.BatchFailedError`/`AggregateError` composed message (each tolerated-but-
     exceeded bad record's doubly-nested `batch item "..." (id id): step "import-record" (id
     id): record recX: failed validation` segment, joined by `AggregateError`'s `"; "`
     separator) and the outer MAP CONTEXT operation's checkpointed `ErrorMessage` directly.
   - `wait-for-callback-go`'s `TestHandler_RejectedExpense` - now asserts the exact 3-layer
     composed message (handler wrapper -> `*operations.ChildContextFailedError` ->
     `*operations.CallbackFailedError` -> the manager's literal rejection text) and both the
     CALLBACK's and the enclosing CONTEXT's checkpointed `ErrorMessage` fields directly.
   - `retry-go`'s `TestHandler_ExhaustsRetries` (the exact test this gap's original description
     named) - now asserts the exact wrapped `*operations.StepFailedError` message format and the
     checkpointed step's `Operation.GetError().ErrorMessage` directly, in addition to the
     pre-existing `StepDetails.Attempt` check.

   Every tightened assertion follows the SAME approach, chosen per the task's own guidance:
   since `TestResult.GetError()` only ever exposes a flattened string (re-verified this session
   by reading `result.go`/`runner.go` in full - `errorMessage *string`, set once from
   `types.DurableExecutionOutput.ErrorMessage`, with no richer accessor), the message is now
   asserted against the EXACT format the relevant structured error type's own `Error()` method
   produces (traced by reading each candidate type's construction site in `invoke.go`/
   `batch.go`/`callback.go`/`step.go`, not guessed), rather than a "contains" substring check -
   AND, wherever the checkpointed `Operation` itself carries the same information more directly
   (`Operation.GetError() *types.ErrorObject`), that structured field is asserted on too as a
   second, stronger, non-string-matching check. No handler runtime behavior was changed, and no
   pre-existing passing assertion was weakened - every existing check in these 5 tests remains,
   with the tightened checks added alongside them. All 5 touched examples' full test suites,
   plus every other example and the root module, were re-verified with `go build`/`go vet`/
   `gofmt -l .`/`go test -race -count=10` (and `-count=20` for the 5 touched examples
   specifically) with no failures.
6. **Closed this session: a Go example now proves "zero operations checkpointed" for a
   pre-operation handler failure, matching TS's `handler-error` `toHaveLength(0)` assertion in
   substance.** `examples/simple-step-go`'s new `ValidatingHandler` returns a genuine Go error
   immediately on invalid input, before `operations.Step` is ever called; its test asserts the
   execution FAILS and that zero non-EXECUTION operations were ever checkpointed, then verifies
   the identical shape against a real deployed Lambda function (`GetDurableExecutionHistory`
   showing only `ExecutionStarted`/`InvocationCompleted`/`ExecutionFailed`, no operation-level
   events at all). See the Error handling / determinism table above for the full writeup,
   including a real, narrow API-shape divergence found and documented along the way (this Go
   SDK's `TestResult.GetOperations()` includes the root EXECUTION operation, unlike the TS SDK's
   `getOperations()` and this repo's own `testing.EventSignatures`, both of which exclude it).
7. **Closed this session: large-payload / Serdes-overflow handling now has a Go example**,
   demonstrating the conservative client-side `*operations.ResultTooLargeError` rejection
   `docs/remaining-work.md` §6 task 16 implemented (an oversized Step result fails clearly,
   verified fast/client-side against a real deployed Lambda function) plus the recommended
   reference-pattern alternative (stage large data externally - simulated - and return a
   small reference instead). The underlying `FileSystemSerDes`/`OVERFLOW` automatic-offload
   mechanism itself remains unbuilt in Go (TS/Python have it fully documented; Java's guide
   says "Coming soon") - this example demonstrates ONLY the conservative rejection pattern
   Go actually has, not the automatic offload the reference SDKs document, and says so
   explicitly rather than overstating what it proves. See the Serialization / config table
   above and `docs/remaining-work.md` §6 task 16's "Update, later session" note for the full
   writeup, including measured CloudWatch duration evidence (~95ms client-side rejection vs.
   ~1000ms for a real checkpoint network round-trip) confirming the rejection genuinely
   happens before any oversized payload would be sent over the network.
8. **Closed this session: custom Serdes now demonstrated at `RunInChildContext` scope, not just
   `Step`.** `custom-config-go` already demonstrated a custom `Serdes` at `Step` scope
   (`WithStepSerdes`); TS additionally demonstrates it at Callback and ChildContext scope.
   `run-in-child-context-go`'s new `OrderEvent.UseCustomChildSerdes:true` scenario
   (`customSerdesHandlerScenario`) applies the SAME `screamingSnakeCaseSerdes` implementation
   (duplicated verbatim from `custom-config-go`, not reinvented — proving the SAME mechanism
   works at a DIFFERENT scope, not a different serialization scheme) to a `RunInChildContext`
   call's own checkpointed result via `WithChildSerdes`. `TestHandler_ReconciliationUsesCustomChildSerdes`
   asserts on the RAW checkpointed CONTEXT payload string to prove `WithChildSerdes` genuinely
   ran (not just that the deserialized Go value round-trips, which the DEFAULT Serdes would also
   do), and separately confirms the nested Step inside the same child context remains in the
   default format — proving the custom Serdes is scoped to the CONTEXT specifically. Verified
   locally (golden file, `-race -count=20`) and against the real, already-deployed
   `run-in-child-context-go-example` Lambda function (redeployed to version 4): a real invoke
   reached genuine terminal `SUCCEEDED`, `GetDurableExecutionHistory` showed the correct
   `ContextStarted`/`ContextSucceeded` (`RUN_IN_CHILD_CONTEXT`) shape, and — since that API's own
   Result/Payload fields came back `Truncated:true`, matching every prior real-cloud verification
   in this document — CloudWatch Logs confirmed the actual checkpointed content was genuinely
   SCREAMING_SNAKE_CASE (`{"AUDIT_NOTE":"reconciled order recon-cloud-2","RECONCILED_TOTAL":999.01}`),
   not the default JSON format. `wait-for-callback/serdes` (Callback scope) and
   `run-in-child-context/serdes-large-payload` remain open. See the RunInChildContext and
   Serialization/config tables above for the full writeup, and `docs/remaining-work.md`'s
   Suggested-sequencing task 18 for the session-level details.
9. **Closed this session: `parallel/empty` (zero-branch `Parallel`/`All`).** `map-parallel-go`'s
   handler gained a `SkipVerifications` event flag that drives `operations.All` with an
   empty branch slice, exercising `runBatch`'s own `n == 0` early-return path through the
   SAME `batchScheduler` machinery this repo's history has repeatedly found real concurrency
   bugs in — no new bug was found this time; the zero-branch path was already correct.
   `TestHandler_EmptyVerificationBranches` (a real order still prices via Map, isolating the
   Parallel-side zero-branches case from the pre-existing zero-*item* Map case) asserts the
   outer `CONTEXT/PARALLEL` operation SUCCEEDS with zero children and pins a dedicated golden
   file. Verified against a real, already-deployed `map-parallel-go-example` Lambda function
   (redeployed to version 3): a real invoke with `skipVerifications:true` reached genuine
   terminal `SUCCEEDED`, and `GetDurableExecutionHistory` showed `ContextStarted`(PARALLEL)
   immediately followed by `ContextSucceeded`(PARALLEL) with zero `PARALLEL_BRANCH` events in
   between. A regression-check re-invoke of the same version with the original 3-order/2-branch
   payload confirmed the pre-existing happy path is unaffected.

**Final honest assessment of the originally-identified 8 concrete gaps (re-checked fresh, not
assumed):** items 3, 4 (Invoke half only — Callback's own retry-strategy gap remains open), 5,
6, 7, 8, and the separately-tracked `parallel/empty` gap (9, above) are now genuinely, fully
closed with both local and real-cloud verification. Item 1 (`CompletionConfig`) is **only
partially closed** — `Map`'s `ToleratedFailureCount` has an example now, but `Map`'s
`MinSuccessful`/`ToleratedFailurePercentage` and ALL of `Parallel`'s `CompletionConfig` variants
remain unexampled; this was never claimed as fully closed and still is not. Item 2
(`WaitForCallback` timeout/heartbeat) is correctly identified as blocked on a real, separate
SDK-level gap (`WithWaitForCallbackTimeout` accepted but unenforced) — no Go example can close
this until that SDK gap itself is closed first; it is not an example-writing task. So: 7 of 9
tracked items are fully closed, 1 is partially closed (by design, not oversight — the remaining
`CompletionConfig` variants are additional, lower-priority examples beyond this round's original
scope), and 1 is correctly blocked on upstream SDK work rather than something an example could
ever close on its own.

## Session update: 9 new examples added (2026-07-19), re-derived against the JS monorepo's own examples package

This session re-derived the comparison from scratch by reading
`packages/aws-durable-execution-sdk-js-examples/src/examples/` directly in a local checkout of
`/tmp/aws-durable-execution-sdk-js-experiment` (the actual monorepo the public
`aws/aws-durable-execution-sdk-js` repo's examples package lives in), rather than the GitHub
API/raw-content reads the original version of this document used - confirmed the same ~107
distinct example scenarios across ~29 top-level categories the original count implied. Go
example count going into this session: 14. Going out: **23**.

**New examples added this session** (all verified build/vet/gofmt clean, `go test ./... -race
-count=5` passing reliably, full repo-wide regression check clean after each commit):

- `create-callback-go` — `operations.CreateCallback` had ZERO prior example coverage despite
  being a distinct, documented operation (`WaitForCallback` only demonstrates the higher-level
  wrapper). Scenario: create a callback, hand its ID to an external system via a `Step`, then -
  WHILE that callback is still outstanding - run a second, unrelated `Step`, only then
  `AwaitCallback`. Demonstrates `CreateCallback`'s own documented reason to exist over
  `WaitForCallback` ("interleave callback registration with other durable operations before
  suspending").
- `wait-go` — `operations.Wait` had NO dedicated standalone example at all (only appeared
  incidentally inside `retry-go`/`wait-for-condition-go`). Four handlers: basic, event-configurable
  duration, custom name, and an "unawaited" fire-and-forget variant (included for JS-example
  parity with an explicit doc-comment caveat that Go's `Wait` is a plain synchronous call, not a
  JS Promise, so this is NOT a recommended production pattern).
- `promise-combinators-go` — `operations.All`/`AllSettled`/`Any`/`Race` (all built on `Parallel`)
  had ZERO example coverage. Four handlers via a multi-vendor price-check scenario. **Found a
  real test-design mistake while building this** (not an SDK bug, but a genuine correctness trap
  worth recording): initially added `WithParallelMaxConcurrency(1)` to the `AllSettled`/`Any`
  handlers purely for "deterministic golden files" - but `batch.go`'s own documented
  `MinSuccessful: 0`/`1` early-exit semantics (`thresholdExceeded` satisfied the instant the
  FIRST branch finishes at all) mean bounded concurrency can silently skip later branches via
  `errBatchSkipped` before they ever run, for these two operations specifically. Reverted to
  unbounded concurrency for `AllSettledHandler`/`AnyHandler` (their real, correct semantics) and
  switched those two tests to assert on the aggregated RESULT rather than a golden
  event-signature file, since branch completion order under unbounded concurrency is genuinely
  non-deterministic; `AllHandler`/`RaceHandler` safely keep `MaxConcurrency(1)` since neither
  uses a `MinSuccessful` threshold.
- `wait-for-callback-failing-submitter-go`, `wait-for-callback-timeout-go`,
  `wait-for-callback-heartbeat-go`, `wait-for-callback-nested-go`,
  `wait-for-callback-submitter-retry-success-go`, `wait-for-callback-serdes-go` — six more
  `WaitForCallback` edge-case examples (submitter retry exhaustion, timeout, heartbeat timeout,
  composition with `RunInChildContext`, submitter retry that recovers, and a custom result
  serdes). `wait-for-callback-nested-go` deliberately consolidates the JS reference SDK's own
  separate `child-context` and `nested` examples into one Go example, since both exercise the
  identical composition (`WaitForCallback` inside `RunInChildContext`) at different nesting
  depths with no distinct Go-SDK mechanism the extra JS nesting level would exercise.

**Two more real, honest gaps found and explicitly documented (not worked around or faked) while
building the `WaitForCallback` cluster** - both are genuine limitations in THIS Go SDK's own
`testing` package, confirmed by reading `operation.go`/`runner.go`/`callback.go` directly, not
assumptions:

1. **No way to simulate a backend-driven callback timeout or heartbeat locally.**
   `testing.Operation` only exposes `SendCallbackSuccess`/`SendCallbackFailure`, both of which
   set `OperationStatusFailed` - a REAL timeout sets `OperationStatusTimedOut`, which nothing in
   `LocalTestRunner`'s fake in-memory client can produce. `types.Operation` also carries no
   `CallbackOptions` field on the read side, so a checkpointed `HeartbeatTimeoutSeconds` value
   can't be independently inspected after the fact either. Both `wait-for-callback-timeout-go`
   and `wait-for-callback-heartbeat-go` document this explicitly in their own `handler_test.go`
   top-level doc comments and test only the genuinely-reachable adjacent scenario instead (an
   explicit `SendCallbackFailure` for the timeout example, confirming `Timeout=false` for it - a
   real, distinct code path also reachable through the same handler).
2. **No way to inject a raw, pre-serialized callback result string.**
   `testing.Operation.SendCallbackSuccess(result any)` always JSON-marshals its own argument -
   there is no sibling that accepts an already-serialized wire-format string the way a genuinely
   external system using a custom `Serdes.Serialize` would actually send one.
   `wait-for-callback-serdes-go` works around this HONESTLY (documented in its own test's
   top-level doc comment): it passes `SendCallbackSuccess` a Go value whose OWN JSON struct tags
   already spell the exact custom wire shape the real serdes would have produced, so plain
   `encoding/json` marshaling that value produces byte-for-byte the same string the custom
   `Serialize` would - this still genuinely exercises the real `Deserialize` logic being tested,
   without needing `testing.Operation` to support raw-string injection.

**Also confirmed as a real, non-fabricatable gap and deliberately NOT ported this session**: the
JS reference SDK's own `context-validation/*` example family (3 examples: parent context misused
inside a child/step/wait-condition closure). Confirmed via direct reading of
`durable-context.ts` that the JS SDK's own guard is built on a global "active context" identity
tracker with no equivalent in this Go SDK at all (`types.DurableContext` is passed explicitly as
a function parameter here, with no global tracking to violate) - there is nothing this Go SDK
would currently do differently if a handler reused a stale outer context inside a child, so
writing an example titled "demonstrates the error" would be actively misleading without first
characterizing what (if anything) actually happens, which is its own separate investigation, not
an example-porting task.

**Remaining, not-yet-investigated scope** (identified but genuinely not reached this session -
each needs the same "confirm it maps to something real first" treatment applied above, not a
mechanical port): `wait-for-callback/anonymous`/`mixed-ops`/`multiple-invocations`/
`quick-completion`/`submitter-failure-catchable` (likely mostly duplicates of examples already
built or already-demonstrated compositions - worth a confirmation pass, not fresh building);
`with-retry/callback`/`with-retry/invoke` (an end-to-end `withRetry` helper - unconfirmed whether
this Go SDK has an equivalent); Map/Parallel's own `virtual-context` FLAT-nesting examples (the
underlying `NestingModeFlat` SDK feature was implemented and tested in a separate, earlier part
of this same overall session - see `docs/remaining-work.md` §16 - but has no dedicated
`examples/` directory yet, a pure documentation gap and a good low-risk next candidate);
`force-checkpointing/*` (4 variants, unconfirmed against this Go SDK's own force-checkpoint
polling behavior); the `logger-test/*` powertools-logger variants (JS-ecosystem-specific,
possibly no meaningful Go port); `serde/basic`/`configure-serdes` (JS-specific
`createClassSerdes`/`configureSerdes` ergonomic helpers, unconfirmed whether this Go SDK has or
needs an equivalent); and the remaining un-investigated singletons
(`comprehensive-operations`, `undefined-results`, `no-replay-execution`, `non-durable`,
`block-example`, `multiple-waits`).

## Session close-out: final 3 examples added, remaining gaps confirmed and closed out

Went back through the "not yet investigated" list from the section above and resolved every
remaining item - either by porting a genuinely new one, or by confirming (not assuming) it's
either a true duplicate of an existing example or a real, non-fabricatable gap. Go example count:
23 -> **26**.

**3 more examples added:**
- `wait-for-callback-multiple-invocations-go` - proves checkpoint/replay tracking correctly
  resumes across MANY independent suspend/resume cycles within one handler (2 `Wait`s, 2
  `WaitForCallback`s, and a `Step`, interleaved); a 3-invocation test drives both callback
  resolutions separately.
- `map-virtual-context-go`, `parallel-virtual-context-go` - the FLAT-nesting SDK feature
  (`NestingModeFlat`) implemented and tested earlier in this same overall session
  (`docs/remaining-work.md` §16) had zero dedicated example coverage - a pure documentation gap,
  now closed. Both tests directly confirm the real checkpoint-count reduction (zero per-iteration/
  per-branch `CONTEXT` operations, correct `ParentID` linkage to the outer context).

**Confirmed as genuine, non-fabricatable gaps and explicitly declined:**
- `run-in-child-context/virtual` - confirmed via direct source reading that this Go SDK's
  `RunInChildContext` has no nesting/virtual option at all; FLAT nesting was only ever
  implemented for `Map`/`Parallel` in this Go SDK.
- `force-checkpointing/*` (4 variants) - confirmed the underlying JS example mechanism is a
  backend-level "force checkpoint polling" behavior (a long-running branch alongside another
  branch's own independent retries), not a distinct SDK API surface at all - it's just
  `Parallel` with ordinary branches, and `testing.LocalTestRunner` has no concept of "force
  checkpoint polling" as a distinct simulatable behavior, so a Go port would add no new
  assertion value beyond what `parallel-processing-go`/`retry-go` already demonstrate.
- `undefined-results` - JS-specific `undefined`-vs-missing-field replay semantics with no
  meaningful Go equivalent (Go's zero-value struct fields always serialize; there's no
  "returned undefined" ambiguity the same way).
- `non-durable` - tests a plain, non-SDK Lambda handler (`durableConfig: null` in the JS
  example's own config) - a baseline/control case for the JS test harness itself, not something
  that exercises this Go SDK at all.

**Confirmed as true duplicates of already-existing examples and correctly skipped** (verified by
reading each JS source file, not assumed from its description alone): `multiple-waits`
(`wait-go` already covers sequential `Wait` mechanics), `block-example` (`run-in-child-context-go`
already covers nested `RunInChildContext`), `wait-for-callback/quick-completion` and
`wait-for-callback/anonymous` (both functionally identical to `wait-for-callback-go`'s own basic
case - an inline-closure style difference only, not a distinct mechanism), `with-retry/invoke`'s
target function (functionally identical to `chained-invoke-go` + `retry-go`'s already-existing
patterns combined), `handler-error` and `retry-exhaustion` (confirmed via direct grep already
covered by `error-handling-go`/`retry-go` respectively).

**Remaining, still-unconfirmed scope, deliberately left for a future pass** (lower priority, not
reached this session): `comprehensive-operations` (a broad, multi-operation showcase - likely
mostly redundant with the sum of every other example, but not individually confirmed);
`serde/basic`/`configure-serdes` (JS-specific `createClassSerdes`/`configureSerdes` ergonomic
helpers - unconfirmed whether this Go SDK has or needs an equivalent); the `logger-test/*`
powertools-logger variants (JS-ecosystem-specific, possibly no meaningful Go port);
`wait-for-callback/mixed-ops` and `submitter-failure-catchable` (likely, but not yet individually
confirmed, redundant with existing examples the way `quick-completion`/`anonymous` turned out to
be).

## Notes on TS assertion rigor observed (for calibration)

Two useful, generalizable observations from reading the actual TS test bodies, worth keeping in
mind when writing future Go examples:

- TS is not perfectly consistent about calling `assertEventSignatures` in *every* test within a
  multi-test file — `map/basic`'s first test (`"should run correct number of durable steps"`)
  does not call it, only the second one does, despite `ADDING_EXAMPLES.md` stating it's required
  in every test. The Go repo's own convention (calling `AssertEventSignatures` in every test
  function that reaches a terminal state) is actually stricter/more consistent than the TS
  source it was modeled on in this specific respect.
- TS's structured-error assertions consistently include `stackTrace: undefined` as an explicit
  expected field (not just omitted) — a minor but real convention difference; Go's error
  comparisons don't have a stack-trace-shaped field to compare at all given the language's
  different error-representation model, so this isn't a portable gap, just a noted difference.
- **Closed this session (gap 5/8):** the specific looseness this section originally flagged —
  several Go example tests asserting only a non-empty error string on a failure path — has been
  tightened across every genuinely loose instance found (5 of 8 grep hits for the
  `!ok || msg == ""` pattern; the other 3 were already followed by a real structured check
  further down the same test and were left alone). Since `TestResult.GetError()` has no
  TS-equivalent structured-object accessor (it flattens to a plain string — confirmed by reading
  `result.go`/`runner.go` in full), Go's version of TS's `toEqual({errorMessage, errorType,
  stackTrace})` exact-object-equality idiom is necessarily two string/field comparisons instead
  of one object comparison: (1) the flattened message asserted against the EXACT format the
  relevant structured error type's own `Error()` method produces (not a substring), and (2),
  wherever available, the checkpointed `Operation.GetError() *types.ErrorObject`'s own
  `ErrorMessage` field asserted directly as a second, stronger, non-string-matching check. See
  this document's gap-5 entry above for the full list of the 5 tests tightened
  (`chained-invoke-go` x2, `completion-config-go`, `wait-for-callback-go`, `retry-go`) and
  exactly what each now asserts.
