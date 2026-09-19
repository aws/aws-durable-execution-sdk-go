# AWS Lambda Durable Execution SDK for Go — Examples

Deployable example workflows demonstrating the AWS Lambda Durable Execution SDK for Go.

## Prerequisites

- Go 1.24+
- AWS SAM CLI
- AWS credentials configured for a target account and region

## Examples

### Step & Retry

| Example | Operations Demonstrated | Expected Terminal State |
|---------|------------------------|------------------------|
| [simple-step](simple-step/main.go) | Step | SUCCEEDED |
| [named-step](named-step/main.go) | Step (named) | SUCCEEDED |
| [step-with-retry](step-with-retry/main.go) | Step, RetryStrategy | SUCCEEDED |
| [steps-with-retry](steps-with-retry/main.go) | Step (multiple), RetryStrategy | SUCCEEDED |
| [attempt-fallback](attempt-fallback/main.go) | Step (sequential fallback) | SUCCEEDED |
| [interrupted-no-retry](interrupted-no-retry/main.go) | Step, AtMostOnce semantics | SUCCEEDED |
| [step-error-determinism](step-error-determinism/main.go) | Step, error replay determinism | SUCCEEDED |
| [retry-exhaustion](retry-exhaustion/main.go) | Step, RetryStrategy exhaustion | FAILED |
| [retry-linear](retry-linear/main.go) | Step, MustLinearBackoff, LinearBackoff (delays 1 s, 2 s, 3 s) | SUCCEEDED |
| [retry-invoke](retry-invoke/main.go) | Invoke, retry loop | SUCCEEDED |
| [retry-invoke-target](retry-invoke-target/main.go) | (target function for retry-invoke) | — |
| [retry-callback](retry-callback/main.go) | WaitForCallback, retry loop | FAILED |

### Wait & WaitForCondition

| Example | Operations Demonstrated | Expected Terminal State |
|---------|------------------------|------------------------|
| [wait-basic](wait-basic/main.go) | Wait | SUCCEEDED |
| [wait-named](wait-named/main.go) | Wait (named) | SUCCEEDED |
| [wait-configurable](wait-configurable/main.go) | Wait (input-driven duration) | SUCCEEDED |
| [wait-unawaited](wait-unawaited/main.go) | Wait, Step (fire-and-forget) | SUCCEEDED |
| [wait-for-condition](wait-for-condition/main.go) | WaitForCondition | SUCCEEDED |
| [multiple-waits](multiple-waits/main.go) | Wait (multiple sequential) | SUCCEEDED |

### Invoke & Child Context

| Example | Operations Demonstrated | Expected Terminal State |
|---------|------------------------|------------------------|
| [invoke-simple](invoke-simple/main.go) | Invoke | SUCCEEDED |
| [invoke-simple-target](invoke-simple-target/main.go) | (target function for invoke) | — |
| [invoke-tenant-id](invoke-tenant-id/main.go) | Invoke, WithTenantID | SUCCEEDED |
| [invoke-tenant-target](invoke-tenant-target/main.go) | (target function for invoke-tenant-id) | — |
| [chained-invoke](chained-invoke/main.go) | Invoke (sequential chain) | SUCCEEDED |
| [child-context-basic](child-context-basic/main.go) | RunInChildContext | SUCCEEDED |
| [child-context-virtual](child-context-virtual/main.go) | RunInChildContext (virtual) | SUCCEEDED |
| [child-context-serdes](child-context-serdes/main.go) | RunInChildContext, WithChildSerdes | SUCCEEDED |
| [child-context-serdes-virtual](child-context-serdes-virtual/main.go) | RunInChildContextAsync, WithChildSerdes (virtual) | SUCCEEDED |
| [child-context-serdes-large-payload](child-context-serdes-large-payload/main.go) | RunInChildContext, FileSystemSerdes (>256KB) | SUCCEEDED |
| [child-context-large-data](child-context-large-data/main.go) | RunInChildContext (ReplayChildren) | SUCCEEDED |
| [child-context-error-propagation](child-context-error-propagation/main.go) | RunInChildContext, error propagation | SUCCEEDED |
| [child-context-error-data-propagation](child-context-error-data-propagation/main.go) | RunInChildContext, WithErrorData propagation | SUCCEEDED |
| [child-context-failing-step](child-context-failing-step/main.go) | RunInChildContext, Step failure | SUCCEEDED |
| [child-context-checkpoint-size-limit](child-context-checkpoint-size-limit/main.go) | RunInChildContext, size limit | SUCCEEDED |
| [child-context-nested-blocks](child-context-nested-blocks/main.go) | RunInChildContext, nested parent/child/grandchild | SUCCEEDED |
| [block-example](block-example/main.go) | RunInChildContext (nested), Step, Wait, struct result | SUCCEEDED |
| [child-ops-preservation](child-ops-preservation/main.go) | RunInChildContext, operation ordering | SUCCEEDED |
| [child-ops-invalid-depth](child-ops-invalid-depth/main.go) | RunInChildContext, depth validation | FAILED |

### Callbacks

| Example | Operations Demonstrated | Expected Terminal State |
|---------|------------------------|------------------------|
| [wait-callback-basic](wait-callback-basic/main.go) | WaitForCallback | SUCCEEDED |
| [wait-callback-anonymous](wait-callback-anonymous/main.go) | WaitForCallback (unnamed) | SUCCEEDED |
| [wait-callback-timeout](wait-callback-timeout/main.go) | WaitForCallback, timeout | SUCCEEDED |
| [wait-callback-heartbeat](wait-callback-heartbeat/main.go) | WaitForCallback, heartbeat | SUCCEEDED |
| [wait-callback-failures](wait-callback-failures/main.go) | WaitForCallback, submitter failure handling | SUCCEEDED |
| [wait-callback-serdes](wait-callback-serdes/main.go) | WaitForCallback, WithCallbackSerdes | SUCCEEDED |
| [wait-callback-mixed-ops](wait-callback-mixed-ops/main.go) | WaitForCallback, Step, Wait | SUCCEEDED |
| [wait-callback-nested](wait-callback-nested/main.go) | WaitForCallback (nested) | SUCCEEDED |
| [wait-callback-child-context](wait-callback-child-context/main.go) | WaitForCallback, RunInChildContext | SUCCEEDED |
| [wait-callback-multiple-invocations](wait-callback-multiple-invocations/main.go) | WaitForCallback (multiple) | SUCCEEDED |
| [wait-callback-quick-completion](wait-callback-quick-completion/main.go) | WaitForCallback, immediate completion | SUCCEEDED |
| [wait-callback-submitter-failure](wait-callback-submitter-failure/main.go) | WaitForCallback, submitter error | SUCCEEDED |
| [wait-callback-submitter-retry](wait-callback-submitter-retry/main.go) | WaitForCallback, WithSubmitterRetry | SUCCEEDED |
| [wait-callback-error-instance-failure](wait-callback-error-instance-failure/main.go) | WaitForCallback, CallbackError | SUCCEEDED |
| [wait-callback-error-instance-submitter](wait-callback-error-instance-submitter/main.go) | WaitForCallback, submitter CallbackError | SUCCEEDED |
| [wait-callback-error-instance-timeout](wait-callback-error-instance-timeout/main.go) | WaitForCallback, ErrCallbackTimedOut | SUCCEEDED |
| [create-callback-simple](create-callback-simple/main.go) | CreateCallback | SUCCEEDED |
| [create-callback-timeout](create-callback-timeout/main.go) | CreateCallback, timeout | SUCCEEDED |
| [create-callback-heartbeat](create-callback-heartbeat/main.go) | CreateCallback, heartbeat | SUCCEEDED |
| [create-callback-concurrent](create-callback-concurrent/main.go) | CreateCallback (multiple concurrent) | SUCCEEDED |
| [create-callback-failures](create-callback-failures/main.go) | CreateCallback, failure handling | SUCCEEDED |
| [create-callback-serdes](create-callback-serdes/main.go) | CreateCallback, WithCallbackSerdes | SUCCEEDED |
| [create-callback-mixed-ops](create-callback-mixed-ops/main.go) | CreateCallback, Step, Wait | SUCCEEDED |
| [create-callback-error-instance](create-callback-error-instance/main.go) | CreateCallback, CallbackError | SUCCEEDED |
| [callback-sender](callback-sender/main.go) | (companion submitter function) | — |

### Map & Parallel

| Example | Operations Demonstrated | Expected Terminal State |
|---------|------------------------|------------------------|
| [map-basic](map-basic/main.go) | Map, WithMaxConcurrency | SUCCEEDED |
| [map-empty](map-empty/main.go) | Map (empty input) | SUCCEEDED |
| [map-large-scale](map-large-scale/main.go) | Map (100 items) | SUCCEEDED |
| [map-error-preservation](map-error-preservation/main.go) | Map, error ordering | SUCCEEDED |
| [map-min-successful](map-min-successful/main.go) | Map, WithCompletion (min successful) | SUCCEEDED |
| [map-tolerated-failure-count](map-tolerated-failure-count/main.go) | Map, ToleratedFailureCount | SUCCEEDED |
| [map-failure-threshold](map-failure-threshold/main.go) | Map, fail-fast threshold | SUCCEEDED |
| [map-failure-threshold-percentage](map-failure-threshold-percentage/main.go) | Map, ToleratedFailurePercentage (failure propagated) | FAILED |
| [map-high-concurrency-invoke](map-high-concurrency-invoke/main.go) | Map, Invoke, WithMaxConcurrency | SUCCEEDED |
| [map-tolerated-failure-percentage](map-tolerated-failure-percentage/main.go) | Map, WithToleratedFailurePercentage | SUCCEEDED |
| [map-completion-config-issue](map-completion-config-issue/main.go) | Map, early-completion partial results | SUCCEEDED |
| [map-virtual-context](map-virtual-context/main.go) | Map, WithNesting (NestingFlat) | SUCCEEDED |
| [map-custom-summary-generator-replay](map-custom-summary-generator-replay/main.go) | Map, WithBatchSummary (>256KB, replay across suspension) | SUCCEEDED |
| [map-flat-summarized-replay](map-flat-summarized-replay/main.go) | Map, WithNesting (NestingFlat), >256KB replay across suspension | SUCCEEDED |
| [parallel-basic](parallel-basic/main.go) | Parallel, WithMaxConcurrency | SUCCEEDED |
| [parallel-empty](parallel-empty/main.go) | Parallel (no branches) | SUCCEEDED |
| [parallel-invoke](parallel-invoke/main.go) | Parallel, Invoke | SUCCEEDED |
| [parallel-wait](parallel-wait/main.go) | Parallel, Wait | SUCCEEDED |
| [parallel-error-preservation](parallel-error-preservation/main.go) | Parallel, error ordering | SUCCEEDED |
| [parallel-min-successful](parallel-min-successful/main.go) | Parallel, WithCompletion (min successful) | SUCCEEDED |
| [parallel-min-successful-callback](parallel-min-successful-callback/main.go) | Parallel, MinSuccessful, WaitForCallback | SUCCEEDED |
| [parallel-min-successful-threshold](parallel-min-successful-threshold/main.go) | Parallel, MinSuccessful reached by the two fastest of five staggered branches | SUCCEEDED |
| [parallel-invalid-max-concurrency](parallel-invalid-max-concurrency/main.go) | Parallel, WithMaxConcurrency validation (zero is rejected) | FAILED |
| [parallel-should-complete](parallel-should-complete/main.go) | Parallel, WithCompletion (ShouldComplete quorum) | SUCCEEDED |
| [parallel-tolerated-failure](parallel-tolerated-failure/main.go) | Parallel, ToleratedFailureCount | SUCCEEDED |
| [parallel-tolerated-failure-percentage](parallel-tolerated-failure-percentage/main.go) | Parallel, WithToleratedFailurePercentage | SUCCEEDED |
| [parallel-failure-threshold-count](parallel-failure-threshold-count/main.go) | Parallel, fail-fast ToleratedFailureCount=0 (failure propagated) | FAILED |
| [parallel-failure-threshold-percentage](parallel-failure-threshold-percentage/main.go) | Parallel, ToleratedFailurePercentage (failure propagated) | FAILED |
| [parallel-virtual-context](parallel-virtual-context/main.go) | Parallel, WithNesting (NestingFlat) | SUCCEEDED |
| [parallel-heterogeneous](parallel-heterogeneous/main.go) | Parallel, Step, Wait, Invoke | SUCCEEDED |
| [parallel-custom-summary-generator](parallel-custom-summary-generator/main.go) | Parallel, WithBatchSummary (>256KB) | SUCCEEDED |

### Future Combinators & Concurrency

| Example | Operations Demonstrated | Expected Terminal State |
|---------|------------------------|------------------------|
| [future-all](future-all/main.go) | Go, All | SUCCEEDED |
| [future-all-settled](future-all-settled/main.go) | Go, AllSettled | SUCCEEDED |
| [future-all-wait](future-all-wait/main.go) | Go, All, WaitAsync | SUCCEEDED |
| [future-any](future-any/main.go) | Go, Any | SUCCEEDED |
| [future-race](future-race/main.go) | Go, Race | SUCCEEDED |
| [future-race-wait](future-race-wait/main.go) | Go, Race, WaitAsync | SUCCEEDED |
| [future-select](future-select/main.go) | Select, branching on the winner's name | SUCCEEDED |
| [future-join](future-join/main.go) | Join, StepAsync, Go, Wait: barrier over futures of different result types | SUCCEEDED |
| [future-combinators-mixed](future-combinators-mixed/main.go) | Go, All, Any, Race | SUCCEEDED |
| [future-replay](future-replay/main.go) | Go, Future replay determinism | SUCCEEDED |
| [future-unhandled-error](future-unhandled-error/main.go) | Go, unhandled Future error | SUCCEEDED |
| [concurrent-wait](concurrent-wait/main.go) | Go, WaitAsync | SUCCEEDED |
| [concurrent-operations](concurrent-operations/main.go) | Go, StepAsync, WaitAsync | SUCCEEDED |
| [concurrent-callback-submitter](concurrent-callback-submitter/main.go) | Go, WaitForCallback | SUCCEEDED |
| [concurrent-callback-wait](concurrent-callback-wait/main.go) | Go, WaitForCallback, WaitAsync | SUCCEEDED |

### Cross-cutting & Observability

| Example | Operations Demonstrated | Expected Terminal State |
|---------|------------------------|------------------------|
| [force-checkpoint-invoke](force-checkpoint-invoke/main.go) | Invoke, force checkpoint | SUCCEEDED |
| [force-checkpoint-wait](force-checkpoint-wait/main.go) | Wait, force checkpoint | SUCCEEDED |
| [force-checkpoint-callback](force-checkpoint-callback/main.go) | WaitForCallback, force checkpoint | SUCCEEDED |
| [force-checkpoint-step-retry](force-checkpoint-step-retry/main.go) | Step, retry, force checkpoint | SUCCEEDED |
| [context-validation-child](context-validation-child/main.go) | RunInChildContext, goroutine validation | FAILED |
| [context-validation-step](context-validation-step/main.go) | Step, goroutine validation | FAILED |
| [context-validation-wait-condition](context-validation-wait-condition/main.go) | WaitForCondition, goroutine validation | FAILED |
| [error-determinism](error-determinism/main.go) | Step, error replay consistency | SUCCEEDED |
| [error-handling-taxonomy](error-handling-taxonomy/main.go) | errors.As, StepError, InvokeError, CallbackError | SUCCEEDED |
| [serde-basic](serde-basic/main.go) | Step, WithStepSerdes, SerdesOf | SUCCEEDED |
| [serde-callback-deserializer](serde-callback-deserializer/main.go) | WithCallbackDeserializer, custom callback deserialization | SUCCEEDED |
| [serde-custom-config](serde-custom-config/main.go) | WithSerdes (handler-level) | SUCCEEDED |
| [serde-circular-references](serde-circular-references/main.go) | Step, SerdesError on a reference cycle, WithStepSerdes (cycle-breaking) | SUCCEEDED |
| [serde-struct-with-times](serde-struct-with-times/main.go) | Step, time.Time round trip, WithStepSerdes (Unix-millisecond wire format) | SUCCEEDED |
| [serde-filesystem](serde-filesystem/main.go) | ConfigureSerdes, NewFileSystemSerdes (FileSystemSerdesModeAlways, FileSystemPathEncodingHash, GeneratePreview with masking) | SUCCEEDED |
| [serde-filesystem-overflow](serde-filesystem-overflow/main.go) | ConfigureSerdes, NewFileSystemSerdes (FileSystemSerdesModeOverflow) | SUCCEEDED |
| [serde-preview-truncation](serde-preview-truncation/main.go) | NewFileSystemSerdes, GeneratePreview, BuildPreview (include-all, exclude, truncation) | SUCCEEDED |
| [serde-preview-field-selection](serde-preview-field-selection/main.go) | NewFileSystemSerdes, GeneratePreview, BuildPreview (exclude-all, path matching, masking) | SUCCEEDED |
| [logger-after-wait](logger-after-wait/main.go) | Wait, Context.Logger, replay suppression | SUCCEEDED |
| [logger-after-callback](logger-after-callback/main.go) | WaitForCallback, Context.Logger | SUCCEEDED |
| [logger-log-levels](logger-log-levels/main.go) | Context.Logger, all log levels | SUCCEEDED |
| [logger-slog-handler](logger-slog-handler/main.go) | WithLogHandler, application slog.Handler | SUCCEEDED |
| [plugin-lifecycle](plugin-lifecycle/main.go) | WithPlugins, hook lifecycle ordering | SUCCEEDED |

A serdes written for one result type can use `durable.SerdesOf`, which
adapts typed marshal and unmarshal functions to the untyped `Serdes`
interface and rejects any other type with a descriptive error.
`serde-basic` shows the pattern with `WithStepSerdes`. A `SerdesOf` serdes
also works handler-wide with `WithSerdes`, but then every operation result
in the handler must be that one type.

A filesystem serdes stores only a file reference in the checkpoint. In
the default `FileSystemSerdesModeAlways` every value is written to a file;
`serde-filesystem` shows it with `FileSystemPathEncodingHash`, which names
each execution's directory by a hash of the execution ARN. In
`FileSystemSerdesModeOverflow` a value small enough for the checkpoint is
stored inline instead, and only a larger value is written to a file;
`serde-filesystem-overflow` shows one of each. The base path must be a
durable, shared mount such as EFS or S3 Files, not Lambda's `/tmp`; each
filesystem example documents why a local directory is nonetheless
sufficient for that example. Setting
`FileSystemSerdesConfig.GeneratePreview` adds a compact preview of the
value next to that reference, so the operation log shows what was stored.
`durable.BuildPreview` builds one from a `PreviewConfig` with include,
exclude, and mask selectors and a byte cap; `serde-preview-truncation` and
`serde-preview-field-selection` show both base modes, and
`serde-preview-field-selection` and `serde-filesystem` each mask a
sensitive field. A preview is advisory metadata: masking or excluding a
field there does not remove it from the offloaded file.

### Showcase & Edge Cases

| Example | Operations Demonstrated | Expected Terminal State |
|---------|------------------------|------------------------|
| [hello-world](hello-world/main.go) | (no operations) | SUCCEEDED |
| [simple-execution](simple-execution/main.go) | (no operations, structured result) | SUCCEEDED |
| [non-durable](non-durable/main.go) | (plain Lambda, no SDK) | SUCCEEDED |
| [no-replay-execution](no-replay-execution/main.go) | Step (complete on first invocation) | SUCCEEDED |
| [large-payload](large-payload/main.go) | RunInChildContext, ReplayChildren (>256KB) | SUCCEEDED |
| [undefined-results](undefined-results/main.go) | Step, RunInChildContext, Wait (nil results) | SUCCEEDED |
| [comprehensive-operations](comprehensive-operations/main.go) | Step, Wait, Map, Parallel | SUCCEEDED |
| [handler-error](handler-error/main.go) | (handler-level error) | FAILED |
| [order-fulfillment](order-fulfillment/main.go) | Step, Wait, Parallel, RunInChildContext | SUCCEEDED |

## Reference parity

This set mirrors the examples shipped with the reference JavaScript SDK
(`packages/aws-durable-execution-sdk-js-examples/src/examples/` in that
repository). Examples are matched by behaviour, not by name: the reference
groups examples in nested directories (`run-in-child-context/basic`,
`promise/all`, `step/named`), while every Go example is one flat directory
(`child-context-basic`, `future-all`, `named-step`). `parity-map.txt` records
the correspondence, one reference handler per line, and `parity.sh` checks it
against a clone of the reference repository:

```bash
./parity.sh ../../aws-durable-execution-sdk-js
```

It prints one line per reference example with no Go counterpart (`unmapped`
when the map has no row for it, `missing` when the mapped Go example does not
exist yet, `stale` when a row names a reference example that no longer
exists) and exits 0 only when there is no gap. Run it when the reference adds
an example, and add a row for every new handler.

A few examples deliberately differ from the reference because the language
does:

- **Reference cycles.** The reference SDK logs a cyclic object graph with a
  cycle-safe stringifier. Go's `encoding/json` rejects a cycle with an
  error, so `serde-circular-references` shows the resulting `SerdesError`
  from a step and a custom serdes that flattens the graph so it checkpoints.
- **Timestamps.** The reference SDK needs a dedicated serdes to turn ISO
  strings back into `Date` objects after replay. `time.Time` marshals and
  unmarshals itself, so `serde-struct-with-times` shows the default serdes
  round-tripping the fields and a custom serdes changing the wire format.
- **Logger.** The reference SDK has two Powertools Logger examples. There
  is no Powertools for Go, so `logger-slog-handler` covers both with a
  user-supplied `slog.Handler` passed through `WithLogHandler`.

## Build

The build script compiles each example into a static Linux/amd64 binary
named `bootstrap`, suitable for the `provided.al2023` Lambda runtime:

```bash
./build.sh
```

Binaries are output to `.aws-sam/build-artifacts/<example>/bootstrap`.

## Deploy

```bash
./build.sh
sam build
sam deploy --guided
```

On first deploy, SAM prompts for stack name, region, and other parameters.
Subsequent deploys reuse the saved `samconfig.toml`:

```bash
./build.sh && sam build && sam deploy
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ExecutionRoleArn` | (required) | IAM role the deployed functions assume. The template does not create a role, so pass an existing one. `scripts/test-execution-role.yaml` at the repository root is a one-time template that creates a suitable role; its `RoleArn` output is the value to pass. |
| `FunctionNamePrefix` | (empty) | Optional prefix for Lambda function names |

## Testing

Every example has a `handler_test.go` that drives the handler with the
runner returned by `internal/extest.New`. By default that is the in-memory
`durabletest.LocalRunner`, so `go test ./...` needs no AWS credentials or
network access. Setting `DURABLE_EXAMPLES_RUNNER=cloud` switches every
migrated test to `durabletest.CloudRunner`, which invokes the deployed
example and checks the same assertions against its recorded execution. The
function name is `FUNCTION_NAME_PREFIX` + `go-` + the example directory, so
set the prefix the stack was deployed with:

```bash
DURABLE_EXAMPLES_RUNNER=cloud FUNCTION_NAME_PREFIX=myprefix- go test ./... -run TestHandler -timeout 80m
```

Steps that exist only locally, such as resolving a chained invoke or a
callback by hand, are guarded with `runner.Local()` and a comment saying
what resolves them in the cloud. Assertions that hold in only one
environment are guarded the same way; a local-only runner method called in
cloud mode fails the test instead of being skipped. Local timers advance
without waiting; the cloud waits for real, so a test does not assert
elapsed times.

Examples are migrated to the helper in batches by operation family. The
Step & Retry and Wait & WaitForCondition families are migrated; the
remaining families still construct `durabletest.NewLocalRunner` directly
and run locally in both modes; the cloud test matrix below checks the
operation signature of every deployed example regardless.

### Operation signature goldens

Every durable example checks the shape of its operation log against
`testdata/signature.golden`, a JSON list of the type, subtype, name, and
status of each checkpointed operation in checkpoint order (see
`durabletest.EventSignature`). The signature holds no timestamps, tokens,
identifiers, or ARNs, so one file serves both the local runner and the
deployed function. A change that alters the checkpoint structure without
changing the final result fails the test. `non-durable` and
`callback-sender` are plain Lambda functions with no operation log and
have no golden.

`internal/extest/coverage_test.go` parses every `handler_test.go` (see
`extest.ParseHandlerTest`) and fails when:

- the test does not assert `testdata/signature.golden` with exactly one
  mode;
- a `Test` function or `t.Run` subtest runs the handler, directly or
  through a helper, without asserting a signature;
- a golden file the test names is missing, a `testdata/signature*.golden`
  file is not named by the test, or a golden holds anything but operation
  signatures.

Each `handler_test.go` records how its golden is compared by the mode it
passes to `extest.AssertSignature`, with a comment giving the reason when
the mode is not `Ordered`. The mode and the golden path must be written as
`extest` constants or a string literal, so the choice is visible in the
source:

| Mode | Comparison | Used when |
|------|------------|-----------|
| `extest.Ordered` | Same operations in the same order | Operations run sequentially (the default) |
| `extest.Unordered` | Same operations and counts, any order | Map, Parallel, Go, or Async branches checkpoint in scheduling-dependent order |
| `extest.Subset` | Every listed operation is present; others are tolerated | An operation is optional: a fire-and-forget `WaitAsync`, or branches left unstarted by early completion (`Race`, `Any`, `Select`, `MinSuccessful`, a failure threshold) |

A test scenario whose operations differ from the default scenario's
asserts its own file with `extest.AssertSignatureFile`, named
`testdata/signature.<scenario>.golden` (for example
`future-any/testdata/signature.all-fail.golden`). Scenarios that change
only payload sizes or storage paths share the default golden, because the
signature does not record payloads.

To regenerate a golden after an intended change, run the example's test
with `UPDATE_GOLDEN=1` and commit the result:

```bash
UPDATE_GOLDEN=1 go test ./simple-step
```

In `Subset` mode the regenerated file lists every operation that run
produced; delete the optional ones before committing, because a later run
that lacks them would fail. `internal/extest/signature_test.go` shows each
mode passing and, in a child process, failing on a deliberate change to the
operation sequence.

#### Cloud goldens

The cloud test matrix (`cloud/cloud_test.go`, next section) invokes every
deployed example and compares the execution's signature with the same
golden the local test asserts, in the mode the local test declares. An
example whose deployed run differs from its local run by design records
the deployed sequence in `testdata/signature.cloud.golden`; the matrix
then requires the deployed run to match that file and fails when the
deployed run matches the local golden again, so a cloud golden cannot
outlive the difference it documents. The deployed run differs for these
reasons:

- The callback examples resolve their callbacks by calling the callback
  API from a step. Locally that call has no service to reach, so the step
  fails and the callback stays `STARTED`; in the cloud both succeed.
- `interrupted-no-retry` runs a step longer than the deployed function's
  timeout, so the step is `FAILED` in the cloud. The local runner has no
  function timeout.
- `concurrent-operations`, `parallel-wait`, and
  `map-custom-summary-generator-replay` run waits of different lengths in
  concurrent branches. The local runner completes every pending timer at
  once; in the cloud the shorter timer resumes the execution and the
  longer waits are still `STARTED` when it completes.
- `parallel-heterogeneous` invokes a companion function that the deployed
  stack does not provide, so the invoke branch is `FAILED` in the cloud.

To record a cloud golden, run the matrix with `UPDATE_GOLDEN=1`. It never
rewrites `testdata/signature.golden`, which belongs to the local test; it
writes the cloud file when the deployed run differs, and reports a cloud
file the deployed run has made redundant:

```bash
FUNCTION_NAME_PREFIX=myprefix- UPDATE_GOLDEN=1 go test -tags cloud ./cloud -run 'TestExamples/create-callback-simple$'
```

An example whose `handler_test.go` runs in cloud mode through
`extest.New` asserts the cloud file itself with
`extest.AssertSignatureFile(t, result, mode, extest.CloudGoldenPath)`
under `runner.Cloud()`, as `retry-callback` and `interrupted-no-retry`
do, and regenerates it the same way:

```bash
DURABLE_EXAMPLES_RUNNER=cloud FUNCTION_NAME_PREFIX=myprefix- UPDATE_GOLDEN=1 go test ./retry-callback
```

Companion functions (`invoke-simple-target`, `retry-invoke-target`, and
the other invoke targets) are exercised in the cloud through the examples
that invoke them and are not in the matrix; their goldens are asserted
locally.

## Cloud test matrix

`cloud/cloud_test.go` runs every deployed example once with `event.json`,
waits for the terminal state, checks the terminal state, the result of a
succeeding example, or the error type of a failing example against
`cloud/expectations.go`, and compares the execution's operation signature
with the example's golden (see "Cloud goldens" above).
`cloud/expectations.go` is the single table of expected outcomes; the
"Expected Terminal State" column in the tables above mirrors it. A result
that legitimately varies between runs (a timestamp, a measured duration, a
count that depends on scheduling) is checked by a predicate that states
why. The unit test in `cloud/expectations_test.go` fails when an example
in `build.sh` has no entry, so a new example cannot pass the matrix by
default.

The matrix is kept alongside the per-example cloud mode above because it
is one invocation per example with the shared event and runs in about
three minutes; `.github/workflows/cloud-tests.yml` runs both. Deploy with
a stack name and `FunctionNamePrefix` of your own so that concurrent
deployments from different branches do not overwrite each other. Then set
`FUNCTION_NAME_PREFIX` to that prefix (empty if none) and run the test
with the same credentials and region:

```bash
FUNCTION_NAME_PREFIX=myprefix- go test -tags cloud ./cloud -run TestExamples -v -timeout 80m -parallel 8
```

## Invoke

Start a durable execution (requires a qualified function reference):

```bash
aws lambda invoke \
  --function-name 'go-simple-step:$LATEST' \
  --payload fileb://event.json \
  /dev/stdout
```

The function returns with the execution result or a durable execution ARN.
To check execution status:

```bash
aws lambda get-durable-execution \
  --durable-execution-arn <arn>
```

## Cleanup

```bash
sam delete --stack-name <your-stack-name>
```

## Module Structure

The `examples/` directory is a separate Go module with a `replace` directive
pointing at the parent SDK (`../`). This is standard for same-repo example
modules — it ensures examples always build against the local SDK source
without requiring a published version.
