# AWS Lambda Durable Execution SDK for Go — Examples

Deployable example workflows demonstrating the AWS Lambda Durable Execution SDK for Go.

## Prerequisites

- Go 1.25+
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
| [child-context-serdes-large-payload](child-context-serdes-large-payload/main.go) | RunInChildContext, FileSystemSerdes (>256KB) | SUCCEEDED |
| [child-context-large-data](child-context-large-data/main.go) | RunInChildContext (ReplayChildren) | SUCCEEDED |
| [child-context-error-propagation](child-context-error-propagation/main.go) | RunInChildContext, error propagation | SUCCEEDED |
| [child-context-failing-step](child-context-failing-step/main.go) | RunInChildContext, Step failure | SUCCEEDED |
| [child-context-checkpoint-size-limit](child-context-checkpoint-size-limit/main.go) | RunInChildContext, size limit | SUCCEEDED |
| [child-context-nested-blocks](child-context-nested-blocks/main.go) | RunInChildContext, nested parent/child/grandchild | SUCCEEDED |
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
| [map-tolerated-failure-count](map-tolerated-failure-count/main.go) | Map, WithToleratedFailureCount | SUCCEEDED |
| [map-failure-threshold](map-failure-threshold/main.go) | Map, fail-fast threshold | SUCCEEDED |
| [map-high-concurrency-invoke](map-high-concurrency-invoke/main.go) | Map, Invoke, WithMaxConcurrency | SUCCEEDED |
| [map-tolerated-failure-percentage](map-tolerated-failure-percentage/main.go) | Map, WithToleratedFailurePercentage | SUCCEEDED |
| [map-completion-config-issue](map-completion-config-issue/main.go) | Map, early-completion partial results | SUCCEEDED |
| [map-virtual-context](map-virtual-context/main.go) | Map, WithNesting (NestingFlat) | SUCCEEDED |
| [parallel-basic](parallel-basic/main.go) | Parallel, WithMaxConcurrency | SUCCEEDED |
| [parallel-empty](parallel-empty/main.go) | Parallel (no branches) | SUCCEEDED |
| [parallel-invoke](parallel-invoke/main.go) | Parallel, Invoke | SUCCEEDED |
| [parallel-wait](parallel-wait/main.go) | Parallel, Wait | SUCCEEDED |
| [parallel-error-preservation](parallel-error-preservation/main.go) | Parallel, error ordering | SUCCEEDED |
| [parallel-min-successful](parallel-min-successful/main.go) | Parallel, WithCompletion (min successful) | SUCCEEDED |
| [parallel-tolerated-failure](parallel-tolerated-failure/main.go) | Parallel, WithToleratedFailureCount | SUCCEEDED |
| [parallel-tolerated-failure-percentage](parallel-tolerated-failure-percentage/main.go) | Parallel, WithToleratedFailurePercentage | SUCCEEDED |
| [parallel-virtual-context](parallel-virtual-context/main.go) | Parallel, WithNesting (NestingFlat) | SUCCEEDED |
| [parallel-heterogeneous](parallel-heterogeneous/main.go) | Parallel, Step, Wait, Invoke | SUCCEEDED |

### Future Combinators & Concurrency

| Example | Operations Demonstrated | Expected Terminal State |
|---------|------------------------|------------------------|
| [future-all](future-all/main.go) | Go, All | SUCCEEDED |
| [future-all-settled](future-all-settled/main.go) | Go, AllSettled | SUCCEEDED |
| [future-all-wait](future-all-wait/main.go) | Go, All, WaitAsync | SUCCEEDED |
| [future-any](future-any/main.go) | Go, Any | SUCCEEDED |
| [future-race](future-race/main.go) | Go, Race | SUCCEEDED |
| [future-race-wait](future-race-wait/main.go) | Go, Race, WaitAsync | SUCCEEDED |
| [future-combinators-mixed](future-combinators-mixed/main.go) | Go, All, Any, Race | SUCCEEDED |
| [future-replay](future-replay/main.go) | Go, Future replay determinism | SUCCEEDED |
| [future-unhandled-error](future-unhandled-error/main.go) | Go, unhandled Future error | FAILED |
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
| [serde-basic](serde-basic/main.go) | Step, WithStepSerdes | SUCCEEDED |
| [serde-callback-deserializer](serde-callback-deserializer/main.go) | WithCallbackDeserializer, custom callback deserialization | SUCCEEDED |
| [serde-custom-config](serde-custom-config/main.go) | WithSerdes (handler-level) | SUCCEEDED |
| [logger-after-wait](logger-after-wait/main.go) | Wait, WithLogger, replay suppression | SUCCEEDED |
| [logger-after-callback](logger-after-callback/main.go) | WaitForCallback, WithLogger | SUCCEEDED |
| [logger-log-levels](logger-log-levels/main.go) | WithLogger, all log levels | SUCCEEDED |
| [plugin-lifecycle](plugin-lifecycle/main.go) | WithPlugins, hook lifecycle ordering | SUCCEEDED |

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
| `FunctionNamePrefix` | (empty) | Optional prefix for Lambda function names |

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
