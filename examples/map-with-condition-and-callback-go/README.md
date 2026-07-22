# map-with-condition-and-callback-go

Demonstrates `operations.Map` combined with `operations.WaitForCondition`
**and** `operations.WaitForCallback` in a single workflow - a genuinely
different combination from any single existing example in this repo:

- `examples/map-parallel-go` combines `Map` + `Parallel` (no `Wait*`
  operations at all).
- `examples/wait-for-callback-go` and `examples/wait-for-condition-go`
  each demonstrate exactly one `Wait*` operation in isolation (no `Map`).
- `examples/completion-config-go` demonstrates `Map`'s `CompletionConfig`
  alone.

This closes `docs/ts-sdk-examples-comparison.md`'s
"map-with-condition-and-callback" catalog entry with the EXACT catalog
name.

## What it demonstrates

A batch of orders is priced via `Map` (one Map iteration per order). ONE
SPECIFIC order in the batch (`CarrierPollOrderID`) additionally polls a
simulated external shipping-carrier status API via `WaitForCondition`,
nested INSIDE that one Map iteration's own child context - a genuinely
different nesting depth from `wait-for-condition-go`'s top-level-only
`WaitForCondition` call. After the ENTIRE Map batch resolves (every order
priced, including the carrier-polling one), the WHOLE BATCH's completion
is gated on a single human-approval callback via `WaitForCallback`,
scoped to the aggregate priced result - not per-order, and not
per-Map-iteration. This is the key structural difference from
`wait-for-callback-go`'s own single-order approval: here the callback
gates the ENTIRE Map's aggregate result, not one item's own processing.

- **`TestHandler_ApprovedBatchWithCarrierPolling`**: a 3-order batch,
  with `order-2` requiring carrier-status polling (resolves after 3
  simulated polls, matching `wait-for-condition-go`'s identical polling
  cadence). Both invocations are driven via `LocalTestRunner.Continue`:
  the first invocation prices all three orders and fully resolves the
  carrier poll (via `SkipTime`'s fast-forwarded waits) before suspending
  at the callback; a simulated manager then approves via
  `SendCallbackSuccess`, and the second invocation completes. Asserts
  every Map iteration and the nested carrier poll are checkpointed
  `SUCCEEDED` BEFORE the callback resolves, that only `order-2` carries a
  non-empty `CarrierStatus`, and pins the full operation-log shape via a
  golden file.
- **`TestHandler_RejectedBatch`**: a 2-order batch with no carrier-polling
  order at all (isolating the callback-rejection path from the polling
  mechanism), where a simulated manager rejects the whole batch via
  `SendCallbackFailure`. Asserts the exact composed
  `ChildContextFailedError`/`CallbackFailedError` message format, the
  structured checkpointed errors on both the `CALLBACK` and enclosing
  `CONTEXT` operations, and that both orders' own `Map` iterations remain
  checkpointed `SUCCEEDED` despite the later rejection - pricing happens
  BEFORE the approval gate, so the callback's rejection doesn't
  retroactively unwind already-completed `Map` work.

## Local testing

```bash
GOPROXY=direct GOSUMDB=off go test ./...
```

Both scenarios assert on the checkpointed operation log plus an
`AssertEventSignatures` golden-file check pinning the exact shape of the
operation log for each scenario. Regenerate golden files with:

```bash
UPDATE_GOLDEN=1 GOPROXY=direct GOSUMDB=off go test ./...
```

## Deploying

This example is built and deployed exactly like `examples/large-payload-go`
(see that example's README for the full container-image deployment
steps) - `main.go`/`Dockerfile` are copied from that example verbatim,
adjusted only for the module path and handler reference, and it uses the
same production `pkg/durable/awssdk`-backed `checkpoint.Client`.

```sh
finch build -t map-with-condition-and-callback-go-example .
finch tag map-with-condition-and-callback-go-example:latest <account>.dkr.ecr.<region>.amazonaws.com/map-with-condition-and-callback-go-example:latest
finch push <account>.dkr.ecr.<region>.amazonaws.com/map-with-condition-and-callback-go-example:latest

aws lambda create-function \
  --function-name map-with-condition-and-callback-go-example \
  --package-type Image \
  --code ImageUri=<account>.dkr.ecr.<region>.amazonaws.com/map-with-condition-and-callback-go-example:latest \
  --role <execution-role-arn> \
  --architectures arm64 \
  --durable-config '{"RetentionPeriodInDays":1,"ExecutionTimeout":120}'

aws lambda publish-version --function-name map-with-condition-and-callback-go-example

# Durable functions require a qualified ARN (a published version or
# alias) - $LATEST is rejected. This first invoke will suspend (PENDING)
# at the batch-approval callback - poll GetDurableExecution, then resolve
# it via SendDurableExecutionCallbackSuccess/Failure using the callback ID
# from GetDurableExecutionHistory.
aws lambda invoke \
  --function-name map-with-condition-and-callback-go-example:1 \
  --invocation-type RequestResponse \
  --payload '{"orderIds":["order-1","order-2","order-3"],"carrierPollOrderId":"order-2"}' \
  response.json
```

See `docs/remaining-work.md` §10 task 23's writeup for this example's
real-cloud verification results (or deferred-verification status, if
deployment was prioritized to only one of the two new examples this
session).
