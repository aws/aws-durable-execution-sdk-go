# parallel-processing-go

Demonstrates `operations.Parallel` (via its typed convenience wrapper
`operations.All`) as a **focused, standalone** reference - fanning out
over several genuinely independent branches for a single order (fraud
check, inventory check, address validation, payment authorization), with
**no `Map` anywhere in this example**.

This closes `docs/ts-sdk-examples-comparison.md`'s "parallel-processing"
catalog entry (the TS SDK's top-level `parallel` example group) with the
EXACT catalog name. The pre-existing `examples/map-parallel-go` already
demonstrates `Map` and `Parallel` **together** in one combined
order-checkout scenario (pricing orders via `Map`, then two verification
branches via `Parallel`/`All`) - useful as a "how do these compose"
reference, but not a minimal "learn `Parallel` by itself" one. This
example is that missing minimal reference: its entire body is a single
`operations.All` call over four branches, so a reader looking specifically
for "how does `Parallel` work" isn't forced to also understand `Map`
first.

## What it demonstrates

- **`TestHandler_AllChecksPassConcurrently`**: all four independent
  branches (fraud-check, inventory-check, address-validation,
  payment-authorization) run concurrently and succeed, asserting on the
  outer `CONTEXT/PARALLEL` operation, its four `PARALLEL_BRANCH` children,
  and each branch's own nested `STEP` by name.
- **`TestHandler_FraudCheckFailsWholeBatch`**: the fraud-check branch's
  own `Step` returns a genuine error (order amount over a simplistic
  threshold) - `operations.All`'s `Promise.all`-style semantics
  (`batch.go`'s own doc: "returns the successful results only if every
  branch succeeds; otherwise ... `*AggregateError`") mean this single
  failing branch fails the WHOLE `operations.All` call and the whole
  execution, even though the other three branches succeed on their own.
  Asserts the exact composed error message format and that the three
  sibling branches still ran to completion and succeeded independently -
  a failing branch does not cancel its siblings, only the aggregate
  outcome.

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
finch build -t parallel-processing-go-example .
finch tag parallel-processing-go-example:latest <account>.dkr.ecr.<region>.amazonaws.com/parallel-processing-go-example:latest
finch push <account>.dkr.ecr.<region>.amazonaws.com/parallel-processing-go-example:latest

aws lambda create-function \
  --function-name parallel-processing-go-example \
  --package-type Image \
  --code ImageUri=<account>.dkr.ecr.<region>.amazonaws.com/parallel-processing-go-example:latest \
  --role <execution-role-arn> \
  --architectures arm64 \
  --durable-config '{"RetentionPeriodInDays":1,"ExecutionTimeout":120}'

aws lambda publish-version --function-name parallel-processing-go-example

# Durable functions require a qualified ARN (a published version or
# alias) - $LATEST is rejected.
aws lambda invoke \
  --function-name parallel-processing-go-example:1 \
  --invocation-type RequestResponse \
  --payload '{"orderId":"order-1","sku":"widget-42","amount":49.99}' \
  response.json
```

See `docs/remaining-work.md` §10 task 23's writeup for this example's
real-cloud verification results.
