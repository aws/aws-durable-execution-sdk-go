# large-payload-go

Demonstrates `operations.ResultTooLargeError`
(`pkg/durable/operations/errors.go`, `docs/remaining-work.md` §6 task 16) -
this SDK's conservative, client-side fallback for a single durable
operation's result exceeding the 750KB checkpoint payload threshold.

This closes `docs/ts-sdk-examples-comparison.md`'s gap 6/8 (large-payload
/ Serdes-overflow handling): the underlying `FileSystemSerDes`/`OVERFLOW`
automatic-offload mechanism the TS/Python reference SDKs document remains
unbuilt in Go (Java's own guide says "Coming soon" for the identical
feature) - **and this SDK's own comparison table entry does not claim
otherwise**. What Go actually implemented, per task 16's own research
into the real reference SDKs' large-payload mechanisms, is a conservative
client-side size-limit rejection: fail clearly with a typed error instead
of silently sending an oversized payload that would presumably fail
unhelpfully at the network/backend layer. Before this example, no Go
example demonstrated even that conservative behavior itself.

## What it demonstrates

Two mutually exclusive scenarios, selected via `LargePayloadEvent.Scenario`:

- **`"oversized"`** (`TestHandler_OversizedResultFailsClearly`): a Step
  (`generate-oversized-report`) returns a deterministic 800KB
  (`oversizedPayloadSizeBytes`) repeated-character string *directly* as
  its result. `operations.checkResultSize` rejects this **before** it is
  ever checkpointed, and the Step call returns a
  `*operations.ResultTooLargeError`, which this handler inspects via
  `errors.As` (mirroring `error-handling-go`'s identical pattern for
  `*operations.StepFailedError`) and propagates as a clear, actionable
  failure - the whole execution fails cleanly, rather than silently
  corrupting/truncating the oversized value or letting it reach the
  network layer and fail unhelpfully there.
- **`"reference"`** (`TestHandler_ReferencePatternSucceeds`): a *different*
  Step (`stage-report-externally`) generates the **same size** (800KB) of
  large data, but instead of returning it directly, calls
  `StageLargeResultExternally` - a **simulated** stand-in for real
  external storage (an in-memory, package-level map keyed by a generated
  reference ID, standing in for what would be a real `s3.PutObject` call
  in production - see that function's doc comment for the explicit
  disclaimer that this is NOT real S3 and does not prove anything about
  real external-storage integration) - and returns only the small
  reference key as its actual Step result. This is the RECOMMENDED
  pattern the real TS/Python/Java reference SDKs actually use for large
  results, per task 16's research: stage large data externally and
  return a reference, or supply a custom `Serdes` that does the same.
  Because the *checkpointed* result is just the small reference string,
  it is comfortably under the threshold and the Step succeeds normally.

Both scenarios generate the identical-sized underlying value
(`generateOversizedPayload`, a deterministic repeated-`'x'` string of an
exact, known byte length - not something size-fuzzy), so the contrast
between "returned directly: fails" and "staged + reference returned:
succeeds" is a direct, apples-to-apples comparison of the two approaches
to the same data.

**What this example does NOT prove**: it does not demonstrate real S3/
DynamoDB integration (the "external staging" is an in-memory map, purely
local to this process and this example's own test run - see
`externalStore`'s doc for the full disclaimer), and it does not
demonstrate the `FileSystemSerDes`/`OVERFLOW` automatic-offload mechanism
the TS/Python reference SDKs have, since that mechanism remains unbuilt
in this Go SDK (task 16's own finding).

## Local testing

```bash
GOPROXY=direct GOSUMDB=off go test ./...
```

Both scenarios assert on the checkpointed operation log (the STEP's own
status, its exact checkpointed `ErrorMessage` for the oversized case, and
the small reference-key result for the reference case), plus an
`AssertEventSignatures` golden-file check pinning the exact shape of the
operation log for each scenario. Regenerate golden files with:

```bash
UPDATE_GOLDEN=1 GOPROXY=direct GOSUMDB=off go test ./...
```

## Deploying

This example is built and deployed exactly like `examples/completion-config-go`
(see that example's README for the full container-image deployment
steps) - `main.go`/`Dockerfile` are copied from that example verbatim,
adjusted only for the module path and handler reference, and it uses the
same production `pkg/durable/awssdk`-backed `checkpoint.Client`.

```sh
finch build -t large-payload-go-example .
finch tag large-payload-go-example:latest <account>.dkr.ecr.<region>.amazonaws.com/large-payload-go-example:latest
finch push <account>.dkr.ecr.<region>.amazonaws.com/large-payload-go-example:latest

aws lambda create-function \
  --function-name large-payload-go-example \
  --package-type Image \
  --code ImageUri=<account>.dkr.ecr.<region>.amazonaws.com/large-payload-go-example:latest \
  --role <execution-role-arn> \
  --architectures arm64 \
  --durable-config '{"RetentionPeriodInDays":1,"ExecutionTimeout":120}'

aws lambda publish-version --function-name large-payload-go-example

# Durable functions require a qualified ARN (a published version or
# alias) - $LATEST is rejected.
aws lambda invoke \
  --function-name large-payload-go-example:1 \
  --invocation-type RequestResponse \
  --payload '{"scenario":"oversized"}' \
  response.json

aws lambda invoke \
  --function-name large-payload-go-example:1 \
  --invocation-type RequestResponse \
  --payload '{"scenario":"reference"}' \
  response.json
```

This example has been deployed and verified end-to-end against a real
Lambda function, including confirming the client-side
`ResultTooLargeError` rejection fires (and fails the execution) FAST -
before any oversized payload is ever sent over the network to the real
Lambda Durable Functions backend - see `docs/remaining-work.md` §10's
entry for this task for the full real-cloud verification writeup,
including both invocations' durations and `GetDurableExecutionHistory`
results.
