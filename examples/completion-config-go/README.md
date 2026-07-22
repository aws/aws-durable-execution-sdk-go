# completion-config-go

Demonstrates `operations.WithMapCompletionConfig` (`types.CompletionConfig`'s
`ToleratedFailureCount` threshold): a `Map` fan-out over a batch of
records where some records are deliberately configured to fail, showing
that the batch as a whole can **succeed despite tolerated failures**, or
correctly **fail once the tolerance is exceeded**, depending on the
configured threshold.

This closes a real, previously-zero-coverage gap: `CompletionConfig`
(`MinSuccessful`/`ToleratedFailureCount`/`ToleratedFailurePercentage`) was
already fully implemented and unit-tested at the SDK level (see
`docs/remaining-work.md` §1 task 8, §3 task 9), but no example ever set a
`CompletionConfig` option at all before this one -
`examples/map-parallel-go`'s handler uses `operations.Map`/`operations.All`
with no completion-config options set. See
`docs/ts-sdk-examples-comparison.md`'s "Map / Parallel" section, which
identifies this as the single largest concrete example-coverage gap
between the TypeScript and Go SDKs (12 `CompletionConfig` variants across
`map/*`/`parallel/*` in TS, 0 in Go before this example).

## What it demonstrates

`handler.go`'s handler runs `operations.Map` over a batch of record IDs,
where a configurable subset (`BadRecordIDs`) fail their simulated
`import-record` step deterministically (not a transient/retryable
failure - there's nothing to retry, so no retry strategy is configured).
`WithMapCompletionConfig` is applied with a `ToleratedFailureCount`
threshold taken directly from the event:

- **Within threshold** (`TestHandler_ToleratesFailuresWithinThreshold`):
  2 of 5 records are bad, `ToleratedFailureCount: 2` - the batch's outer
  `CONTEXT/MAP` operation, and the whole execution, **SUCCEED**. The two
  failed `MAP_ITERATION` children are still genuinely checkpointed as
  `FAILED` (the tolerance changes the *batch's* overall outcome, not
  whether each item's own failure is real), while the three good records
  succeed and appear in the result's `ImportedRecordIDs`.
- **Threshold exceeded** (`TestHandler_ExceedsToleratedFailureThreshold`):
  the same 2 of 5 records are bad, but `ToleratedFailureCount: 1` only
  tolerates one failure - the outer `CONTEXT/MAP` operation, and the
  whole execution, correctly **FAIL**, matching
  `batchCompletion.overallSucceeded`'s documented semantics in
  `pkg/durable/operations/batch.go`.

Per `operations.Map`'s own doc, a per-item failure never stops other
already-running items - every item still runs to completion regardless of
the threshold, and failures only count toward the configured limit when
deciding the batch's own final outcome.

## Local testing

```bash
GOPROXY=direct GOSUMDB=off go test ./...
```

Both scenarios assert on the checkpointed operation log (the outer
`CONTEXT/MAP` operation's own status, each `MAP_ITERATION` child's status
by index, and the aggregated result), plus an `AssertEventSignatures`
golden-file check pinning the exact shape of the operation log for each
scenario. Regenerate golden files with:

```bash
UPDATE_GOLDEN=1 GOPROXY=direct GOSUMDB=off go test ./...
```

## Deploying

This example is built and deployed exactly like `examples/simple-step-go`
(see that example's README for the full container-image deployment
steps) - `main.go`/`Dockerfile` are copied from that example verbatim,
adjusted only for the module path and handler reference, and it uses the
same production `pkg/durable/awssdk`-backed `checkpoint.Client`.

```sh
finch build -t completion-config-go-example .
finch tag completion-config-go-example:latest <account>.dkr.ecr.<region>.amazonaws.com/completion-config-go-example:latest
finch push <account>.dkr.ecr.<region>.amazonaws.com/completion-config-go-example:latest

aws lambda create-function \
  --function-name completion-config-go-example \
  --package-type Image \
  --code ImageUri=<account>.dkr.ecr.<region>.amazonaws.com/completion-config-go-example:latest \
  --role <execution-role-arn> \
  --architectures arm64 \
  --durable-config '{"RetentionPeriodInDays":1,"ExecutionTimeout":120}'

aws lambda publish-version --function-name completion-config-go-example

# Durable functions require a qualified ARN (a published version or
# alias) - $LATEST is rejected.
aws lambda invoke \
  --function-name completion-config-go-example:1 \
  --invocation-type RequestResponse \
  --payload '{"recordIds":["rec-1","rec-2","rec-3","rec-4","rec-5"],"badRecordIds":["rec-2","rec-4"],"toleratedFailureCount":2}' \
  response.json
```

This example has been deployed and verified end-to-end against a real
Lambda function - see `docs/remaining-work.md` §10 task 23's note for the
deployed function's name/ARN and what the real invoke confirmed.
