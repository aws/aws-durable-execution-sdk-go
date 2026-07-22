# run-in-child-context-go

Demonstrates `operations.RunInChildContext`: grouping related durable
operations (here, two payment-processing steps) into an isolated child
context with its own step-ID namespace and its own single
CONTEXT-level checkpoint, distinct from a sibling step at the root
context.

## What it demonstrates

The handler processes an order in two parts:

1. A **"payment" child context** (`operations.RunInChildContext`)
   wrapping two steps: `validate-amount` and `charge-card`. Both steps
   are checkpointed independently inside the child context's own
   namespace (`1-1`, `1-2` if the context itself is step `1`), but the
   context as a whole is also checkpointed as a single `CONTEXT`
   operation. On replay, if the context has already succeeded, neither
   nested step's body re-runs — the context-level checkpoint is a
   coarser barrier on top of the steps' own replay-skip checks.
2. A **sibling `create-shipping-label` step** at the root context,
   entirely independent of what happens inside `payment`.
3. A **`UseCustomChildSerdes:true` scenario** (`customSerdesHandlerScenario`)
   demonstrating `operations.WithChildSerdes`: a second, mutually
   exclusive `RunInChildContext` call ("reconciliation") whose OWN
   checkpointed result is serialized with a custom `types.Serdes`
   (`screamingSnakeCaseSerdes` — the SAME implementation
   `examples/custom-config-go` uses at `Step` scope via `WithStepSerdes`,
   duplicated here to prove the same mechanism also works when scoped to
   a `RunInChildContext` instead), rather than the default JSON Serdes.
   Selected via `OrderEvent.UseCustomChildSerdes` (mirroring
   `examples/large-payload-go`'s own `Scenario`-field pattern), since a
   single deployed Lambda function can only wire one `Handler[TEvent,
   TResult]` pair via `durable.WithDurableExecution`.

## Local testing

This example's handler is split into `handler.go` (business logic) and
`main.go` (Lambda Runtime API plumbing), so `handler_test.go` can
exercise it directly against the SDK's `pkg/durable/testing.LocalTestRunner`
without deploying anything:

```bash
go test ./...
```

The tests assert on:

- The final result (`OrderResult`)
- The `payment` `CONTEXT` operation's own type, status, and checkpointed
  result
- The two operations nested *inside* `payment` (`validate-amount`,
  `charge-card`) via `TestResult.GetChildOperations`
- The sibling `create-shipping-label` step, confirming it is **not**
  nested under `payment`
- (`TestHandler_ReconciliationUsesCustomChildSerdes`) the RAW checkpointed
  payload string for the `reconciliation` `CONTEXT` operation, confirming
  `WithChildSerdes`'s custom Serdes genuinely ran (SCREAMING_SNAKE_CASE
  keys), and that the nested `compute-reconciled-total` step's own
  checkpointed payload remains in the default format

## Deploying

This example is deployed to a real AWS account as
`run-in-child-context-go-example` (see `docs/remaining-work.md` for the
full deployment history). Build and push exactly like
`examples/simple-step-go` (see that example's README for the full
container-image steps using `finch`/Lambda's `provided.al2023-arm64`
base image), then `aws lambda update-function-code` and
`aws lambda publish-version`. Invoke the custom-Serdes-scoped-to-
RunInChildContext scenario with:

```json
{"orderId": "recon-1", "amount": 123.45, "useCustomChildSerdes": true}
```
