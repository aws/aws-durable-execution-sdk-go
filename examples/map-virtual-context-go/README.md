# map-virtual-context-go

Demonstrates `operations.WithMapNesting(operations.NestingModeFlat)` —
Map's own "virtual context" cost optimization. Mirrors the JS reference
SDK's own `map/virtual-context` example.

## What it demonstrates

Under FLAT nesting, each iteration's own `CONTEXT/MAP_ITERATION`
`ContextStarted`/`ContextSucceeded` checkpoint pair is skipped entirely
(roughly a 30% checkpoint-count reduction for large Maps) — each
item's own inner `Step` is checkpointed directly under the *outer* Map
context instead of an intermediate per-item context.

## Local testing

```bash
go test ./...
```

Confirms zero per-iteration `MapIteration` contexts are ever
checkpointed, and every inner `Step`'s own `ParentID` points directly
at the outer Map context — mirroring
`pkg/durable/durable_flat_nesting_test.go`'s own established assertion
pattern for this SDK feature.

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
