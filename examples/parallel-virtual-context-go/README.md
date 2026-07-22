# parallel-virtual-context-go

Demonstrates `operations.WithParallelNesting(operations.NestingModeFlat)`
— Parallel's own "virtual context" cost optimization, the sibling of
`examples/map-virtual-context-go`'s identical mechanism one level up.
Mirrors the JS reference SDK's own `parallel/virtual-context` example.

## What it demonstrates

Under FLAT nesting, each branch's own `CONTEXT/PARALLEL_BRANCH`
checkpoint pair is skipped entirely — each branch's own inner `Step`
is checkpointed directly under the *outer* Parallel context instead of
an intermediate per-branch context.

## Local testing

```bash
go test ./...
```

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
