# map-parallel-go

Demonstrates `operations.Map` and `operations.Parallel` (plus the
`All` combinator built on top of `Parallel`): concurrent fan-out over a
collection of items, and concurrent fan-out over a fixed set of
independent branches.

## What it demonstrates

- `operations.Map` prices every order ID in the input batch
  concurrently, each within its own isolated child context. Per the
  confirmed real-backend flowchart (internal SDK Operation Diagrams
  design doc, "Map"), this checkpoints an outer
  `CONTEXT/MAP START/SUCCEED/FAIL` wrapping the whole call, and one
  inner `CONTEXT/MAP_ITERATION START/SUCCEED/FAIL` per item - each
  itself a full child-context lifecycle, so a nested `Step` inside one
  iteration checkpoints and replay-skips completely independently of
  its siblings.
- `operations.All` (built on `operations.Parallel`) runs two
  independent pre-checkout verifications - a fraud check and an
  inventory check - concurrently. `Parallel` checkpoints the
  structurally identical `CONTEXT/PARALLEL` +
  `CONTEXT/PARALLEL_BRANCH` lifecycle.

Both operations run their items/branches on separate goroutines,
coordinated with the SDK's suspend-detection machinery so that a
branch blocking on a real external operation (a `Wait`, a callback)
correctly suspends the whole invocation while other, already-finished
branches' results are preserved across the resulting replay.

## Local testing

```bash
go test ./...
```

Two tests cover: a batch of orders being priced and both
verifications passing (asserting on the checkpointed `CONTEXT/MAP`,
`CONTEXT/MAP_ITERATION`, `CONTEXT/PARALLEL`, and
`CONTEXT/PARALLEL_BRANCH` operations, not just the final result), and
an empty order batch (`Map` over zero items).

This is the local half of the completion criteria described in
`docs/remaining-work.md`; a cloud-runner counterpart does not exist
yet (see that document's task 17a).

## Deploying

This example can be built and deployed exactly like
`examples/simple-step-go` (see that example's README for the full
container-image deployment steps) - not done in this session's work.
