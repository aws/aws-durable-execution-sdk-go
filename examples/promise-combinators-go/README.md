# promise-combinators-go

Demonstrates the SDK's four Promise-combinator operations —
`operations.All`, `AllSettled`, `Any`, and `Race` — all built on top of
`operations.Parallel`. Mirrors the JS reference SDK's own `promise/`
example family (`promise/all`, `promise/all-settled`, `promise/any`,
`promise/race`).

## What it demonstrates

Scenario: checking a product's price across 3 vendors concurrently.

- **`AllHandler`** (deployed as this example's primary handler) —
  `operations.All`: every vendor's quote must succeed; returns the
  ordered slice of quotes. If any vendor fails, the whole operation
  fails outright (the Promise.all-style contract).
- **`AllSettledHandler`** — `operations.AllSettled`: every branch runs
  to completion regardless of individual failures; the caller inspects
  the aggregated `BatchResult` to see which succeeded. One vendor is
  deliberately down here, and the handler still returns the lowest
  *surviving* quote.
- **`AnyHandler`** — `operations.Any`: resolves with the first branch
  to *succeed* (only fails if every branch fails). Two of three
  vendors are down; the handler returns the one that's up.
- **`RaceHandler`** — `operations.Race`: resolves (or fails) with
  whichever branch finishes *first*, success or failure. Uses
  `WithParallelMaxConcurrency(1)` so branch completion order is
  deterministic for testing (this Go SDK has no artificial-delay
  primitive the way the JS examples use `setTimeout` to stagger
  completion).

## Local testing

```bash
go test ./...
```

Every test uses the single-call `runner.Run`.

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
