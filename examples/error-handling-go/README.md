# error-handling-go

Demonstrates the AWS Durable Execution SDK for Go's structured error
hierarchy (`pkg/durable/operations/errors.go`,
`docs/remaining-work.md` §4 tasks 10/11): a common `OperationError`
base recoverable via `errors.As`, embedded by specific types like
`*operations.StepFailedError` and `*operations.NonDeterministicReplayError`.

## What it demonstrates

1. **`*operations.StepFailedError`** (`handler.go`,
   `handler_test.go`): a step that calls a simulated payment
   processor which, when instructed to always fail, declines every
   attempt and exhausts a small, fixed-delay retry strategy. The
   handler uses `errors.As` to recover the structured
   `*operations.StepFailedError` and inspect its `Attempt` field,
   producing a more actionable error message than an opaque error
   ever could — the core point of this example.
2. **`*operations.NonDeterministicReplayError`**
   (`nondeterministic_test.go`): a genuinely different scenario that
   can't be produced by a single, unchanging handler's normal
   operation. It requires a checkpointed operation log from one
   "deployment" of a handler to be replayed against a *different*
   deployment that calls a different kind of durable operation at
   the same step ID — simulating incompatible handler code changes
   between invocations. This example expresses that as one handler
   value with a package-level "deployment version" switch (see that
   file's own doc for why, and how it mirrors
   `pkg/durable/durable_nondeterministic_replay_test.go`'s
   `TestNonDeterministicReplay_StepThenWaitAtSameID`, which uses an
   internal test double not available outside the `durable` package).

## Local testing

```bash
go test ./...
```

- `TestHandler_ChargeSucceeds` — the happy path.
- `TestHandler_ChargeExhaustsRetries` — the card-decline path,
  asserting the handler's `errors.As`-derived error message reflects
  the specific `Attempt`/`ID` fields recovered from the
  `*operations.StepFailedError`.
- `TestNonDeterministicReplay_RedeployedHandlerChangesOperationType` —
  checkpoints a `STEP` on a first invocation, then replays against a
  handler that calls `Wait` at the same step ID, confirming this
  fails with a clearly-typed `*operations.NonDeterministicReplayError`
  rather than silently replay-skipping as if it had succeeded.

This is the local half of the completion criteria described in
`docs/remaining-work.md`; a cloud-runner counterpart does not exist
yet (see that document's task 17a).

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
