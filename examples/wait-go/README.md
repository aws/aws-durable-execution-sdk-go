# wait-go

Demonstrates `operations.Wait`, the SDK's basic pause-execution
primitive, across four variants mirroring the JS reference SDK's own
`wait/` example family.

## What it demonstrates

- **`BasicHandler`** (deployed as this example's primary handler) —
  the simplest possible usage: `operations.Wait(dc, "cool-down",
  types.Duration{Seconds: 1})`. The Lambda invocation suspends (no
  compute charges) and resumes once the timer fires.
- **`ConfigurableHandler`** — the wait duration comes from the event
  payload instead of being hardcoded.
- **`NamedHandler`** — gives the wait a descriptive, caller-chosen
  name (`await-payment-settlement`) rather than a generic id, for
  observability.
- **`UnawaitedHandler`** — schedules a `Wait` in a background
  goroutine and returns without ever synchronizing on it. This is
  included for parity with the JS reference SDK's own
  `wait/unawaited` example (which relies on JS's fire-and-forget
  Promise semantics), but Go has no direct equivalent — see that
  handler's own doc comment for why this is **not** a recommended
  production pattern in Go.

## Local testing

```bash
go test ./...
```

Every test here uses the single-call `runner.Run` (not `Continue`),
since `LocalTestRunnerConfig.SkipTime` defaults to `true` and
fast-forwards every `Wait` synchronously within one invocation.

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
