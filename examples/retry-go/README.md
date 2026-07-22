# retry-go

Demonstrates `operations.Step`'s retry strategies and retry-delay
suspension: a step that calls a flaky external dependency and retries
it with a configured backoff strategy, without the durable execution
incurring compute charges while it waits between attempts.

## What it demonstrates

The handler calls a simulated flaky dependency through two steps in
sequence, each configured with a different retry strategy from
`pkg/durable/utils/retry.go`:

1. **`call-flaky-dependency-preset`** uses
   `utils.Presets.ExponentialBackoff()` — the ready-to-use preset
   (`maxAttempts=3, initialDelay=5s, maxDelay=5m, backoffRate=2`, full
   jitter) most callers reach for first.
2. **`call-flaky-dependency-custom`** uses
   `operations.WithStepRetryStrategy` with a hand-built strategy from
   `utils.Presets.FixedDelay` — a fixed, non-zero 30-second delay
   across up to 3 attempts, demonstrating the fully-parameterized API
   for callers who need more control than the presets expose (see
   `utils.CreateRetryStrategy` for the general-purpose constructor
   both presets and custom strategies are built from).

Per the confirmed real-backend behavior (`docs/remaining-work.md` §2
task 7): a retry with a **non-zero** delay suspends the whole
invocation — the SDK returns `PENDING` and the Lambda execution
environment can be frozen or recycled — rather than blocking
in-process, exactly like `Wait` suspends for its duration. A **zero**
delay is the one deliberate exception that re-executes immediately
without suspending.

## Local testing

```bash
go test ./...
```

`handler_test.go` covers three scenarios against the default
`SkipTime: true` runner — the retry delays resolve invisibly within a
single `Run` call, so these tests focus on the retry *outcome*
(attempt counts, success/failure) rather than the suspension itself:

- `TestHandler_SucceedsAfterRetries` — both steps fail once and
  succeed on their second attempt.
- `TestHandler_NoRetriesNeeded` — both steps succeed on the first
  attempt.
- `TestHandler_ExhaustsRetries` — the preset-strategy step never
  succeeds and exhausts all 3 attempts, failing the whole execution
  with a `*operations.StepFailedError` before the custom-strategy step
  ever runs.

`suspend_resume_test.go`'s `TestHandler_CustomStrategyRetryDelaySuspends`
demonstrates the genuine suspend mechanism directly, using a runner
configured with `SkipTime: false` and `LocalTestRunner.RunAsync`
(exactly one raw invocation, unlike `Run`/`Continue`, which internally
loop until the execution reaches a terminal status or genuinely
cannot make further progress - see that test's own doc comment for why
`Continue`, tried first while writing this test, turned out to silently
resolve the entire retry loop across multiple internal invocations
before ever returning a `PENDING` result, exactly as if `SkipTime` were
enabled). This test deliberately targets the **custom**-strategy step,
not the preset one: `utils.Presets.ExponentialBackoff`'s full jitter
means its actual delay is a random draw that truncates to exactly zero
roughly 1 run in 5, which would make an assertion of genuine suspension
against it a real, inherent flake rather than a bug — `FixedDelay` has
no jitter, so its 30s delay is deterministic on every run. The single
invocation is asserted to be genuinely `PENDING` with the exact
in-flight attempt count recorded on the retrying step, and that the
preset step (which runs first) already succeeded — showing the
invocation actually suspends mid-retry rather than the delay being
invisibly resolved.

This is the local half of the completion criteria described in
`docs/remaining-work.md`; a cloud-runner counterpart does not exist
yet (see that document's task 17a).

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
