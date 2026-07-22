# wait-for-callback-failing-submitter-go

Demonstrates `operations.WithWaitForCallbackSubmitterRetryStrategy`:
the *submitter* function itself (not the callback's own timeout) can
fail and be retried, exactly like any `Step`'s own retry policy.
Mirrors the JS reference SDK's own `wait-for-callback/failing-submitter`
example.

## What it demonstrates

This example's submitter always fails. A 3-attempt retry policy (1
second delay between attempts) is configured via
`WithWaitForCallbackSubmitterRetryStrategy`; once exhausted, the whole
`WaitForCallback` operation fails. The handler catches that error and
returns a structured `{success: false, error: ...}` result rather than
letting it propagate — matching the JS example's own try/catch
structure.

## Local testing

```bash
go test ./...
```

Since `LocalTestRunnerConfig.SkipTime` defaults to `true`, the
submitter's own 1-second retry delay resolves synchronously within a
single `runner.Run` call (see `examples/retry-go`'s own test for the
identical precedent with `Step`'s own retry delay).

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
