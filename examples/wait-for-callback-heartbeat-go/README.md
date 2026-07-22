# wait-for-callback-heartbeat-go

Demonstrates `operations.WithWaitForCallbackHeartbeatTimeout`: a
callback that requires the external system to periodically confirm
it's still working, rather than a single fixed overall timeout.
Mirrors the JS reference SDK's own `wait-for-callback/heartbeat-sends`
example.

## What it demonstrates

The handler registers a callback with a 30-second heartbeat timeout.
This Go SDK has no distinct heartbeat-*send* API surfaced on
`types.StepContext` — the external system's own
`SendDurableExecutionCallbackHeartbeat` calls happen out-of-band,
against the real backend, and are what keep the callback alive
between the submitter finishing and the eventual
`SendDurableExecutionCallbackSuccess`.

## A real local-testing limitation

`testing.LocalTestRunner` has no `SendCallbackHeartbeat`-equivalent
test helper, and `types.Operation` doesn't retain the checkpointed
`CallbackOptions.HeartbeatTimeoutSeconds` value for later inspection
either — so this Go SDK's own testing package cannot independently
verify either half of this feature locally today. See
`handler_test.go`'s own top-level doc comment for the full
explanation. This test only confirms the callback completes normally
via `SendCallbackSuccess`, exactly like `examples/wait-for-callback-go`'s
own basic test.

## Local testing

```bash
go test ./...
```

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
