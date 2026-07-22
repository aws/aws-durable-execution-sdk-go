# wait-for-callback-timeout-go

Demonstrates `operations.WithWaitForCallbackTimeout` and the SDK's
`*operations.CallbackFailedError.Timeout` field. Mirrors the JS
reference SDK's own `wait-for-callback/timeout` example.

## What it demonstrates

The submitter succeeds immediately, but no external system ever
completes the callback. `WithWaitForCallbackTimeout` configures the
backend to unilaterally fail the callback after the configured
duration elapses. The handler checks `Timeout` on the resulting
`*operations.CallbackFailedError` via `errors.As` to distinguish this
case from an explicit external failure.

Unlike the JS reference SDK — which exposes three distinct,
`instanceof`-able error types (`CallbackError`/`CallbackSubmitterError`/
`CallbackTimeoutError`) — this Go SDK surfaces every callback failure
through the same `*CallbackFailedError` type, with `Timeout` as a
plain `bool` field. There is no Go port of the JS reference SDK's
`wait-for-callback/error-instance-*` examples for exactly this reason:
this Go SDK has no equivalent set of distinct error types to check.

## A real local-testing limitation

The actual timeout path (nothing ever resolves the callback, and the
*backend itself* times it out) can only be exercised against a real
deployment — `testing.LocalTestRunner`'s fake in-memory client has no
mechanism to simulate a backend-driven timeout; it only supports
explicit `SendCallbackSuccess`/`SendCallbackFailure`, both of which map
to a different operation status than a real timeout does. See
`handler_test.go`'s own top-level doc comment for the full explanation.
This test instead exercises the distinct, locally-reachable
*explicit-failure* path and confirms `Timeout` is correctly `false` for
it — proving the field reflects the real checkpointed status rather
than being hard-coded.

## Local testing

```bash
go test ./...
```

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
