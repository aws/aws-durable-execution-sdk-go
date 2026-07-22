# wait-for-callback-nested-go

Demonstrates `operations.WaitForCallback` composed with
`operations.RunInChildContext` — a callback at the top level, and a
second, independent callback inside a child context. Consolidates the
JS reference SDK's own `wait-for-callback/child-context` and
`wait-for-callback/nested` examples into one Go example, since both
demonstrate the same underlying composition (`WaitForCallback` nested
inside `RunInChildContext`) at different nesting depths — this Go SDK
has no distinct mechanism the JS examples' extra nesting level would
exercise that a single child-context level doesn't already cover.

## What it demonstrates

1. `WaitForCallback` at the top level (`parent-callback`)
2. A `RunInChildContext` wrapping a `Wait` followed by a SECOND,
   independent `WaitForCallback` (`child-callback`) — each callback
   has its own isolated checkpoint namespace and must be resolved
   separately

## Local testing

```bash
go test ./...
```

This test drives **three** invocations: the first suspends on the
parent callback, `SendCallbackSuccess` resolves it; the second
invocation proceeds into the child context and suspends on the child
callback (found via `GetOperationRecursive`, since it's nested inside
a child context), `SendCallbackSuccess` resolves it; the third
invocation completes.

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
