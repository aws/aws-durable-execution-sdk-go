# wait-for-callback-multiple-invocations-go

Demonstrates that this SDK's checkpoint/replay tracking correctly
resumes across MANY independent suspend/resume cycles within a single
handler — two `Wait`s, two `WaitForCallback`s, and a `Step`,
interleaved. Mirrors the JS reference SDK's own
`wait-for-callback/multiple-invocations` example.

## Local testing

```bash
go test ./...
```

Drives **three** invocations: the first resolves both `Wait`s
synchronously (via `SkipTime`) and suspends on the first callback; the
second resumes past it, runs the `Step` and second `Wait`, and
suspends on the second callback; the third completes.

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
