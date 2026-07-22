# create-callback-go

Demonstrates `operations.CreateCallback` — the low-level primitive
underlying `WaitForCallback` (see `examples/wait-for-callback-go` for
that higher-level, single-call wrapper).

## What it demonstrates

Unlike `WaitForCallback`, `CreateCallback` returns the callback ID and
a result channel **immediately**, without blocking — letting the
caller do other durable work before ever awaiting the result. This
example simulates an order-shipment workflow that:

1. Creates a callback for a third-party carrier's "package picked up"
   webhook (`CreateCallback`)
2. Hands the callback ID off to the carrier via a `Step`
   (`register-with-carrier`) — replay-safe, so the hand-off only
   really happens once even if the handler re-runs before the
   callback resolves
3. **While that callback is still outstanding**, runs a second,
   unrelated `Step` (`check-inventory`)
4. Only then blocks on the callback's result via `AwaitCallback` (never
   a raw channel receive — see `operations.CreateCallback`'s own doc
   comment for why that would be a real correctness bug, not just a
   style preference)

This mirrors the JS reference SDK's own `create-callback/mixed-ops`
example ("createCallback mixed with steps, waits, and other
operations") and `CreateCallback`'s own documented reason to exist
over `WaitForCallback`: "prefer `WaitForCallback` unless you need to
interleave callback registration with other durable operations before
suspending."

## Local testing

Like `examples/wait-for-callback-go`, this operation genuinely
suspends across two invocations, so the test drives both halves
explicitly using `LocalTestRunner.Continue` and
`Operation.SendCallbackSuccess`/`SendCallbackFailure`:

```bash
go test ./...
```

`TestHandler_SuccessfulPickup` drives the happy path, and also asserts
that both interleaved `Step`s (`register-with-carrier`,
`check-inventory`) already succeeded *before* the first invocation
suspends — proving `CreateCallback` genuinely let the handler keep
working instead of blocking immediately.
`TestHandler_FailedPickup` drives the failure path with
`SendCallbackFailure`.

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
