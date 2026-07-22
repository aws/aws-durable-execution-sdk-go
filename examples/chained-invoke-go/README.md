# chained-invoke-go

Demonstrates `operations.Invoke`: calling another Lambda function
(durable or standard) from within a durable execution and durably
awaiting its result.

## What it demonstrates

The handler calls a (mocked, in this example) inventory-check service
via `operations.Invoke[InventoryCheckRequest, InventoryCheckResponse]`.
Per the confirmed real-backend flowchart (internal SDK Operation
Diagrams design doc, "Invoke"), this checkpoints exactly once —
`CHAINED_INVOKE/CHAINED_INVOKE START` — and the backend itself is
responsible for calling the target function and reporting the outcome
back; the SDK's job is only to register the request and await the
result (which can resolve as `Succeeded`, `Failed`, `TimedOut`, or
`Stopped`).

## Local testing

Because there is no second, real Lambda function deployed for this
example's target, the test registers a mock via
`testing.RegisterDurableFunction`, matching the official cross-SDK
Testing API Reference's documented "Register mock handlers for
invoke" pattern:

```bash
go test ./...
```

Three tests cover: inventory available, inventory unavailable (both
still `SUCCEEDED` executions — availability is business data, not an
error), and the target function itself erroring (which fails the
`CHAINED_INVOKE` operation and the execution).

This is the local half of the completion criteria described in
`docs/remaining-work.md`; a cloud-runner counterpart does not exist
yet, and would additionally require deploying a real second Lambda
function as the invoke target (see that document's task 17a).

## Deploying

This example's own function can be built and deployed exactly like
`examples/simple-step-go` (see that example's README for the full
container-image deployment steps), but doing so usefully requires
also deploying a real `inventory-check` target function — not done in
this session's work. `InventoryCheckFunctionARN` in `handler.go` would
need to point at that real function's ARN.
