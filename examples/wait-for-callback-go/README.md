# wait-for-callback-go

Demonstrates `operations.WaitForCallback`: a human-in-the-loop
expense-approval workflow that suspends the durable execution (no
compute charges) until an external system — here, a simulated
manager approval — calls back with a result.

## What it demonstrates

The handler calls `operations.WaitForCallback[string]` with a
submitter function that hands a callback ID off to an external system
(in this example, just logged; in production this would email an
approver or register a webhook). This checkpoints, in order, matching
the confirmed real-backend flowchart:

1. `CONTEXT/WAIT_FOR_CALLBACK START` — the wrapping child context
2. `CALLBACK/CALLBACK START` — a fresh callback ID
3. `STEP/STEP START` — the submitter step (gets `Step`'s retry
   behavior for free)
4. The execution suspends, returning `PENDING`
5. Once `SendDurableExecutionCallbackSuccess`/`Failure` is called
   externally, the next invocation observes the resolved callback and
   completes: `CONTEXT/WAIT_FOR_CALLBACK SUCCEED`/`FAILED`

## Local testing

Because this operation genuinely suspends across two invocations,
the test drives both halves explicitly using
`LocalTestRunner.Continue` and `Operation.SendCallbackSuccess`/
`SendCallbackFailure`, rather than the single-call `Run` used by
`examples/simple-step-go` and `examples/run-in-child-context-go`:

```bash
go test ./...
```

`TestHandler_ApprovedExpense` drives the happy path (first invocation
suspends PENDING, `SendCallbackSuccess` resolves it, second invocation
succeeds). `TestHandler_RejectedExpense` drives the failure path with
`SendCallbackFailure`.

This is the local half of the completion criteria described in
`docs/remaining-work.md`; a cloud-runner counterpart does not exist
yet (see that document's task 17a).

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
