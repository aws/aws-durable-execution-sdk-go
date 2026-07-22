# wait-for-condition-go

Demonstrates `operations.WaitForCondition`: polling an external system
(here, simulated) until it reports a terminal status, without the
durable execution incurring compute charges between polls.

## What it demonstrates

The handler polls a simulated job-status check that reports "running"
for its first two polls and "completed" on the third. Per the
confirmed real-backend flowchart (internal SDK Operation Diagrams
design doc, "WaitForCondition"), this checkpoints, in order:

1. `STEP/WAIT_FOR_CONDITION START`
2. Runs the check function
3. If `ConditionMet: false` → `STEP/WAIT_FOR_CONDITION RETRY` with a
   backoff (structurally identical to `Step`'s own retry loop, just
   triggered by "condition not met" rather than an error) → poll again
4. If `ConditionMet: true` → `STEP/WAIT_FOR_CONDITION SUCCEED` with the
   final state

## Local testing

Because `LocalTestRunnerConfig.SkipTime` defaults to `true` in this Go
port, the simulated poll delays complete without real wall-clock
waiting, so a single `Run` call drives the whole poll loop to
completion:

```bash
go test ./...
```

The test asserts on the final result (job completed after exactly 3
polls) and on the `poll-job-status` `STEP` operation's type and
status.

This is the local half of the completion criteria described in
`docs/remaining-work.md`; a cloud-runner counterpart does not exist
yet (see that document's task 17a).

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
