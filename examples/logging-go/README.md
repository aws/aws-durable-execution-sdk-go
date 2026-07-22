# logging-go

Demonstrates replay-aware, contextual logging
(`pkg/durable/utils/logger.go`'s `ContextLogger`,
`docs/remaining-work.md` §5 tasks 13/14): a handler that logs at
various points — before, during, and after two sequential steps —
using both `dc.Logger()` (suppressible on replay) and `sc.Logger()`
(never suppressed, since a `StepContext` only ever exists during real
execution).

## What it demonstrates

The handler generates a two-step report (`gather-data` then
`summarize-data`), logging around and inside each step. Configured
with `types.LoggerConfig{ModeAware: true}` (see `main.go`), a replay
of a fully-completed execution suppresses **every** log call the
handler makes — not just calls positioned before the steps — because
there is no "next incomplete operation" left to reach at all, matching
the official SDK reference's documented suppression semantics.

## Local testing

```bash
go test ./...
```

- `handler_test.go`'s `TestHandler_GeneratesReport` is the ordinary
  `LocalTestRunner`-based example test every example in this repo has
  (result + operation assertions + event-history golden file).
- `loggingconfig_test.go` constructs `durable.WithDurableExecution`
  **directly** (bypassing `LocalTestRunner`, which has no hook today
  for injecting a custom `types.LoggerConfig`) against a small local
  `fakeClient` and a `recordingLogger` test double (mirroring
  `pkg/durable/durable_fake_client_test.go` and
  `pkg/durable/durable_logging_test.go`'s internal test doubles of the
  same names, not importable from an example module), driving two
  real, separate invocations of the same execution:
  - `TestLogging_ReplaySkipSuppressesThroughCompletedSteps` — the
    first invocation records all 8 log calls the handler makes; the
    second (a full replay, since both steps are already checkpointed)
    records **zero** — every call is suppressed.
  - `TestLogging_ModeAwareFalseNeverSuppresses` — the same two-invocation
    scenario with `ModeAware: false`, confirming suppression is
    genuinely opt-in: all 8 calls are recorded on both invocations.

This is the local half of the completion criteria described in
`docs/remaining-work.md`; a cloud-runner counterpart does not exist
yet (see that document's task 17a).

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
