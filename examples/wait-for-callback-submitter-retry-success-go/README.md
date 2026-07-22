# wait-for-callback-submitter-retry-success-go

Demonstrates `operations.WithWaitForCallbackSubmitterRetryStrategy` on
the *success* path: the submitter fails on its first few attempts,
then succeeds — unlike `examples/wait-for-callback-failing-submitter-go`
(whose submitter always fails, exhausting every retry). Mirrors the JS
reference SDK's own `wait-for-callback/submitter-retry-success`
example (exponential backoff: 1s, 2s, 4s between attempts).

## Local testing

```bash
go test ./...
```

`TestHandler_SucceedsAfterTransientFailures` fails on attempts 1-2 and
succeeds on attempt 3. `TestHandler_SucceedsImmediatelyWithNoFailures`
confirms the zero-retry-needed case still works.

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
