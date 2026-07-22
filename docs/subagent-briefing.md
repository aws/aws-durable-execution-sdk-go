# Subagent briefing: aws-durable-execution-sdk-csharp, golang branch

You are implementing ONE task in a Go SDK for AWS Lambda Durable Execution Functions.
Repo: /tmp/aws-durable-execution-sdk-csharp, branch `golang`.

## Mandatory workflow for every task
1. Read `docs/remaining-work.md` in full first for context on what's already done and the project's conventions.
2. Read the actual existing code for anything adjacent to your task before writing new code (e.g. read `pkg/durable/operations/step.go` before touching retry logic). Never guess at existing types/signatures.
3. Implement the feature in the appropriate package.
4. If the task involves an operation-level feature, add or update a test that exercises it via `pkg/durable/testing.LocalTestRunner` where applicable, or a lower-level test using the `fakeClient` pattern (see `pkg/durable/durable_fake_client_test.go`) if you need to test suspend/resume across invocations directly.
5. Environment note: the Go module proxy (`proxy.golang.org`) is blocked on this network. ALWAYS prefix Go commands that might fetch dependencies with `GOPROXY=direct GOSUMDB=off`, e.g. `GOPROXY=direct GOSUMDB=off go build ./...`. Direct git/HTTPS access to github.com works fine. Cold fetches of large modules can take 30-45s - don't assume a short timeout means failure.
6. Run, in this order, and fix any failures before proceeding: `GOPROXY=direct GOSUMDB=off go build ./... && GOPROXY=direct GOSUMDB=off go vet ./... && gofmt -l .` (must produce no output) `&& GOPROXY=direct GOSUMDB=off go test ./... -race -count=5`. If your task touches concurrency-sensitive code (Map/Parallel/batch.go/execmgr), use `-count=20` or higher.
7. Also verify every example module still builds/tests cleanly if your change touches shared packages (`pkg/durable/operations`, `pkg/durable/types`, `pkg/durable/testing`, `pkg/durable/execmgr`, `pkg/durable/checkpoint`, `pkg/durable/context`):
   ```
   for d in examples/simple-step-go examples/run-in-child-context-go examples/wait-for-callback-go examples/wait-for-condition-go examples/chained-invoke-go examples/map-parallel-go; do
     (cd "$d" && GOPROXY=direct GOSUMDB=off go build ./... && GOPROXY=direct GOSUMDB=off go test ./... -race -count=3) || echo "FAILED: $d"
   done
   ```
8. Update `docs/remaining-work.md` for your specific task/section: strike through the completed task text with `~~...~~`, add "— **done this session.**" (or similar) explaining what was implemented, what was verified and how, and any bugs found and fixed. Follow the EXACT prose style already used for prior completed tasks in that file (see e.g. task 7's writeup) - detailed, specific, not vague. Update the corresponding row in whatever comparison table covers your feature, and update the "Suggested sequencing" list entry for your task number.
9. If you find a REAL bug (not just a style issue) while implementing your task - fix it at the root cause and document it in both your commit message and the remaining-work.md writeup, exactly like prior sessions have done repeatedly in this repo. Do not paper over bugs.
10. Stage ONLY the files relevant to your task (never `git add .` or `git add -A`) and commit with a detailed message covering what was implemented, what was verified, and any bugs found/fixed - matching the style of recent commits (run `git log --oneline -5` and `git log -1 --format=%B` to see the exact style expected). Do NOT push - the supervising process will push after review.
11. Do NOT modify files outside the scope of your assigned task. Do NOT touch `pkg/durable/awssdk`, `pkg/durable/awscli`, `pkg/durable/sigv4lambda` unless your task explicitly says to.
12. At the end, report back: what you implemented, what you verified (paste key test output), what bugs you found/fixed if any, and the commit hash you created.

## Core repo conventions (already established, follow them)
- Exact-pinned dependency versions only, never open ranges, if adding any new dependency.
- Every checkpointed operation follows the pattern: check `ExecManager().GetOperation(id)` for replay-skip first, then checkpoint START, run the work, checkpoint SUCCEED/FAIL (or RETRY for retry loops).
- `errSuspended` (in `pkg/durable/operations/errors.go`) is a sentinel meaning "this invocation is suspending, abandon this goroutine" - it must never be checkpointed as a real failure, and must propagate via `errors.Is(err, errSuspended)` checks unwrapped through any wrapping layer (RunInChildContext, runBatchItem, etc. already do this correctly - follow the same pattern for anything new).
- Comments should explain WHY, especially for anything non-obvious or where a bug was found - this codebase has a strong existing style of long, detailed doc comments explaining design decisions and past bugs. Match it.
- Never guess at real-backend behavior - if uncertain how the actual Lambda Durable Functions backend behaves, note the uncertainty explicitly in a comment rather than asserting something unverified as fact.
