# DAG (Go) — Implementation Status

Worktree: `/Users/parpooya/workplace/go-dag`, branch `feature/dag-go`.
Module: `github.com/aws/aws-durable-execution-sdk-go`. Base: firstcut/b layered `pkg/durable/...`.

## Summary

All 12 ordered tasks are implemented, building, and tested. Phase 0 (the 3
gating base-SDK additive extensions) is complete. Every exported DAG symbol
carries an `// Experimental:` doc-comment paragraph.

Verification (whole module): `go build ./...` ✓, `go vet ./...` ✓,
`go test -race ./pkg/...` ✓. Example module `examples/dag-go`:
`go build ./...` ✓, `go test ./...` ✓, `go vet ./...` ✓.
(`golangci-lint` not installed in this environment; `go vet` is clean.)

## Progress log

- [x] Phase 0 Task 1 — Name-based task-ID seam (`pkg/durable/context/dag_seam.go`, `dcontext_test.go`)
- [x] Phase 0 Task 2 — Custom completion predicate shared types (`pkg/durable/types/completion.go`)
- [x] Phase 0 Task 3 — Completion-reason supersets (`pkg/durable/operations/batch.go`)

**PHASE 0 COMPLETE** (all 3 gating base-SDK extensions build + test green).

- [x] Task 4 — Public types, enums & error values (`trigger.go`, `result.go`, `completion.go`, `errors.go`)
- [x] Task 5 — Free-function registration + `TaskHandle[T]` + `Deps`/`Get[T]` (`dag.go`, `handle.go`, `deps.go`, `registration.go`)
- [x] Task 6 — Validator (`validate.go`, `validate_test.go`)
- [x] Task 7 — Goroutine-based scheduler (`scheduler.go`, `scheduler_test.go`) — `-race` clean
- [x] Task 8 — `DagResult` + serdes (lazy `json.RawMessage`, error round-trip, nested recursion) (`result.go`, `serdes.go`, `result_test.go`)
- [x] Task 9 — Wire `Dag()` to base context (`dag_run.go`; name-based child contexts + suspend protocol; `operations/suspend_export.go`)
- [x] Task 10 — Unit tests: handles, deps, entity IDs, trigger truth table (`handle_test.go`)
- [x] Task 11 — Runner integration + replay tests incl. wait suspend/resume & custom completion (`dag_replay_test.go`)
- [x] Task 12 — Package doc (`doc.go`), example (`examples/dag-go/`)

## Architecture notes / decisions

- **Name-based IDs, no core edit.** Task seam layers on `NewChildWithName` +
  `HashOperationID(TaskEntityID(prefix,name))`; the ID counter is untouched.
  Each task runs under a name-derived child context so its operation ID is
  deterministic and order-independent (§4 Route A).
- **Re-execution replay model** (§7, §8.1): the aggregate `DagResult` is
  recomputed by re-running `register` + the scheduler each replay; each task
  hits its own per-op checkpoint fast-path. Internal serialize/restore
  helpers exist for future offload/inspection but the control-flow path is
  re-execution (they are unexported until actually wired).
- **Scheduler is injected with hooks** (`schedHooks`) so scheduling logic is
  unit-testable with a fake runner while the real wiring plugs in execmgr's
  Register/Deregister/Suspended and `operations.IsSuspended`.
- **Suspend protocol**: mirrors the base batch scheduler's parent-park +
  register hand-off, generalized to a streaming completion loop. Completions
  are delivered into a mutex-guarded pending queue (not a separate channel),
  so the main loop's park decision and a worker's hand-off decision are
  serialized by a single lock — the active count can never spuriously reach
  zero while a completion is pending. Verified by a Wait-task suspend/resume
  test under the SkipTime local runner and a high-iteration no-spurious-
  suspension test against a faithful execmgr model.
- **Additive base-SDK export**: `operations.ErrSuspended` / `IsSuspended`
  (reuse-enabling; lets the DAG recognize the suspend sentinel).
- **Nested DAG** registered via `SubDag` (named to avoid clashing with the
  top-level `Dag` entry function).

## Known limitations / follow-ups

- Task-level operation options threaded where each base op supports them
  (Step/Callback/Condition retry+serdes, Invoke/Child serdes,
  Map/Parallel maxConcurrency). Other per-kind knobs can be added as needed.
- Deps key-membership is not compile-time checked (documented §2.5 gap):
  `Get[T]` returns `ErrDepNotAvailable` at run time for a non-dep handle.
- `golangci-lint` was unavailable in this environment; `go vet` is clean.
