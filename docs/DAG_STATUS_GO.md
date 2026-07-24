# DAG (Go) — Implementation Status

Worktree: `/Users/parpooya/workplace/go-dag`, branch `feature/dag-go`.
Module: `github.com/aws/aws-durable-execution-sdk-go`. Base: firstcut/b layered `pkg/durable/...`.

## Progress log

- [x] Phase 0 Task 1 — Name-based task-ID seam (`pkg/durable/context`) — DONE, tested
- [x] Phase 0 Task 2 — Custom completion predicate types (`pkg/durable/types`) — DONE, tested
- [x] Phase 0 Task 3 — Completion-reason supersets (`pkg/durable/operations/batch.go`) — DONE, tested

**PHASE 0 COMPLETE** (all 3 gating base-SDK extensions build + test green; full `go test ./pkg/...` passes).
- [ ] Task 4 — DAG public types, enums, errors
- [ ] Task 5 — Free-function registration + TaskHandle[T] + Deps/Get[T]
- [ ] Task 6 — Validator
- [ ] Task 7 — Goroutine scheduler
- [ ] Task 8 — DagResult + serdes
- [ ] Task 9 — Wire Dag() entry to base context
- [ ] Task 10 — Unit tests (handles, deps, entity IDs, trigger)
- [ ] Task 11 — Runner integration + replay tests
- [ ] Task 12 — Docs, package doc, example

Verification bar per task: `go build ./...`, `go vet ./...`, `go test ./...` (with `-race` for scheduler).
