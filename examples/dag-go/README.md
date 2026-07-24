# dag-go — EXPERIMENTAL DAG example

Demonstrates the experimental `pkg/durable/dag` package:

- **Diamond**: `fetch -> {double, triple} -> merge` using typed dependencies
  via `dag.Get[T]`.
- **Compensation with trigger rules**: `charge` then `fulfill`
  (`ALL_SUCCESS`, default), `refund` (`ALL_FAILED`), and `notify`
  (`ALL_DONE`).

> **Experimental.** The DAG API may change or be removed in a future release.

## Run

```bash
go test ./...
```

`handler_test.go` exercises both the success path (charge succeeds, refund
skipped) and the compensation path (charge fails → refund runs, reason
`COMPLETED_WITH_FAILURES`) against the SDK's `LocalTestRunner`.

## Key API notes

- Registration uses **free functions** (`dag.Step`, `dag.Invoke[In,Out]`,
  `dag.Callback[T]`, `dag.Wait`, `dag.WaitForCondition[S]`, `dag.Child`,
  `dag.Map[In,Out]`, `dag.Parallel[Out]`, `dag.SubDag`) — Go methods cannot
  be generic.
- `dag.Invoke` and `dag.Callback` **require explicit type arguments** (their
  result type appears only in the return).
- Task failures are reported inside the result (`res.Err()`), while
  registration/validation errors are the `error` return of `dag.Dag(...)`.
