// Package dag implements an EXPERIMENTAL directed-acyclic-graph (DAG)
// orchestration layer on top of the durable-execution SDK's core
// operations. A handler declares a graph of named tasks with dependencies,
// trigger rules, conditional execution (runIf), and threshold/custom
// completion, then runs them with bounded concurrency while preserving the
// SDK's replay/checkpoint guarantees.
//
// # Stability
//
// This feature is EXPERIMENTAL and may be changed or removed in future
// releases without a major-version bump. Every exported symbol carries an
// "Experimental:" doc-comment paragraph.
//
// # Free-function API
//
// Because Go methods cannot declare type parameters, task registration uses
// free functions rather than methods on the registration context:
//
//	res, err := dag.Dag(dc, "etl", func(d *dag.Context) {
//	    fetch := dag.Step(d, "fetch", nil,
//	        func(_ dag.Deps, s dag.StepContext) (Source, error) { return fetchSource() })
//	    a := dag.Step(d, "a", []dag.AnyHandle{fetch},
//	        func(deps dag.Deps, s dag.StepContext) (A, error) {
//	            src, _ := dag.Get(deps, fetch) // typed: src is Source
//	            return transformA(src)
//	        })
//	    dag.Step(d, "merge", []dag.AnyHandle{a},
//	        func(deps dag.Deps, s dag.StepContext) (Out, error) { ... })
//	})
//	if err != nil { return err }              // registration/validation error
//	if err := res.Err(); err != nil { return err } // >=1 task FAILED
//
// The builder methods DependsOn and WithTrigger are methods on
// TaskHandle[T] (they introduce no new type parameter) and chain:
// h.DependsOn(x).WithTrigger(dag.AllDone).
//
// # Explicit type-argument divergence
//
// Result-type inference works for Step, WaitForCondition, Child, Map, and
// Parallel. Two kinds REQUIRE explicit type arguments because their result
// type appears only in the return (Go's inference does not use return
// types): dag.Invoke[In, Out](...) and dag.Callback[T](...).
//
// # Dependencies and results
//
// dag.Get[T](deps, handle) returns the upstream's typed value (no manual
// assertion). Key membership is NOT compile-time checked (unlike the JS
// DepsMap); a handle not in the task's deps yields ErrDepNotAvailable at
// run time. dag.Result[T](res, handle) / dag.ResultByName[T](res, name)
// read a task's result from the aggregate DagResult.
//
// # Determinism
//
// The register callback must be deterministic (same names, deps, rules
// every replay). Put non-deterministic work inside tasks. Task IDs are
// name-derived ({dagScope}-DAG_NODE_T_{name}, hashed) so they are stable
// across replays and independent of scheduling/completion order; task names
// must match ^[a-zA-Z0-9_]+$ and must not contain the reserved substring
// "DAG_NODE_T_".
//
// See docs/DAG_SPEC_GO.md for the full design.
package dag
