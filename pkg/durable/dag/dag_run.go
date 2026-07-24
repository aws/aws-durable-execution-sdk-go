package dag

import (
	"context"

	dcontext "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/context"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/execmgr"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
)

// dagRuntime is the minimal slice of the concrete base-SDK context that the
// DAG entry needs to derive name-based task IDs (Prefix/NewNamedChild) and
// coordinate suspension (ExecManager). Asserting against this behavioral
// interface rather than the concrete *dcontext.Context documents exactly
// what the DAG depends on and keeps the seam narrow; a context that does
// not satisfy it (e.g. a pure-logic/test stub) falls back to direct
// execution with no name-based IDs or suspend coordination.
type dagRuntime interface {
	Prefix() string
	NewNamedChild(entityID, name string) *dcontext.Context
	// NewMaterializedChild / FinishMaterializedChild materialize the DAG
	// SCOPE container as a durably-checkpointed CONTEXT operation so that
	// every task CONTEXT op nested under it (and, transitively, every task
	// STEP op) has a valid, already-recorded ParentID on the real backend
	// (see dag_seam.go). A bare NewNamedChild only builds the scope
	// in-memory, which the real CheckpointDurableExecution API rejects with
	// "Invalid parent operation id".
	NewMaterializedChild(entityID, name, subType string) (*dcontext.Context, bool, error)
	FinishMaterializedChild(entityID, name, subType string, opErr error) error
	ExecManager() *execmgr.Manager
}

// Dag declares and runs a directed acyclic graph of tasks. The register
// callback builds the graph by calling the free registration functions
// (Step, Invoke, Callback, Wait, WaitForCondition, Child, Map, Parallel,
// SubDag). Dag returns (*DagResult, error):
//
//   - err != nil: a registration/validation/config failure (nothing was
//     scheduled), e.g. *DagValidationError, a cycle error, or a
//     *DagInvalidConfigError. On suspension it returns the base SDK's
//     suspend sentinel (operations.ErrSuspended) so the invocation pauses.
//   - err == nil: the DAG drained (or early-completed). Individual task
//     failures are reported INSIDE the result via res.ThrowIfError(),
//     mirroring the JS reject-vs-resolve split.
//
// Each task runs under a deterministic, name-derived child context
// ({dagScope}-DAG_NODE_T_{taskName}, hashed) so task operation IDs are
// stable across replays and independent of scheduling/completion order; the
// aggregate DagResult is reconstructed by re-execution on replay while each
// task hits its own per-operation checkpoint fast-path.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func Dag(dc DurableContext, name string, register func(d *Context), opts ...Option) (*DagResult, error) {
	cfg := buildConfig(opts)

	dctx, isReal := dc.(dagRuntime)
	parentPrefix := ""
	if isReal {
		parentPrefix = dctx.Prefix()
	}

	d := newContext(parentPrefix)
	d.defaultRetry = cfg.defaultRetry
	if register != nil {
		register(d)
	}
	if err := validate(d, cfg); err != nil {
		return nil, err
	}

	maxConc := 0
	if cfg.maxConcurrency != nil {
		maxConc = *cfg.maxConcurrency
	}

	var hooks schedHooks
	sctx := context.Background()
	// scopeID/scopeDone track the DAG's own container CONTEXT op so it can
	// be finished (CONTEXT/SUCCEED) after the graph drains; empty scopeID
	// marks the non-runtime path (no scope materialized).
	var (
		dctxReal      dagRuntime
		scopeID       string
		scopeTerminal bool
	)

	if isReal {
		// Name-based scope for this DAG's task IDs, folding the DAG name in
		// so sibling DAGs in the same parent don't collide and nested DAGs
		// recurse. The scope is MATERIALIZED as a CONTEXT op (not just an
		// in-memory child) so every task CONTEXT op nested under it has a
		// valid, already-checkpointed ParentID on the real backend.
		scopeID = dcontext.HashOperationID(dcontext.TaskEntityID(parentPrefix, name))
		dagScope, alreadyTerminal, err := dctx.NewMaterializedChild(scopeID, name, dcontext.DagContextSubType)
		if err != nil {
			return nil, err
		}
		dctxReal = dctx
		scopeTerminal = alreadyTerminal
		em := dctx.ExecManager()
		hooks = schedHooks{
			runTask: func(def *taskDef, deps Deps) (any, error) {
				// Materialize this task's OWN container CONTEXT op under the
				// DAG scope (CONTEXT + inner STEP per task, mirroring a Map
				// item / RunInChildContext) so the task's STEP/START has a
				// valid parent. def.run is ALWAYS executed - even when the
				// container is already terminal on replay - so the task's
				// inner operation fast-paths and returns its correctly-typed
				// result for DagResult reconstruction; alreadyTerminal only
				// gates the terminal CONTEXT checkpoint.
				taskID := dcontext.HashOperationID(dcontext.TaskEntityID(dagScope.Prefix(), def.name))
				taskChild, taskTerminal, cerr := dagScope.NewMaterializedChild(taskID, def.name, dcontext.DagContextSubType)
				if cerr != nil {
					return nil, cerr
				}
				result, runErr := def.run(taskChild, deps)
				if taskTerminal || operations.IsSuspended(runErr) {
					// Container already terminal on record, or the body
					// merely paused: do not (re-)record a terminal CONTEXT
					// transition (would corrupt replay).
					return result, runErr
				}
				if ferr := dagScope.FinishMaterializedChild(taskID, def.name, dcontext.DagContextSubType, runErr); ferr != nil {
					return result, ferr
				}
				return result, runErr
			},
			isSuspend: operations.IsSuspended,
		}
		if em != nil {
			hooks.register = em.Register
			hooks.deregister = em.Deregister
			hooks.suspendedCh = em.Suspended
		}
		if c := dc.Context(); c != nil {
			sctx = c
		}
	} else {
		// Non-runtime context (e.g. pure-logic use): run task bodies
		// directly against dc with no suspend coordination.
		hooks = schedHooks{
			runTask: func(def *taskDef, deps Deps) (any, error) {
				return def.run(dc, deps)
			},
		}
	}

	s := newScheduler(d.tasks, maxConc, cfg.completion, hooks)
	s.defaultTrigger = cfg.defaultTrigger
	execs, reason, suspended := s.run(sctx)
	if suspended {
		return nil, operations.ErrSuspended
	}

	// The graph drained: finish the DAG's own container CONTEXT op
	// (CONTEXT/SUCCEED, ReplayChildren) so it is not left dangling in the
	// STARTED state. Skipped when the scope was already terminal on a prior
	// invocation (replay of a fully-completed DAG) or on the non-runtime
	// path (scopeID == ""). The DAG itself never fails at the container
	// level - individual task failures are reported inside DagResult - so
	// this is always a SUCCEED.
	if scopeID != "" && !scopeTerminal {
		if err := dctxReal.FinishMaterializedChild(scopeID, name, dcontext.DagContextSubType, nil); err != nil {
			return nil, err
		}
	}

	res := newDagResult(execs, reason)
	// Spec §2.8: total = number of REGISTERED tasks, not the settled count.
	// Under early completion never-started tasks are absent from execs but
	// still count toward total.
	res.total = len(d.tasks)
	if cfg.summaryGen != nil {
		res.summary = cfg.summaryGen(res)
	}
	return res, nil
}
