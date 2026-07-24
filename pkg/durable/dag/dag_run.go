package dag

import (
	"context"

	dcontext "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/context"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
)

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
//     failures are reported INSIDE the result via res.Err(), mirroring the
//     JS reject-vs-resolve split.
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

	dctx, isReal := dc.(*dcontext.Context)
	parentPrefix := ""
	if isReal {
		parentPrefix = dctx.Prefix()
	}

	d := newContext(parentPrefix)
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

	if isReal {
		// Name-based scope for this DAG's task IDs, folding the DAG name in
		// so sibling DAGs in the same parent don't collide and nested DAGs
		// recurse.
		dagScope := dctx.NewNamedChild(
			dcontext.HashOperationID(dcontext.TaskEntityID(parentPrefix, name)), name,
		)
		em := dctx.ExecManager()
		hooks = schedHooks{
			runTask: func(def *taskDef, deps Deps) (any, error) {
				taskID := dcontext.HashOperationID(dcontext.TaskEntityID(dagScope.Prefix(), def.name))
				taskChild := dagScope.NewNamedChild(taskID, def.name)
				return def.run(taskChild, deps)
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
	execs, reason, suspended := s.run(sctx)
	if suspended {
		return nil, operations.ErrSuspended
	}

	res := newDagResult(execs, reason)
	if cfg.summaryGen != nil {
		res.summary = cfg.summaryGen(res)
	}
	return res, nil
}
