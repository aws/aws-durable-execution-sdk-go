package durable

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// This file implements an EXPERIMENTAL directed-acyclic-graph (DAG)
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
// free functions rather than methods on the registration handle. The
// registration functions carry a "Dag" prefix (DagStep, DagInvoke, ...) so
// they do not collide with the core operation functions of the same base
// name (Step, Invoke, ...):
//
//	res, err := durable.Dag(ctx, "etl", func(d *durable.DagBuilder) {
//	    fetch := durable.DagStep(d, "fetch", nil,
//	        func(_ durable.Deps, s durable.StepContext) (Source, error) { return fetchSource() })
//	    a := durable.DagStep(d, "a", []durable.AnyHandle{fetch},
//	        func(deps durable.Deps, s durable.StepContext) (A, error) {
//	            src, _ := durable.Get(deps, fetch) // typed: src is Source
//	            return transformA(src)
//	        })
//	    durable.DagStep(d, "merge", []durable.AnyHandle{a},
//	        func(deps durable.Deps, s durable.StepContext) (Out, error) { ... })
//	})
//	if err != nil { return err }              // registration/validation error
//	if err := res.ThrowIfError(); err != nil { return err } // >=1 task FAILED
//
// # Materialization
//
// Each DAG materializes a single scope CONTEXT operation (SubType "Dag").
// Each task's underlying operation (STEP/WAIT/CONTEXT/...) is then
// checkpointed DIRECTLY under that scope with a deterministic NAME-BASED id
// "{scopeId}-DAG_NODE_T_{name}" — there is no per-task container operation.
// The scope is checkpointed START-before-body (as RunInChildContext and
// Map/Parallel do) so every task op always has a valid, already-recorded
// ParentId (the scope). This flat model costs N+1 checkpoints for N tasks
// (one scope + one op per task) rather than the 2N+1 of a per-task-container
// design.

// operationSubTypeDag is the wire subtype for the DAG scope CONTEXT
// operation. Tasks are flat name-based ops under it (no task container).
const operationSubTypeDag = "Dag"

// dagTaskIDPrefix is the reserved id segment for a task's underlying
// operation: its full id is "{scopeId}-DAG_NODE_T_{name}". The
// "DAG_NODE_T_" substring is reserved in task-name validation so a task
// name can never collide with this scheme.
const dagTaskIDPrefix = "DAG_NODE_T_"

// Dag declares and runs a directed acyclic graph of tasks. The register
// callback builds the graph by calling the free registration functions
// (DagStep, DagInvoke, DagCallback, DagWait, DagWaitForCondition, DagChild,
// DagMap, DagParallel, SubDag). Dag returns (*DagResult, error):
//
//   - err != nil: a registration/validation/config failure (nothing was
//     scheduled), e.g. *DagValidationError, a cycle error, or a
//     *DagInvalidConfigError. On suspension it propagates the SDK's internal
//     suspend signal so the invocation pauses.
//   - err == nil: the DAG drained (or early-completed). Individual task
//     failures are reported INSIDE the result via res.ThrowIfError(),
//     mirroring the JS reject-vs-resolve split. The DAG container itself
//     never fails.
//
// Each task's underlying operation runs directly under the DAG scope with a
// deterministic name-based id, so task operation IDs are stable across
// replays; the aggregate DagResult is reconstructed by re-execution on
// replay while each task hits its own per-operation checkpoint fast-path.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func Dag(ctx Context, name string, register func(d *DagBuilder), opts ...DagOption) (*DagResult, error) {
	ec, ok := ctx.(*execContext)
	if !ok {
		return nil, fmt.Errorf("durable: Dag %q: Context was not created by the SDK", name)
	}
	cfg := buildDagConfig(opts)

	d := newDagBuilder()
	d.defaultRetry = cfg.defaultRetry
	d.defaultSerdes = cfg.dagSerdes
	if register != nil {
		register(d)
	}
	if err := validateDag(d, cfg); err != nil {
		return nil, err
	}

	maxConc := 0
	if cfg.maxConcurrency != nil {
		maxConc = *cfg.maxConcurrency
	}

	// Materialize the DAG scope as a CONTEXT op. The scope id is claimed
	// from the enclosing context's counter (one id per DAG); task ids are
	// minted under the scope, so the enclosing counter advances by exactly
	// one for the whole DAG.
	scopeID, err := ec.claimOperation()
	if err != nil {
		return nil, err
	}
	scopeEc, scopeTerminal, _, err := dagMaterializeChild(ec, scopeID, name, operationSubTypeDag)
	if err != nil {
		return nil, err
	}

	// Pre-claim nothing per task: task operations use NAME-BASED ids
	// ("{scopeId}-DAG_NODE_T_{name}") minted independently per task, so the
	// enclosing counter advances by exactly one (the scope) for the whole
	// DAG regardless of task count, and concurrent tasks share no mutable
	// id state.

	hooks := dagSchedHooks{
		isSuspend:   func(err error) bool { return errors.Is(err, errSuspendExecution) },
		suspendedCh: ec.suspend.done,
		runTask: func(def *dagTaskDef, deps Deps) (any, error) {
			// FLAT model: run the task's single underlying operation
			// DIRECTLY under the DAG scope with a name-based id; there is
			// no per-task container. childNamed captures THIS worker
			// goroutine as owner and derives the replay mode from the
			// underlying op's checkpoint, so a completed task fast-paths
			// and returns its correctly-typed result for DagResult
			// reconstruction.
			suffix := dagTaskIDPrefix + def.name
			taskCtx := scopeEc.childNamed(suffix)
			result, runErr := def.run(taskCtx, deps)
			// Suspension is not a task failure: propagate it unchanged so
			// the invocation ends PENDING and the task resumes later.
			if errors.Is(runErr, errSuspendExecution) {
				return result, runErr
			}
			if runErr != nil {
				return result, &DagTaskFailedError{Name: def.name, TaskID: scopeEc.ids.formatSuffix(suffix), Err: runErr}
			}
			return result, nil
		},
	}

	s := newDagScheduler(d.tasks, maxConc, cfg.completion, hooks)
	s.defaultTrigger = cfg.defaultTrigger
	execs, reason, suspended := s.run(ec)
	if suspended {
		return nil, errSuspendExecution
	}

	// The graph drained: finish the DAG's own container CONTEXT op
	// (CONTEXT/SUCCEED, ReplayChildren) unless it was already terminal on a
	// prior invocation. The DAG never fails at the container level.
	if !scopeTerminal {
		if err := dagFinishChild(ec, scopeID, name, operationSubTypeDag, nil); err != nil {
			return nil, err
		}
	}

	res := newDagResult(execs, reason)
	// total = number of REGISTERED tasks, not the settled count.
	res.total = len(d.tasks)
	if cfg.summaryGen != nil {
		res.summary = cfg.summaryGen(res)
	}
	return res, nil
}

// dagMaterializeChild claims the CONTEXT-op START for the DAG scope, whose
// positional id has already been claimed, mirroring the START-before-body
// recipe of RunInChildContext and the batch child items. It returns the
// scope execContext, whether the scope was already terminal on a prior
// invocation (caller must then skip the matching finish), and the
// checkpointed operation (nil on first execution). Tasks are flat name-based
// ops under the returned scope and do not use this helper.
func dagMaterializeChild(parent *execContext, id, name, subType string) (*execContext, bool, *operation, error) {
	op := parent.state.get(id)
	if err := validateReplayConsistency(op, string(types.OperationTypeContext), subType, name); err != nil {
		return nil, false, nil, err
	}
	if op != nil && op.status.terminal() {
		mode := childReplayMode(parent, id, op)
		child := parent.child(id, currentGoroutineOwner(), mode)
		return child, true, op, nil
	}
	if op == nil {
		update := dagContextUpdate(parent, id, name, subType, types.OperationActionStart)
		if err := parent.checkpointer.checkpoint(parent, []types.OperationUpdate{update}); err != nil {
			return nil, false, nil, err
		}
	}
	mode := childReplayMode(parent, id, op)
	child := parent.child(id, currentGoroutineOwner(), mode)
	return child, false, op, nil
}

// dagFinishChild checkpoints the terminal CONTEXT transition for a container
// created by dagMaterializeChild: CONTEXT/FAIL carrying opErr when non-nil,
// otherwise CONTEXT/SUCCEED with an empty payload and ReplayChildren=true
// (the DAG re-runs each task body on replay, so the container's real result
// lives in its nested child operations). parent is the enclosing context
// (the scope's parent for the scope, the scope for a task).
func dagFinishChild(parent *execContext, id, name, subType string, opErr error) error {
	update := dagContextUpdate(parent, id, name, subType, types.OperationActionSucceed)
	if opErr != nil {
		update.Action = types.OperationActionFail
		update.Error = errorObject(opErr)
	} else {
		update.ContextOptions = &types.ContextOptions{ReplayChildren: aws.Bool(true)}
	}
	return parent.checkpointer.checkpoint(parent, []types.OperationUpdate{update})
}

// dagContextUpdate assembles the shared fields of a DAG CONTEXT-op update.
// IDs are hashed to their wire form; ParentId is the enclosing context's
// prefix (already materialized).
func dagContextUpdate(ec *execContext, id, name, subType string, action types.OperationAction) types.OperationUpdate {
	update := types.OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    types.OperationTypeContext,
		SubType: aws.String(subType),
		Action:  action,
	}
	if name != "" {
		update.Name = aws.String(name)
	}
	if parent := ec.ids.prefix; parent != "" {
		update.ParentId = aws.String(hashID(parent))
	}
	return update
}
