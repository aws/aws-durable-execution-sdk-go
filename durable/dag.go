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
// Each DAG materializes a scope CONTEXT operation (SubType "Dag"), and each
// task materializes its own CONTEXT operation (SubType "DagTask") nested
// under the scope, whose inner operation (STEP/WAIT/...) then runs beneath
// it. The scope and each task are checkpointed START-before-body — the same
// pattern RunInChildContext and Map/Parallel use — so every nested
// operation always has a valid, already-recorded ParentId.

// Wire subtypes for DAG scope and task container CONTEXT operations.
const (
	operationSubTypeDag     = "Dag"
	operationSubTypeDagTask = "DagTask"
)

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
// Each task runs under a deterministic child context (the scope's positional
// child id) so task operation IDs are stable across replays; the aggregate
// DagResult is reconstructed by re-execution on replay while each task hits
// its own per-operation checkpoint fast-path.
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

	// Pre-claim task container ids in registration order (deterministic
	// across replays), on the owning goroutine, before any worker runs.
	taskIDs := make(map[string]string, len(d.tasks))
	for _, t := range d.tasks {
		taskIDs[t.name] = scopeEc.ids.next()
	}

	hooks := dagSchedHooks{
		isSuspend:   func(err error) bool { return errors.Is(err, errSuspendExecution) },
		suspendedCh: ec.suspend.done,
		runTask: func(def *dagTaskDef, deps Deps) (any, error) {
			taskID := taskIDs[def.name]
			// Materialize the task's own container CONTEXT op under the DAG
			// scope. def.run is ALWAYS executed — even when the container is
			// already terminal on replay — so the task's inner operation
			// fast-paths and returns its correctly-typed result for
			// DagResult reconstruction; taskTerminal only gates the terminal
			// CONTEXT checkpoint.
			taskChild, taskTerminal, _, cerr := dagMaterializeChild(scopeEc, taskID, def.name, operationSubTypeDagTask)
			if cerr != nil {
				return nil, cerr
			}
			result, runErr := def.run(taskChild, deps)
			// Suspension is not a task failure: propagate it unchanged so the
			// invocation ends PENDING and the task resumes later. Never
			// checkpoint a terminal transition and never wrap it.
			if errors.Is(runErr, errSuspendExecution) {
				return result, runErr
			}
			if !taskTerminal {
				if ferr := dagFinishChild(scopeEc, taskID, def.name, operationSubTypeDagTask, runErr); ferr != nil {
					return result, ferr
				}
			}
			if runErr != nil {
				return result, &DagTaskFailedError{Name: def.name, TaskID: taskID, Err: runErr}
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

// dagMaterializeChild claims the CONTEXT-op START for a DAG scope or task
// container whose positional id has already been claimed, mirroring the
// START-before-body recipe of RunInChildContext and the batch child items.
// It returns the child execContext, whether the container was already
// terminal on a prior invocation (caller must then skip the matching
// finish), and the checkpointed operation (nil on first execution).
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
