package durable

import (
	"fmt"
	"time"
)

// This file holds the free-function DAG task-registration API. Registration
// functions are free (not methods) because Go methods cannot declare type
// parameters, and each mints a new result type T. The builder methods
// After/WithTrigger live on [TaskHandle] (dag_handle.go). Each function is
// named with a "Dag" prefix so it does not collide with the core operation
// function of the same base name (e.g. [Step], [Map]).

// DagStep registers a step task. Result type T is inferred from fn.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func DagStep[T any](d *DagBuilder, name string, deps []AnyHandle, fn DagStepFunc[T], opts ...DagOption) TaskHandle[T] {
	def, cfg := d.register(name, deps, dagKindPlain, opStep, opts)
	def.run = func(taskCtx Context, dp Deps) (any, error) {
		var sopts []StepOption
		if cfg.retry != nil {
			sopts = append(sopts, WithRetry(cfg.retry))
		}
		if cfg.serdes != nil {
			sopts = append(sopts, WithStepSerdes(cfg.serdes))
		}
		return Step(taskCtx, name, func(sc StepContext) (T, error) {
			return fn(dp, sc)
		}, sopts...)
	}
	return TaskHandle[T]{name: name, id: def.id, kind: dagKindPlain, def: def}
}

// DagInvoke registers a durable-invoke task. Because the result type Out
// appears only in the return, callers MUST supply explicit type args:
// DagInvoke[InType, OutType](d, ...).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func DagInvoke[In, Out any](d *DagBuilder, name string, functionARN string, deps []AnyHandle, payload DagPayloadFunc[In], opts ...DagOption) TaskHandle[Out] {
	def, cfg := d.register(name, deps, dagKindPlain, opInvoke, opts)
	def.run = func(taskCtx Context, dp Deps) (any, error) {
		in, err := payload(dp)
		if err != nil {
			var zero Out
			return zero, err
		}
		var iopts []InvokeOption
		if cfg.serdes != nil {
			iopts = append(iopts, WithInvokeResultSerdes(cfg.serdes))
		}
		return Invoke[Out, In](taskCtx, name, functionARN, in, iopts...)
	}
	return TaskHandle[Out]{name: name, id: def.id, kind: dagKindPlain, def: def}
}

// DagCallback registers a wait-for-callback task. Because the result type T
// appears only in the return, callers MUST supply an explicit type arg:
// DagCallback[ResultType](d, ...).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func DagCallback[T any](d *DagBuilder, name string, deps []AnyHandle, submit DagSubmitterFunc, opts ...DagOption) TaskHandle[T] {
	def, cfg := d.register(name, deps, dagKindPlain, opCallback, opts)
	def.run = func(taskCtx Context, dp Deps) (any, error) {
		var copts []CallbackOption
		if cfg.timeout != nil {
			copts = append(copts, WithCallbackTimeout(*cfg.timeout))
		}
		if cfg.retry != nil {
			copts = append(copts, WithSubmitterRetry(cfg.retry))
		}
		return WaitForCallback[T](taskCtx, name, func(sc StepContext, callbackID string) error {
			return submit(dp, sc, callbackID)
		}, copts...)
	}
	return TaskHandle[T]{name: name, id: def.id, kind: dagKindPlain, def: def}
}

// DagWait registers a wait task (no result value).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func DagWait(d *DagBuilder, name string, deps []AnyHandle, duration time.Duration, opts ...DagOption) TaskHandle[Void] {
	def, _ := d.register(name, deps, dagKindPlain, opWait, opts)
	def.run = func(taskCtx Context, dp Deps) (any, error) {
		if err := Wait(taskCtx, name, duration); err != nil {
			return Void{}, err
		}
		return Void{}, nil
	}
	return TaskHandle[Void]{name: name, id: def.id, kind: dagKindPlain, def: def}
}

// DagWaitForCondition registers a polling task. Result type S is inferred
// from the positional initial state and the check func. The completion
// predicate is supplied via [WithCondition] and is REQUIRED.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func DagWaitForCondition[S any](d *DagBuilder, name string, deps []AnyHandle, initial S, check DagCheckFunc[S], opts ...DagOption) TaskHandle[S] {
	def, cfg := d.register(name, deps, dagKindPlain, opConditionOp, opts)
	if cfg.conditionPred == nil {
		d.regErrs = append(d.regErrs, &DagInvalidConfigError{
			Reason: fmt.Sprintf("WaitForCondition task %q: WithCondition is required", name),
		})
	}
	def.run = func(taskCtx Context, dp Deps) (any, error) {
		var pred func(S) bool
		if cfg.conditionPred != nil {
			if p, ok := cfg.conditionPred.(func(S) bool); ok {
				pred = p
			}
		}
		retry := cfg.retry
		cc := ConditionConfig[S]{
			InitialState: initial,
			WaitStrategy: func(state S, attempt int) WaitDecision {
				if pred != nil && pred(state) {
					return WaitDecision{Continue: false}
				}
				delay := time.Second
				if retry != nil {
					dec := retry(nil, attempt)
					if !dec.Retry {
						return WaitDecision{Continue: false, Err: fmt.Errorf("durable: dag condition %q: retries exhausted after %d attempts", name, attempt)}
					}
					delay = dec.Delay
				}
				return WaitDecision{Continue: true, Delay: delay}
			},
		}
		if cfg.serdes != nil {
			cc.Serdes = cfg.serdes
		}
		return WaitForCondition(taskCtx, name, func(sc StepContext, state S) (S, error) {
			return check(dp, state, sc)
		}, cc)
	}
	return TaskHandle[S]{name: name, id: def.id, kind: dagKindPlain, def: def}
}

// DagChild registers a run-in-child-context task. Result type T is inferred
// from fn.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func DagChild[T any](d *DagBuilder, name string, deps []AnyHandle, fn DagChildFunc[T], opts ...DagOption) TaskHandle[T] {
	def, cfg := d.register(name, deps, dagKindPlain, opChild, opts)
	def.run = func(taskCtx Context, dp Deps) (any, error) {
		var copts []ChildOption
		if cfg.serdes != nil {
			copts = append(copts, WithChildSerdes(cfg.serdes))
		}
		return RunInChildContext(taskCtx, name, func(cc Context) (T, error) {
			return fn(dp, cc)
		}, copts...)
	}
	return TaskHandle[T]{name: name, id: def.id, kind: dagKindPlain, def: def}
}

// DagMap registers a map task over items produced from deps. Result type is
// inferred from mapFn.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func DagMap[In, Out any](d *DagBuilder, name string, deps []AnyHandle, items DagItemsFunc[In], mapFn DagMapFunc[In, Out], opts ...DagOption) TaskHandle[BatchResult[Out]] {
	def, cfg := d.register(name, deps, dagKindBatch, opMap, opts)
	def.run = func(taskCtx Context, dp Deps) (any, error) {
		in := items(dp)
		var mopts []BatchOption
		if cfg.batchMaxConcurrency != nil {
			mopts = append(mopts, WithMaxConcurrency(*cfg.batchMaxConcurrency))
		}
		return Map(taskCtx, name, in, func(cc Context, item In, index int) (Out, error) {
			return mapFn(cc, item, index)
		}, mopts...)
	}
	return TaskHandle[BatchResult[Out]]{name: name, id: def.id, kind: dagKindBatch, def: def}
}

// DagParallel registers a parallel-branches task. Result type is inferred
// from []Branch[Out].
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func DagParallel[Out any](d *DagBuilder, name string, deps []AnyHandle, branches []Branch[Out], opts ...DagOption) TaskHandle[BatchResult[Out]] {
	def, cfg := d.register(name, deps, dagKindBatch, opParallel, opts)
	def.run = func(taskCtx Context, dp Deps) (any, error) {
		var popts []BatchOption
		if cfg.batchMaxConcurrency != nil {
			popts = append(popts, WithMaxConcurrency(*cfg.batchMaxConcurrency))
		}
		return Parallel(taskCtx, name, branches, popts...)
	}
	return TaskHandle[BatchResult[Out]]{name: name, id: def.id, kind: dagKindBatch, def: def}
}

// SubDag registers a nested DAG as a task. The same opts slice is applied at
// BOTH levels: the task-level fields govern how this node participates in
// the parent DAG, and the DAG-level fields govern the nested DAG's own
// scheduling. Result type is *DagResult.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func SubDag(d *DagBuilder, name string, deps []AnyHandle, register func(sub *DagBuilder), opts ...DagOption) TaskHandle[*DagResult] {
	def, _ := d.register(name, deps, dagKindDag, opSubDag, opts)
	def.run = func(taskCtx Context, dp Deps) (any, error) {
		return Dag(taskCtx, name, register, opts...)
	}
	return TaskHandle[*DagResult]{name: name, id: def.id, kind: dagKindDag, def: def}
}
