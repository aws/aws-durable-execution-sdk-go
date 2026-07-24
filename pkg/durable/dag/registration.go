package dag

import "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"

// This file holds the free-function task-registration API. Registration
// functions are free (not methods) because Go methods cannot declare type
// parameters, and each mints a new result type T. The builder methods
// DependsOn/WithTrigger live on TaskHandle[T] (handle.go) since they do not
// introduce a new type parameter.

// Step registers a step task. Result type T is inferred from fn.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func Step[T any](d *Context, name string, deps []AnyHandle, fn StepFunc[T], opts ...Option) TaskHandle[T] {
	def, cfg := d.register(name, deps, kindPlain, opts)
	def.run = func(taskCtx DurableContext, dp Deps) (any, error) {
		var sopts []operations.StepOption[T]
		if cfg.retry != nil {
			sopts = append(sopts, operations.WithStepRetryStrategy[T](cfg.retry))
		}
		if cfg.serdes != nil {
			sopts = append(sopts, operations.WithStepSerdes[T](cfg.serdes))
		}
		return operations.Step(taskCtx, name, func(sc StepContext) (T, error) {
			return fn(dp, sc)
		}, sopts...)
	}
	return TaskHandle[T]{name: name, id: def.id, kind: kindPlain, def: def}
}

// Invoke registers a durable-invoke task. Because the result type Out
// appears only in the return, callers MUST supply explicit type args:
// dag.Invoke[InType, OutType](d, ...). (Verified against Go 1.25: return
// types do not participate in inference.)
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func Invoke[In, Out any](d *Context, name string, functionARN string, deps []AnyHandle, payload PayloadFunc[In], opts ...Option) TaskHandle[Out] {
	def, cfg := d.register(name, deps, kindPlain, opts)
	def.run = func(taskCtx DurableContext, dp Deps) (any, error) {
		in, err := payload(dp)
		if err != nil {
			var zero Out
			return zero, err
		}
		var iopts []operations.InvokeOption[In, Out]
		if cfg.serdes != nil {
			iopts = append(iopts, operations.WithInvokeSerdes[In, Out](cfg.serdes))
		}
		return operations.Invoke[In, Out](taskCtx, name, functionARN, in, iopts...)
	}
	return TaskHandle[Out]{name: name, id: def.id, kind: kindPlain, def: def}
}

// Callback registers a wait-for-callback task. Because the result type T
// appears only in the return, callers MUST supply an explicit type arg:
// dag.Callback[ResultType](d, ...).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func Callback[T any](d *Context, name string, deps []AnyHandle, submit SubmitterFunc, opts ...Option) TaskHandle[T] {
	def, cfg := d.register(name, deps, kindPlain, opts)
	def.run = func(taskCtx DurableContext, dp Deps) (any, error) {
		var copts []operations.WaitForCallbackOption[T]
		if cfg.timeout != nil {
			copts = append(copts, operations.WithWaitForCallbackTimeout[T](*cfg.timeout))
		}
		if cfg.retry != nil {
			copts = append(copts, operations.WithWaitForCallbackSubmitterRetryStrategy[T](cfg.retry))
		}
		if cfg.serdes != nil {
			copts = append(copts, operations.WithWaitForCallbackSerdes[T](cfg.serdes))
		}
		return operations.WaitForCallback[T](taskCtx, name, func(sc StepContext, callbackID string) error {
			return submit(dp, sc, callbackID)
		}, copts...)
	}
	return TaskHandle[T]{name: name, id: def.id, kind: kindPlain, def: def}
}

// Wait registers a wait task (no result value).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func Wait(d *Context, name string, deps []AnyHandle, duration Duration, opts ...Option) TaskHandle[Void] {
	def, _ := d.register(name, deps, kindPlain, opts)
	def.run = func(taskCtx DurableContext, dp Deps) (any, error) {
		if err := operations.Wait(taskCtx, name, duration); err != nil {
			return Void{}, err
		}
		return Void{}, nil
	}
	return TaskHandle[Void]{name: name, id: def.id, kind: kindPlain, def: def}
}

// WaitForCondition registers a polling task. Result type S is inferred from
// the check func and WithInitialState[S]. Supply WithInitialState and
// WithCondition via opts.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WaitForCondition[S any](d *Context, name string, deps []AnyHandle, check CheckFunc[S], opts ...Option) TaskHandle[S] {
	def, cfg := d.register(name, deps, kindPlain, opts)
	def.run = func(taskCtx DurableContext, dp Deps) (any, error) {
		var initial S
		if cfg.initialState != nil {
			if v, ok := cfg.initialState.(S); ok {
				initial = v
			}
		}
		var pred func(S) bool
		if cfg.conditionPred != nil {
			if p, ok := cfg.conditionPred.(func(S) bool); ok {
				pred = p
			}
		}
		var wopts []operations.ConditionOption[S]
		if cfg.retry != nil {
			wopts = append(wopts, operations.WithConditionRetryStrategy[S](cfg.retry))
		}
		if cfg.serdes != nil {
			wopts = append(wopts, operations.WithConditionSerdes[S](cfg.serdes))
		}
		return operations.WaitForCondition(taskCtx, name, func(sc StepContext, state S) (operations.ConditionResult[S], error) {
			next, err := check(dp, state, sc)
			if err != nil {
				return operations.ConditionResult[S]{}, err
			}
			done := pred == nil || pred(next)
			return operations.ConditionResult[S]{State: next, ConditionMet: done}, nil
		}, initial, wopts...)
	}
	return TaskHandle[S]{name: name, id: def.id, kind: kindPlain, def: def}
}

// Child registers a run-in-child-context task. Result type T is inferred
// from fn.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func Child[T any](d *Context, name string, deps []AnyHandle, fn ChildFunc[T], opts ...Option) TaskHandle[T] {
	def, cfg := d.register(name, deps, kindPlain, opts)
	def.run = func(taskCtx DurableContext, dp Deps) (any, error) {
		var copts []operations.ChildOption[T]
		if cfg.serdes != nil {
			copts = append(copts, operations.WithChildSerdes[T](cfg.serdes))
		}
		return operations.RunInChildContext(taskCtx, name, func(cc DurableContext) (T, error) {
			return fn(dp, cc)
		}, copts...)
	}
	return TaskHandle[T]{name: name, id: def.id, kind: kindPlain, def: def}
}

// Map registers a map task over items produced from deps. Result type is
// inferred from mapFn.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func Map[In, Out any](d *Context, name string, deps []AnyHandle, items ItemsFunc[In], mapFn MapFunc[In, Out], opts ...Option) TaskHandle[BatchResult[Out]] {
	def, cfg := d.register(name, deps, kindBatch, opts)
	def.run = func(taskCtx DurableContext, dp Deps) (any, error) {
		in := items(dp)
		var mopts []operations.MapOption[In, Out]
		if cfg.maxConcurrency != nil {
			mopts = append(mopts, operations.WithMapMaxConcurrency[In, Out](*cfg.maxConcurrency))
		}
		return operations.Map(taskCtx, name, in, func(cc DurableContext, item In, index int) (Out, error) {
			return mapFn(cc, item, index)
		}, mopts...)
	}
	return TaskHandle[BatchResult[Out]]{name: name, id: def.id, kind: kindBatch, def: def}
}

// Parallel registers a parallel-branches task. Result type is inferred from
// []Branch[Out].
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func Parallel[Out any](d *Context, name string, deps []AnyHandle, branches []Branch[Out], opts ...Option) TaskHandle[BatchResult[Out]] {
	def, cfg := d.register(name, deps, kindBatch, opts)
	def.run = func(taskCtx DurableContext, dp Deps) (any, error) {
		fns := make([]func(child DurableContext) (Out, error), len(branches))
		for i, b := range branches {
			fns[i] = b.Func
		}
		var popts []operations.ParallelOption[Out]
		if cfg.maxConcurrency != nil {
			popts = append(popts, operations.WithParallelMaxConcurrency[Out](*cfg.maxConcurrency))
		}
		return operations.Parallel(taskCtx, name, fns, popts...)
	}
	return TaskHandle[BatchResult[Out]]{name: name, id: def.id, kind: kindBatch, def: def}
}

// SubDag registers a nested DAG as a task. (Named SubDag rather than Dag to
// avoid clashing with the top-level Dag entry function; see DAG_SPEC_GO.md
// §2.3's nested dag.Dag.) Result type is *DagResult.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func SubDag(d *Context, name string, deps []AnyHandle, register func(sub *Context), opts ...Option) TaskHandle[*DagResult] {
	def, _ := d.register(name, deps, kindDag, opts)
	def.run = func(taskCtx DurableContext, dp Deps) (any, error) {
		return Dag(taskCtx, name, register, opts...)
	}
	return TaskHandle[*DagResult]{name: name, id: def.id, kind: kindDag, def: def}
}
