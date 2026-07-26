package durable

import (
	"errors"
)

// All records a combinator operation and waits for every future to succeed,
// returning the values in input order. If any future fails, All fails
// immediately with a [*ChildContextError] wrapping the first failure.
//
// All uses [RunInChildContext] internally, so the aggregate result is
// checkpointed: on replay, the stored result is returned without
// re-awaiting the futures.
//
// Empty input returns an empty slice immediately (matching Promise.all([])).
func All[O any](ctx Context, name string, fs []*Future[O]) ([]O, error) {
	return RunInChildContext(ctx, name, func(_ Context) ([]O, error) {
		results := make([]O, len(fs))
		for i, f := range fs {
			val, err := f.Result()
			if err != nil {
				if errors.Is(err, errSuspendExecution) {
					return nil, err
				}
				return nil, err
			}
			results[i] = val
		}
		return results, nil
	})
}

// AllSettled records a combinator operation and waits for every future to
// settle, returning each outcome in input order regardless of success or
// failure.
//
// AllSettled uses [RunInChildContext] internally, so the aggregate result is
// checkpointed. On replay, the stored outcomes are returned without
// re-awaiting the futures.
//
// Empty input returns an empty slice immediately.
func AllSettled[O any](ctx Context, name string, fs []*Future[O]) ([]Settled[O], error) {
	return RunInChildContext(ctx, name, func(_ Context) ([]Settled[O], error) {
		results := make([]Settled[O], len(fs))
		for i, f := range fs {
			val, err := f.Result()
			if err != nil {
				if errors.Is(err, errSuspendExecution) {
					return nil, err
				}
				results[i] = Settled[O]{Err: err}
			} else {
				results[i] = Settled[O]{Value: val}
			}
		}
		return results, nil
	})
}

// Any records a combinator operation and returns the value of the first
// future to succeed. If all futures fail, Any returns a [*CombinatorError]
// wrapping all individual errors (analogous to JavaScript's AggregateError
// from Promise.any).
//
// Any uses [RunInChildContext] internally, so the winning result is
// checkpointed: on replay, the same winner is returned deterministically
// regardless of future settlement order.
//
// Empty input fails immediately with a [*CombinatorError] (no futures can
// succeed), matching Promise.any([]).
func Any[O any](ctx Context, name string, fs []*Future[O]) (O, error) {
	return RunInChildContext(ctx, name, func(_ Context) (O, error) {
		var zero O
		if len(fs) == 0 {
			return zero, &CombinatorError{Name: name, Errors: nil}
		}

		// Use a goroutine per future to detect the first success.
		// All futures are already durable (their operations were
		// claimed before the combinator), so this is a pure in-process
		// join — no new durable operations are created.
		type result struct {
			val O
			err error
			idx int
		}
		ch := make(chan result, len(fs))
		for i, f := range fs {
			go func(idx int, fut *Future[O]) {
				val, err := fut.Result()
				ch <- result{val: val, err: err, idx: idx}
			}(i, f)
		}

		errs := make([]error, len(fs))
		errCount := 0
		for range fs {
			r := <-ch
			if r.err == nil {
				return r.val, nil
			}
			if errors.Is(r.err, errSuspendExecution) {
				return zero, r.err
			}
			errs[r.idx] = r.err
			errCount++
		}
		// All failed.
		return zero, &CombinatorError{Name: name, Errors: errs}
	})
}

// Race records a combinator operation and returns the outcome of the first
// future to settle, whether it succeeded or failed.
//
// Race uses [RunInChildContext] internally, so the winner is checkpointed:
// on replay, the same outcome is returned deterministically regardless of
// future settlement order.
//
// Empty input suspends (no future will ever settle), matching
// Promise.race([]) which returns a forever-pending promise.
func Race[O any](ctx Context, name string, fs []*Future[O]) (O, error) {
	return RunInChildContext(ctx, name, func(childCtx Context) (O, error) {
		var zero O
		if len(fs) == 0 {
			// No futures to settle. Match Promise.race([]):
			// suspend cleanly since this will never resolve.
			ec, ok := childCtx.(*execContext)
			if ok {
				ec.blocked.Store(true)
				ec.suspend.commitPending()
			}
			return zero, errSuspendExecution
		}

		// Use a goroutine per future to detect the first settlement.
		type result struct {
			val O
			err error
		}
		ch := make(chan result, len(fs))
		for _, f := range fs {
			go func(fut *Future[O]) {
				val, err := fut.Result()
				ch <- result{val: val, err: err}
			}(f)
		}

		r := <-ch
		if r.err != nil {
			if errors.Is(r.err, errSuspendExecution) {
				return zero, r.err
			}
			return zero, r.err
		}
		return r.val, nil
	})
}
