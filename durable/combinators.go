package durable

import (
	"errors"
)

// combinatorObserve, when non-nil, is called by the Any and Race receive
// loops after each future outcome is observed. Tests use it to release a
// sibling future only after a specific outcome has been seen, making
// interleavings deterministic. It is nil in production.
var combinatorObserve func(err error)

// All records a combinator operation and waits for every future to succeed,
// returning the values in input order. A non-suspension error observed
// before any suspension fails All immediately with that error, without
// awaiting the remaining futures (matching Promise.all's fail-fast
// rejection). Once a suspension is observed, All awaits all remaining
// futures so their branches reach blocking points and checkpoint progress,
// then propagates the suspension; suspension takes precedence over terminal
// outcomes observed after it because the suspended branch completes only on
// a later invocation.
//
// All uses [RunInChildContext] internally, so the aggregate result is
// checkpointed: on replay, the stored result is returned without
// re-awaiting the futures.
//
// Empty input returns an empty slice immediately (matching Promise.all([])).
func All[O any](ctx Context, name string, fs []*Future[O]) ([]O, error) {
	return RunInChildContext(ctx, name, func(_ Context) ([]O, error) {
		results := make([]O, len(fs))
		var sawSuspend bool
		for i, f := range fs {
			val, err := f.Result()
			switch {
			case err == nil:
				results[i] = val
			case errors.Is(err, errSuspendExecution):
				sawSuspend = true
			case !sawSuspend:
				return nil, err
			}
			// A non-suspension error observed after a suspension is
			// discarded: the loop keeps draining so every branch reaches
			// a blocking point, and the suspension propagates below. On
			// the resume invocation the futures replay and the error is
			// returned then.
		}
		if sawSuspend {
			return nil, errSuspendExecution
		}
		return results, nil
	})
}

// AllSettled records a combinator operation and waits for every future to
// settle, returning each outcome in input order regardless of success or
// failure. If any future suspends, AllSettled awaits all remaining futures
// and then propagates the suspension; suspension takes precedence over
// terminal outcomes because the suspended branch completes only on a later
// invocation.
//
// AllSettled uses [RunInChildContext] internally, so the aggregate result is
// checkpointed. On replay, the stored outcomes are returned without
// re-awaiting the futures.
//
// Empty input returns an empty slice immediately.
func AllSettled[O any](ctx Context, name string, fs []*Future[O]) ([]Settled[O], error) {
	return RunInChildContext(ctx, name, func(_ Context) ([]Settled[O], error) {
		results := make([]Settled[O], len(fs))
		var sawSuspend bool
		for i, f := range fs {
			val, err := f.Result()
			switch {
			case err == nil:
				results[i] = Settled[O]{Value: val}
			case errors.Is(err, errSuspendExecution):
				sawSuspend = true
			default:
				results[i] = Settled[O]{Err: err}
			}
		}
		if sawSuspend {
			return nil, errSuspendExecution
		}
		return results, nil
	})
}

// Any records a combinator operation and returns the value of the first
// future to succeed. A success observed before any suspension returns
// immediately, without awaiting the remaining futures. Once a suspension is
// observed, Any awaits all remaining futures so their branches reach
// blocking points, then propagates the suspension; suspension takes
// precedence over terminal outcomes observed after it because the
// suspended branch completes only on a later invocation. If every future
// fails with a non-suspension error, Any returns a [*CombinatorError]
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
		var sawSuspend bool
		for range fs {
			r := <-ch
			if combinatorObserve != nil {
				combinatorObserve(r.err)
			}
			switch {
			case r.err == nil:
				if !sawSuspend {
					return r.val, nil
				}
				// A success after a suspension is discarded: the loop
				// keeps draining so every branch reaches a blocking
				// point, and the suspension propagates below. On the
				// resume invocation the futures replay and the success
				// is returned then.
			case errors.Is(r.err, errSuspendExecution):
				sawSuspend = true
			default:
				errs[r.idx] = r.err
			}
		}
		if sawSuspend {
			return zero, errSuspendExecution
		}
		// All failed.
		return zero, &CombinatorError{Name: name, Errors: errs}
	})
}

// Race records a combinator operation and returns the outcome of the first
// future to settle with a terminal result (success or non-suspension
// error). A terminal outcome observed before any suspension returns
// immediately, without awaiting the remaining futures. Once a suspension is
// observed, Race awaits all remaining futures so their branches reach
// blocking points, then propagates the suspension; suspension takes
// precedence over terminal outcomes observed after it because the
// suspended branch completes only on a later invocation.
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
				ec.suspend.commitPending(ec.abandon)
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

		var sawSuspend bool
		for range fs {
			r := <-ch
			if combinatorObserve != nil {
				combinatorObserve(r.err)
			}
			if r.err != nil && errors.Is(r.err, errSuspendExecution) {
				sawSuspend = true
				continue
			}
			if !sawSuspend {
				return r.val, r.err
			}
			// A terminal outcome after a suspension is discarded: the
			// loop keeps draining so every branch reaches a blocking
			// point, and the suspension propagates below. On the resume
			// invocation the futures replay and the terminal outcome is
			// returned then.
		}
		return zero, errSuspendExecution
	})
}
