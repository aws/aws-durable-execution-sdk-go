package durable

import (
	"errors"
)

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
// re-awaiting the futures. opts configure that child-context operation;
// [WithChildSerdes] selects the serializer for the aggregate result.
//
// Empty input returns an empty slice immediately (matching Promise.all([])).
func All[O any](ctx Context, name string, fs []*Future[O], opts ...ChildOption) ([]O, error) {
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
	}, opts...)
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
// re-awaiting the futures. opts configure that child-context operation;
// [WithChildSerdes] selects the serializer for the aggregate result.
//
// Empty input returns an empty slice immediately.
func AllSettled[O any](ctx Context, name string, fs []*Future[O], opts ...ChildOption) ([]Settled[O], error) {
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
	}, opts...)
}

// Any records a combinator operation and returns the value of the first
// future to succeed. A success observed before any suspension returns
// immediately, without awaiting the remaining futures. Once a suspension is
// observed, Any awaits all remaining futures so their branches reach
// blocking points, then propagates the suspension; suspension takes
// precedence over terminal outcomes observed after it because the
// suspended branch completes only on a later invocation. If every future
// fails with a non-suspension error, Any returns a [*CombinatorError]
// wrapping all individual errors.
//
// Any uses [RunInChildContext] internally, so the winning result is
// checkpointed: on replay, the same winner is returned deterministically
// regardless of future settlement order. opts configure that child-context
// operation; [WithChildSerdes] selects the serializer for the winner.
//
// Empty input fails immediately with a [*CombinatorError] (no futures can
// succeed), matching Promise.any([]).
func Any[O any](ctx Context, name string, fs []*Future[O], opts ...ChildOption) (O, error) {
	return RunInChildContext(ctx, name, func(childCtx Context) (O, error) {
		var zero O
		if len(fs) == 0 {
			return zero, &CombinatorError{Name: name, Errors: nil}
		}

		// Use a goroutine per future to detect the first success.
		// All futures are already durable (their operations were
		// claimed before the combinator), so this is a pure in-process
		// join — no new durable operations are created.
		//
		// stop is closed when Any returns. A receive goroutine whose
		// future has not settled by then exits instead of waiting for
		// it: a losing future may never settle in this invocation, and
		// the goroutine must not outlive the combinator that started
		// it.
		//
		// Deferred-suspension futures (pending callbacks) cannot settle
		// with a terminal outcome in this invocation, so they cannot
		// win; awaiting one commits the invocation to PENDING. They are
		// set aside and awaited only below, once suspension is the only
		// possible aggregate outcome, so a losing pending callback
		// never overrides a terminal winner.
		type result struct {
			val O
			err error
			idx int
		}
		ch := make(chan result, len(fs))
		stop := make(chan struct{})
		defer close(stop)
		var deferred []*Future[O]
		joined := 0
		for i, f := range fs {
			if f.deferredSuspension {
				deferred = append(deferred, f)
				continue
			}
			joined++
			go func(idx int, fut *Future[O]) {
				val, err, ok := fut.resultOrStop(stop)
				if !ok {
					return
				}
				// ch has room for every future, so this never blocks.
				ch <- result{val: val, err: err, idx: idx}
			}(i, f)
		}

		observe := combinatorObserver(childCtx)
		errs := make([]error, len(fs))
		var sawSuspend bool
		for range joined {
			r := <-ch
			observe(r.err)
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
		if !sawSuspend && len(deferred) == 0 {
			// All failed.
			return zero, &CombinatorError{Name: name, Errors: errs}
		}
		// Suspension is the decided outcome: either a branch suspended,
		// or the only unsettled futures are pending callbacks that
		// resolve on a later invocation. Awaiting the deferred futures
		// now records the pending commitment and marks their contexts
		// blocked.
		awaitDeferred(deferred)
		return zero, errSuspendExecution
	}, opts...)
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
// future settlement order. opts configure that child-context operation;
// [WithChildSerdes] selects the serializer for the winner.
//
// Race returns the winner's value but not which future produced it. Code
// that must branch on the winner's identity should use [Select], which
// runs named branches and checkpoints the winner's name with its value.
//
// Empty input suspends (no future will ever settle), matching
// Promise.race([]) which returns a forever-pending promise.
func Race[O any](ctx Context, name string, fs []*Future[O], opts ...ChildOption) (O, error) {
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
		// stop is closed when Race returns so a receive goroutine whose
		// future has not settled by then exits instead of waiting for
		// it (see Any).
		//
		// Deferred-suspension futures (pending callbacks) cannot settle
		// with a terminal outcome in this invocation, so they cannot
		// win; awaiting one commits the invocation to PENDING. They are
		// set aside and awaited only below, once suspension is the only
		// possible aggregate outcome, so a losing pending callback
		// never overrides a terminal winner.
		type result struct {
			val O
			err error
		}
		ch := make(chan result, len(fs))
		stop := make(chan struct{})
		defer close(stop)
		var deferred []*Future[O]
		joined := 0
		for _, f := range fs {
			if f.deferredSuspension {
				deferred = append(deferred, f)
				continue
			}
			joined++
			go func(fut *Future[O]) {
				val, err, ok := fut.resultOrStop(stop)
				if !ok {
					return
				}
				// ch has room for every future, so this never blocks.
				ch <- result{val: val, err: err}
			}(f)
		}

		observe := combinatorObserver(childCtx)
		var sawSuspend bool
		for range joined {
			r := <-ch
			observe(r.err)
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
		// Suspension is the decided outcome: every joined branch
		// suspended, and any remaining futures are pending callbacks
		// that resolve on a later invocation. Awaiting the deferred
		// futures now records the pending commitment and marks their
		// contexts blocked.
		awaitDeferred(deferred)
		return zero, errSuspendExecution
	}, opts...)
}

// combinatorObserver returns the outcome observer installed on ctx by a
// test, or a no-op when none is set. Reading the observer once per
// combinator call, from the child context that inherited it at creation,
// means the receive loop never reads shared mutable state.
func combinatorObserver(ctx Context) func(err error) {
	if ec, ok := ctx.(*execContext); ok && ec.combinatorObserve != nil {
		return ec.combinatorObserve
	}
	return func(error) {}
}

// awaitDeferred awaits deferred-suspension futures (pending callbacks) on
// behalf of a combinator. Called only once suspension is the combinator's
// decided outcome, so the pending commitment made by each future's first
// Result call can no longer override a terminal winner. Every deferred
// future settles with the suspension sentinel by construction; the
// outcomes are discarded because the caller propagates the suspension
// itself.
func awaitDeferred[O any](fs []*Future[O]) {
	for _, f := range fs {
		_, _ = f.Result()
	}
}
