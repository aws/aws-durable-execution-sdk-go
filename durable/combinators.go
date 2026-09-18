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
		if err := awaitBarrier(asAwaitables(fs), true); err != nil {
			return nil, err
		}
		// Every future settled successfully, so each Result call below
		// returns immediately.
		results := make([]O, len(fs))
		for i, f := range fs {
			results[i], _ = f.Result()
		}
		return results, nil
	}, opts...)
}

// Awaitable is a future whose outcome can be awaited without naming its
// result type. Every *[Future] satisfies it, so futures of different result
// types can be passed to [Join] together.
type Awaitable interface {
	// await blocks until the future settles and returns its error, or nil
	// on success. The method is unexported so only SDK futures implement
	// the interface.
	await() error
}

// await implements [Awaitable].
func (f *Future[O]) await() error {
	_, err := f.Result()
	return err
}

// Join records a combinator operation and waits for every future to settle,
// then returns the first non-nil error in argument order, or nil when all
// succeeded. Values are read from each future afterwards with
// [Future.Result], which returns immediately once Join has returned nil.
// Join is the replacement for awaiting several futures by hand: a
// hand-written sequence of Result calls that returns on the first error
// leaves the remaining futures unawaited, so their branches never reach a
// blocking point when the invocation suspends and their progress is not
// checkpointed.
//
// Join never abandons a future: a durable branch has no cancellation, so an
// error observed early does not stop the remaining futures from being
// awaited. Once a suspension is observed, Join awaits all remaining futures
// so their branches reach blocking points and checkpoint progress, then
// propagates the suspension; suspension takes precedence over terminal
// outcomes because the suspended branch completes only on a later
// invocation. On the resume invocation the futures replay and the first
// error is returned then. This matches the draining behaviour of [All].
//
// Join uses [RunInChildContext] internally, so its outcome is checkpointed:
// on replay, the stored outcome is returned without re-awaiting the
// futures, and a failure is returned as a [*ChildContextError] wrapping
// the first error. opts configure that child-context operation.
//
// Empty input returns nil immediately.
//
//	fa := durable.StepAsync(ctx, "charge", chargeCard)
//	fb := durable.Go(ctx, "notify", notifyWarehouse)
//	if err := durable.Join(ctx, "settle", []durable.Awaitable{fa, fb}); err != nil {
//		return err
//	}
//	receipt, _ := fa.Result()
//	ok, _ := fb.Result()
func Join(ctx Context, name string, fs []Awaitable, opts ...ChildOption) error {
	_, err := RunInChildContext(ctx, name, func(_ Context) (Void, error) {
		return Void{}, awaitBarrier(fs, false)
	}, opts...)
	return err
}

// awaitBarrier is the barrier shared by [All] and [Join]. It awaits fs in
// order and reports the aggregate outcome.
//
// A suspension observed from any future is the decided outcome: the
// barrier keeps awaiting the remaining futures so every branch reaches a
// blocking point and checkpoints progress, and then returns the suspension
// sentinel. Terminal outcomes observed after the suspension are discarded
// because the suspended branch completes only on a later invocation, where
// the futures replay and the terminal outcome is returned then.
//
// When no future suspends, the first non-suspension error in order is
// returned. With failFast set, that error is returned as soon as it is
// observed, without awaiting the remaining futures. Without failFast, the
// remaining futures are awaited first.
//
// Deferred-suspension futures (pending callbacks) commit the invocation to
// PENDING when first awaited, so awaiting one in order is the suspension
// case above.
func awaitBarrier(fs []Awaitable, failFast bool) error {
	var firstErr error
	var sawSuspend bool
	for _, f := range fs {
		err := f.await()
		switch {
		case err == nil:
		case errors.Is(err, errSuspendExecution):
			sawSuspend = true
		case failFast && !sawSuspend:
			return err
		case firstErr == nil:
			firstErr = err
		}
	}
	if sawSuspend {
		return errSuspendExecution
	}
	return firstErr
}

// asAwaitables widens a slice of futures to the [Awaitable] interface.
func asAwaitables[O any](fs []*Future[O]) []Awaitable {
	out := make([]Awaitable, len(fs))
	for i, f := range fs {
		out[i] = f
	}
	return out
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
