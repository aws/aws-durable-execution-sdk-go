package durable

import (
	"errors"
	"fmt"
	"slices"
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
// A failure is returned as a [*ChildContextError] whose ErrorType is
// "PromiseCombinatorError" and whose cause is a [*CombinatorError]. Its
// message is the message of the first error in input order.
//
// Empty input returns an empty slice immediately (matching Promise.all([])).
func All[O any](ctx Context, name string, fs []*Future[O], opts ...ChildOption) ([]O, error) {
	return RunInChildContext(ctx, name, func(childCtx Context) ([]O, error) {
		if err := awaitBarrier(childCtx, asAwaitables(fs), true); err != nil {
			return nil, wrapCombinatorFailure(name, err)
		}
		// Every future settled successfully, so each Result call below
		// returns immediately.
		results := make([]O, len(fs))
		for i, f := range fs {
			results[i], _ = f.result()
		}
		return results, nil
	}, opts...)
}

// Awaitable is a future whose outcome can be awaited without naming its
// result type. Every *[Future] satisfies it, so futures of different result
// types can be passed to [Join] together.
type Awaitable interface {
	// await blocks until the future settles and returns its error, or nil
	// on success, as [Future.Result] does with ctx as the receiving
	// context. The method is unexported so only SDK futures implement the
	// interface.
	await(ctx Context) error
}

// await implements [Awaitable].
func (f *Future[O]) await(ctx Context) error {
	_, err := f.Result(ctx)
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
// futures. opts configure that child-context operation.
//
// A failure is returned as a [*ChildContextError] whose ErrorType is
// "PromiseCombinatorError" and whose cause is a [*CombinatorError]. Its
// message is the message of the first error in argument order.
//
// Empty input returns nil immediately.
//
//	fa := durable.StepAsync(ctx, "charge", chargeCard)
//	fb := durable.Go(ctx, "notify", notifyWarehouse)
//	if err := durable.Join(ctx, "settle", []durable.Awaitable{fa, fb}); err != nil {
//		return err
//	}
//	receipt, _ := fa.Result(ctx)
//	ok, _ := fb.Result(ctx)
func Join(ctx Context, name string, fs []Awaitable, opts ...ChildOption) error {
	_, err := RunInChildContext(ctx, name, func(childCtx Context) (Void, error) {
		return Void{}, wrapCombinatorFailure(name, awaitBarrier(childCtx, fs, false))
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
// Each future is awaited with [Future.Result], which parks the goroutine.
// A future whose operation finishes in this invocation settles with its
// outcome; the suspension sentinel arrives only when the invocation
// suspends.
func awaitBarrier(ctx Context, fs []Awaitable, failFast bool) error {
	var firstErr error
	var sawSuspend bool
	for _, f := range fs {
		err := f.await(ctx)
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
// A failing future never fails AllSettled and never produces a
// [*CombinatorError]: AllSettled resolves, and the future's error is in
// Settled[i].Err. A failure to serialize the aggregate result is a
// [*SerdesError], as for any child context.
//
// Empty input returns an empty slice immediately.
func AllSettled[O any](ctx Context, name string, fs []*Future[O], opts ...ChildOption) ([]Settled[O], error) {
	return RunInChildContext(ctx, name, func(_ Context) ([]Settled[O], error) {
		return allSettledOutcomes(fs)
	}, opts...)
}

// allSettledOutcomes awaits every future in fs and returns their outcomes
// in input order, or the suspension sentinel when any future suspended.
func allSettledOutcomes[O any](fs []*Future[O]) ([]Settled[O], error) {
	results := make([]Settled[O], len(fs))
	var sawSuspend bool
	for i, f := range fs {
		val, err := f.result()
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
}

// Any records a combinator operation and returns the value of the first
// future to succeed. A success observed before any suspension returns
// immediately, without awaiting the remaining futures. Once a suspension is
// observed, Any awaits all remaining futures so their branches reach
// blocking points, then propagates the suspension; suspension takes
// precedence over terminal outcomes observed after it because the
// suspended branch completes only on a later invocation. If every future
// fails with a non-suspension error, Any fails with a [*CombinatorError]
// wrapping all individual errors, whose message is
// "All promises were rejected". The failure is returned as a
// [*ChildContextError] whose ErrorType is "PromiseCombinatorError" and
// whose cause is that [*CombinatorError].
//
// Any uses [RunInChildContext] internally, so the winning result is
// checkpointed: on replay, the same winner is returned deterministically
// regardless of future settlement order. opts configure that child-context
// operation; [WithChildSerdes] selects the serializer for the winner.
//
// Empty input fails immediately with a [*CombinatorError] whose message is
// "All promises were rejected" and whose Errors is empty, because no
// future can succeed. This matches Promise.any([]).
func Any[O any](ctx Context, name string, fs []*Future[O], opts ...ChildOption) (O, error) {
	return RunInChildContext(ctx, name, func(childCtx Context) (O, error) {
		var zero O
		if len(fs) == 0 {
			return zero, &CombinatorError{Name: name, Errors: nil, message: anyRejectedMessage}
		}

		// The futures are awaited together on this goroutine: it parks
		// until one of the outstanding futures settles, then reads every
		// settled one in input order. All futures are already durable
		// (their operations were claimed before the combinator), so this
		// is a pure in-process join and no new durable operations are
		// created. A pending callback starts being watched here.
		for _, f := range fs {
			f.activate()
		}
		observe := combinatorObserver(childCtx)
		errs := make([]error, len(fs))
		var sawSuspend bool
		pending := slices.Clone(fs)
		idx := make([]int, len(fs))
		for i := range idx {
			idx[i] = i
		}
		for len(pending) > 0 {
			awaitAnySettled(pending)
			keptF, keptI := pending[:0], idx[:0]
			for j, f := range pending {
				if !f.isDone() {
					keptF, keptI = append(keptF, f), append(keptI, idx[j])
					continue
				}
				val, err := f.value, f.err
				observe(err)
				switch {
				case err == nil:
					if !sawSuspend {
						return val, nil
					}
					// A success after a suspension is discarded: the
					// loop keeps draining so every branch reaches a
					// blocking point, and the suspension propagates
					// below. On the resume invocation the futures
					// replay and the success is returned then.
				case errors.Is(err, errSuspendExecution):
					sawSuspend = true
				default:
					errs[idx[j]] = err
				}
			}
			pending, idx = keptF, keptI
		}
		if !sawSuspend {
			// All failed.
			return zero, &CombinatorError{Name: name, Errors: errs, message: anyRejectedMessage}
		}
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
// A failure is returned as a [*ChildContextError] whose ErrorType is
// "PromiseCombinatorError" and whose cause is a [*CombinatorError]. Its
// message is the message of the first future to settle with a failure.
//
// Empty input returns an error immediately, without recording an
// operation, because no future can ever settle.
func Race[O any](ctx Context, name string, fs []*Future[O], opts ...ChildOption) (O, error) {
	if len(fs) == 0 {
		var zero O
		return zero, fmt.Errorf("durable: Race %q: no futures", name)
	}
	return RunInChildContext(ctx, name, func(childCtx Context) (O, error) {
		var zero O

		// The futures are awaited together on this goroutine; see Any.
		for _, f := range fs {
			f.activate()
		}
		observe := combinatorObserver(childCtx)
		var sawSuspend bool
		pending := slices.Clone(fs)
		for len(pending) > 0 {
			awaitAnySettled(pending)
			kept := pending[:0]
			for _, f := range pending {
				if !f.isDone() {
					kept = append(kept, f)
					continue
				}
				observe(f.err)
				if f.err != nil && errors.Is(f.err, errSuspendExecution) {
					sawSuspend = true
					continue
				}
				if !sawSuspend {
					if f.err != nil {
						return zero, newCombinatorFailure(name, f.err)
					}
					return f.value, nil
				}
				// A terminal outcome after a suspension is discarded:
				// the loop keeps draining so every branch reaches a
				// blocking point, and the suspension propagates below.
				// On the resume invocation the futures replay and the
				// terminal outcome is returned then.
			}
			pending = kept
		}
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
