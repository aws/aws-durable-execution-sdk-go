package durable

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// selectOutcome is the checkpointed result of a [Select] whose winning
// branch succeeded: the name of the winning branch and its settled
// outcome, in the shape [AllSettled] stores. A failed winner is not stored
// here. It is recorded as a FAILED operation, see [Select].
//
// A record whose outcome holds a rejected error is still read. Select
// returns the same failure shape for it as for a FAILED record, with the
// rebuilt branch error as the cause's single error.
type selectOutcome[O any] struct {
	Winner  string     `json:"winner"`
	Outcome Settled[O] `json:"outcome"`
}

// Select runs the branches concurrently, each in its own child context
// named by [Branch].Name, and returns the name and value of the first
// branch to settle with a terminal outcome (success or non-suspension
// error). It is [Race] for callers who need to know which branch won: the
// value alone cannot tell a primary quote from a fallback, but winner can.
//
// A terminal outcome observed before any suspension wins immediately,
// without awaiting the remaining branches. Once a suspension is observed,
// Select awaits all remaining branches so they reach their blocking
// points, then propagates the suspension; suspension takes precedence
// over terminal outcomes observed after it because the suspended branch
// completes only on a later invocation.
//
// If the winning branch fails, winner names it and err is a
// [*ChildContextError] named after the Select operation, whose ErrorType
// is "PromiseCombinatorError" and whose cause is a [*CombinatorError].
// The message is the branch error's message. The Select operation is
// recorded as FAILED with ErrorType "PromiseCombinatorError", the same
// type every combinator failure records. The winner's name is recorded
// with that failure as its ErrorData, the JSON object {"winner":"<name>"},
// so replay returns the same winner and the same error. The
// ChildContextError's ErrorData field holds that object. The failing
// branch's own child context is recorded as FAILED too. A
// [WithChildErrorMapper] mapper receives that ChildContextError, and
// Select returns the mapper's error in its place. winner still names the
// failed branch, because Select reads it from the recorded failure before
// the mapper runs.
//
// Select uses [RunInChildContext] internally, so the winner's name and
// value are checkpointed together: on replay, the same winner is returned
// even when a different branch would finish first if the branches ran
// again. opts configure that child-context operation; [WithChildSerdes]
// selects the serializer for the checkpointed record of a successful
// winner, an object with a "winner" field holding the name and an
// "outcome" field holding the value in the shape [AllSettled] stores.
//
// Empty branches returns an error immediately, without recording an
// operation, because no branch could ever win. Duplicate branch names are
// rejected the same way, because the winner would be ambiguous.
func Select[O any](ctx Context, name string, branches []Branch[O], opts ...ChildOption) (winner string, value O, err error) {
	var zero O
	if len(branches) == 0 {
		return "", zero, fmt.Errorf("durable: Select %q: no branches", name)
	}
	seen := make(map[string]struct{}, len(branches))
	for _, b := range branches {
		if _, dup := seen[b.Name]; dup {
			return "", zero, fmt.Errorf("durable: Select %q: duplicate branch name %q", name, b.Name)
		}
		seen[b.Name] = struct{}{}
	}

	// The winner is read from the recorded failure before a configured
	// error mapper runs. A mapper may return an error that no longer holds
	// the [*ChildContextError], so the winner cannot be read from the
	// mapped error.
	var failedWinner string
	captureWinner := childOptionFunc(func(o *childOptions) {
		mapper := o.errorMapper
		o.errorMapper = func(childErr *ChildContextError) error {
			failedWinner = selectWinnerOf(name, childErr)
			if mapper == nil {
				return nil
			}
			return mapper(childErr)
		}
	})
	opts = append(slices.Clip(opts), captureWinner)

	out, err := RunInChildContext(ctx, name, func(childCtx Context) (selectOutcome[O], error) {
		var none selectOutcome[O]

		// Every branch runs in its own child context.
		fs := make([]*Future[O], len(branches))
		for i, b := range branches {
			fs[i] = Go(childCtx, b.Name, b.Func)
		}

		// The branch futures are awaited together on this goroutine: it
		// parks until one settles, then reads every settled one in branch
		// order.
		observe := combinatorObserver(childCtx)
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
				observe(f.err)
				if f.err != nil && errors.Is(f.err, errSuspendExecution) {
					sawSuspend = true
					continue
				}
				if !sawSuspend {
					winner := branches[idx[j]].Name
					if f.err != nil {
						return none, selectFailure(name, winner, f.err)
					}
					return selectOutcome[O]{Winner: winner, Outcome: Settled[O]{Value: f.value}}, nil
				}
				// A terminal outcome after a suspension is discarded: the
				// loop keeps draining so every branch reaches a blocking
				// point, and the suspension propagates below. On the
				// resume invocation the branches replay and the terminal
				// outcome is returned then.
			}
			pending, idx = keptF, keptI
		}
		return none, errSuspendExecution
	}, opts...)
	if err != nil {
		return failedWinner, zero, err
	}
	if out.Outcome.Err != nil {
		// A record whose result holds a rejected outcome rebuilds the same
		// failure shape as a FAILED record.
		comb := newCombinatorFailure(name, out.Outcome.Err)
		return out.Winner, zero, &ChildContextError{Name: name, ErrorType: "PromiseCombinatorError", Message: comb.Error(), Err: comb}
	}
	return out.Winner, out.Outcome.Value, nil
}

// selectWinnerData is the ErrorData recorded with a failed [Select]: the
// name of the branch whose failure decided it.
type selectWinnerData struct {
	Winner string `json:"winner"`
}

// selectFailure returns the error that escapes the [Select] body when the
// winning branch failed with branchErr. The [*CombinatorError] makes the
// child context record ErrorType "PromiseCombinatorError". The ErrorData
// wrapper records the winner's name, which [selectWinnerOf] reads back
// from the rebuilt error on the first invocation and on replay alike.
func selectFailure(name, winner string, branchErr error) error {
	data, _ := marshalNoHTMLEscape(selectWinnerData{Winner: winner})
	return WithErrorData(newCombinatorFailure(name, branchErr), string(data))
}

// selectWinnerOf returns the winner recorded with err, the error the
// [Select] child context named name returned. It returns "" when err is
// not a recorded Select failure.
func selectWinnerOf(name string, err error) string {
	var childErr *ChildContextError
	if !errors.As(err, &childErr) || childErr.Name != name || childErr.ErrorType != "PromiseCombinatorError" {
		return ""
	}
	var data selectWinnerData
	if json.Unmarshal([]byte(childErr.ErrorData), &data) != nil {
		return ""
	}
	return data.Winner
}
