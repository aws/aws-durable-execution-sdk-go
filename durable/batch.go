package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// Wire subtypes for batch operations.
const (
	operationSubTypeMap            = "Map"
	operationSubTypeMapIteration   = "MapIteration"
	operationSubTypeParallel       = "Parallel"
	operationSubTypeParallelBranch = "ParallelBranch"
)

// Map processes items concurrently, applying fn to each in its own child
// context, and returns the collected results. Concurrency and completion
// behavior are configured with [BatchOption] values.
//
// Each item runs in a MapIteration child context. Items are identified by
// their zero-based index; use [WithItemNamer] to assign display names from
// item values. MaxConcurrency bounds in-flight items; completion config may
// stop scheduling early.
func Map[I, O any](ctx Context, name string, items []I, fn func(ctx Context, item I, index int) (O, error), opts ...BatchOption) (BatchResult[O], error) {
	ec, ok := ctx.(*execContext)
	if !ok {
		return BatchResult[O]{}, fmt.Errorf("durable: Map %q: Context was not created by the SDK", name)
	}

	options := resolveBatchOptions(ec, opts)

	if options.maxConcurrencySet && options.maxConcurrency <= 0 {
		return BatchResult[O]{}, fmt.Errorf("durable: Map %q: max concurrency must be positive, got %d", name, options.maxConcurrency)
	}

	id, err := ec.claimOperation()
	if err != nil {
		return BatchResult[O]{}, err
	}

	// Check if the batch is already checkpointed as a terminal operation.
	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(types.OperationTypeContext), operationSubTypeMap, name); err != nil {
		return BatchResult[O]{}, err
	}
	if ec.unfinishedInSucceededContext(op) {
		return BatchResult[O]{}, ec.parkUnfinishedReplay()
	}
	if op != nil && op.status.terminal() {
		result, err := replayTerminalBatch[I, O](ec, op, id, name, items, fn, options, operationSubTypeMap, operationSubTypeMapIteration)
		if err != nil {
			return BatchResult[O]{}, err
		}
		// Advance the parent counter past the iteration IDs that were
		// consumed from it during the original execution. Sequential
		// (concurrency=1) only claims started items; concurrent claims
		// all items upfront.
		concurrency := options.maxConcurrency
		if concurrency < 0 {
			concurrency = len(items)
		}
		if concurrency == 1 {
			ec.ids.advance(len(result.Items))
		} else {
			ec.ids.advance(len(items))
		}
		return result, nil
	}

	// Checkpoint the parent Map context START.
	if op == nil {
		update := batchParentUpdate(ec, id, name, operationSubTypeMap, types.OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
			return BatchResult[O]{}, err
		}
	}

	totalItems := len(items)
	if totalItems == 0 {
		// Empty collection: checkpoint success immediately.
		result := BatchResult[O]{
			Items:  nil,
			Reason: CompletionAllCompleted,
		}
		return checkpointBatchSuccess(ec, id, name, operationSubTypeMap, result, options)
	}

	// Execute items with bounded concurrency and completion checking.
	return executeBatchItems[I, O](ec, id, name, totalItems, options, operationSubTypeMap, operationSubTypeMapIteration, func(childCtx Context, index int) (O, error) {
		return fn(childCtx, items[index], index)
	})
}

// Parallel executes branches concurrently, each in its own child context,
// and returns the collected results. All branches must produce the same
// type; for heterogeneous fan-out, use [Go] with futures of different
// types.
//
// Each branch runs in a ParallelBranch child context named by [Branch].Name.
// MaxConcurrency bounds in-flight branches; completion config may stop
// scheduling early.
func Parallel[O any](ctx Context, name string, branches []Branch[O], opts ...BatchOption) (BatchResult[O], error) {
	ec, ok := ctx.(*execContext)
	if !ok {
		return BatchResult[O]{}, fmt.Errorf("durable: Parallel %q: Context was not created by the SDK", name)
	}

	options := resolveBatchOptions(ec, opts)

	// Default branch naming from Branch.Name when no explicit itemNamer.
	if options.itemNamer == nil {
		options.itemNamer = func(index int) string {
			if index < len(branches) {
				return branches[index].Name
			}
			return ""
		}
	}

	if options.maxConcurrencySet && options.maxConcurrency <= 0 {
		return BatchResult[O]{}, fmt.Errorf("durable: Parallel %q: max concurrency must be positive, got %d", name, options.maxConcurrency)
	}

	id, err := ec.claimOperation()
	if err != nil {
		return BatchResult[O]{}, err
	}

	// Check if the batch is already checkpointed as a terminal operation.
	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(types.OperationTypeContext), operationSubTypeParallel, name); err != nil {
		return BatchResult[O]{}, err
	}
	if ec.unfinishedInSucceededContext(op) {
		return BatchResult[O]{}, ec.parkUnfinishedReplay()
	}
	if op != nil && op.status.terminal() {
		placeholders := make([]struct{}, len(branches))
		result, err := replayTerminalBatch[struct{}, O](ec, op, id, name, placeholders, func(ctx Context, _ struct{}, index int) (O, error) {
			return branches[index].Func(ctx)
		}, options, operationSubTypeParallel, operationSubTypeParallelBranch)
		if err != nil {
			return BatchResult[O]{}, err
		}
		// Advance the parent counter past the branch IDs that were
		// consumed from it during the original execution.
		concurrency := options.maxConcurrency
		if concurrency < 0 {
			concurrency = len(branches)
		}
		if concurrency == 1 {
			ec.ids.advance(len(result.Items))
		} else {
			ec.ids.advance(len(branches))
		}
		return result, nil
	}

	// Checkpoint the parent Parallel context START.
	if op == nil {
		update := batchParentUpdate(ec, id, name, operationSubTypeParallel, types.OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
			return BatchResult[O]{}, err
		}
	}

	totalItems := len(branches)
	if totalItems == 0 {
		result := BatchResult[O]{
			Items:  nil,
			Reason: CompletionAllCompleted,
		}
		return checkpointBatchSuccess(ec, id, name, operationSubTypeParallel, result, options)
	}

	return executeBatchItems[struct{}, O](ec, id, name, totalItems, options, operationSubTypeParallel, operationSubTypeParallelBranch, func(childCtx Context, index int) (O, error) {
		return branches[index].Func(childCtx)
	})
}

// Branch is one branch of a [Parallel] operation.
type Branch[O any] struct {
	// Name identifies the branch. It may be empty.
	Name string

	// Func is the branch body.
	Func func(Context) (O, error)
}

// BatchItemStatus is the terminal status of one item or branch in a batch
// operation.
type BatchItemStatus int

// Batch item statuses. Values are persisted in checkpoints, so they are
// pinned explicitly rather than derived from iota ordering.
const (
	// BatchItemSucceeded indicates the item completed and produced a
	// result.
	BatchItemSucceeded BatchItemStatus = 1

	// BatchItemFailed indicates the item failed.
	BatchItemFailed BatchItemStatus = 2

	// BatchItemStarted indicates the item started but the batch completed
	// early before it reached a terminal state, so its work was abandoned:
	// the parent stopped awaiting it and does not count it as a success or
	// a failure. It is still reported (and counted by TotalCount) because
	// the work was begun. Items that never started are omitted entirely.
	BatchItemStarted BatchItemStatus = 4
)

// String returns the wire representation of the status.
func (s BatchItemStatus) String() string {
	switch s {
	case BatchItemSucceeded:
		return "SUCCEEDED"
	case BatchItemFailed:
		return "FAILED"
	case BatchItemStarted:
		return "STARTED"
	default:
		return "UNKNOWN"
	}
}

// BatchItem is the outcome of one item or branch in a batch operation.
type BatchItem[O any] struct {
	// Index is the zero-based position of this item in the original input
	// slice.
	Index int

	// Name identifies the item or branch.
	Name string

	// Status is the item's terminal status.
	Status BatchItemStatus

	// Result is the item's result. It is the zero value unless Status is
	// [BatchItemSucceeded].
	Result O

	// Err is the item's error. It is nil unless Status is
	// [BatchItemFailed].
	Err error
}

// BatchResult is the collected outcome of a [Map] or [Parallel] operation.
type BatchResult[O any] struct {
	// Items holds the per-item outcomes in input order. Items that started
	// but were abandoned when the batch completed early are included with
	// status [BatchItemStarted]; items that never started are omitted.
	Items []BatchItem[O]

	// Reason records why the batch completed.
	Reason CompletionReason
}

// Results returns the successful results in input order. Failed or
// not-started items are omitted.
func (r BatchResult[O]) Results() []O {
	var out []O
	for i := range r.Items {
		if r.Items[i].Status == BatchItemSucceeded {
			out = append(out, r.Items[i].Result)
		}
	}
	if out == nil {
		out = []O{}
	}
	return out
}

// Succeeded returns the items that succeeded, in input order.
func (r BatchResult[O]) Succeeded() []BatchItem[O] {
	var out []BatchItem[O]
	for i := range r.Items {
		if r.Items[i].Status == BatchItemSucceeded {
			out = append(out, r.Items[i])
		}
	}
	return out
}

// Failed returns the items that failed, in input order.
func (r BatchResult[O]) Failed() []BatchItem[O] {
	var out []BatchItem[O]
	for i := range r.Items {
		if r.Items[i].Status == BatchItemFailed {
			out = append(out, r.Items[i])
		}
	}
	return out
}

// Errors returns the errors from failed items, in input order.
func (r BatchResult[O]) Errors() []error {
	var out []error
	for i := range r.Items {
		if r.Items[i].Status == BatchItemFailed && r.Items[i].Err != nil {
			out = append(out, r.Items[i].Err)
		}
	}
	return out
}

// HasFailure reports whether any item failed.
func (r BatchResult[O]) HasFailure() bool {
	for i := range r.Items {
		if r.Items[i].Status == BatchItemFailed {
			return true
		}
	}
	return false
}

// Err returns the error that should fail the batch, or nil if the batch
// succeeded. When any item failed with a non-nil error, it returns the
// first such item's error. Otherwise, when [BatchResult.Status] is
// [BatchItemFailed], it returns a batch-level completion error carrying
// the completion reason. Callers propagate the returned error to fail the
// execution.
func (r BatchResult[O]) Err() error {
	for i := range r.Items {
		if r.Items[i].Status == BatchItemFailed && r.Items[i].Err != nil {
			return r.Items[i].Err
		}
	}
	if r.Status() == BatchItemFailed {
		return fmt.Errorf("durable: batch failed: %s", r.Reason)
	}
	return nil
}

// SuccessCount returns the number of items that succeeded.
func (r BatchResult[O]) SuccessCount() int {
	n := 0
	for i := range r.Items {
		if r.Items[i].Status == BatchItemSucceeded {
			n++
		}
	}
	return n
}

// FailureCount returns the number of items that failed.
func (r BatchResult[O]) FailureCount() int {
	n := 0
	for i := range r.Items {
		if r.Items[i].Status == BatchItemFailed {
			n++
		}
	}
	return n
}

// TotalCount returns the number of items that were started: successes,
// failures, and started-but-abandoned items. Items that never started
// (because the batch completed early) are excluded.
func (r BatchResult[O]) TotalCount() int {
	return len(r.Items)
}

// Status returns the overall batch status: [BatchItemFailed] if any item
// failed or the batch-level completion indicates failure,
// [BatchItemSucceeded] otherwise.
//
// Status is derived from the exported Items and Reason fields, so it is
// authoritative for any BatchResult — including one reconstructed by a
// custom [Serdes] round trip or constructed directly in tests.
func (r BatchResult[O]) Status() BatchItemStatus {
	return batchStatusFor(r.Items, r.Reason)
}

// batchStatusFor computes the overall batch status from the item outcomes
// and the completion reason.
func batchStatusFor[O any](items []BatchItem[O], reason CompletionReason) BatchItemStatus {
	for i := range items {
		if items[i].Status == BatchItemFailed {
			return BatchItemFailed
		}
	}
	if reason == CompletionFailureToleranceExceeded {
		return BatchItemFailed
	}
	return BatchItemSucceeded
}

// CompletionReason records why a batch operation completed.
type CompletionReason int

// Batch completion reasons.
const (
	// CompletionAllCompleted indicates every item ran to completion.
	CompletionAllCompleted CompletionReason = iota + 1

	// CompletionMinSuccessfulReached indicates the batch completed early
	// because the MinSuccessful threshold was met.
	CompletionMinSuccessfulReached

	// CompletionFailureToleranceExceeded indicates the batch failed early
	// because more items failed than the tolerance allows.
	CompletionFailureToleranceExceeded
)

// String returns the wire representation of the completion reason.
func (r CompletionReason) String() string {
	switch r {
	case CompletionAllCompleted:
		return "ALL_COMPLETED"
	case CompletionMinSuccessfulReached:
		return "MIN_SUCCESSFUL_REACHED"
	case CompletionFailureToleranceExceeded:
		return "FAILURE_TOLERANCE_EXCEEDED"
	default:
		return "UNKNOWN"
	}
}

// NestingMode controls whether batch items run in real or virtual child
// contexts.
type NestingMode int

// Nesting modes.
const (
	// NestingNormal (default) runs each item in its own child context,
	// producing per-item ContextStarted/ContextSucceeded events.
	NestingNormal NestingMode = iota

	// NestingFlat runs each item in a virtual context: operations inside
	// the item are checkpointed directly under the parent batch context,
	// with no per-item context events.
	NestingFlat
)

// BatchOption configures a [Map] or [Parallel] operation.
type BatchOption interface {
	applyBatch(*batchOptions)
}

// WithMaxConcurrency bounds how many items or branches run at once.
// A value of zero or negative is invalid and causes Map/Parallel to return
// an error.
func WithMaxConcurrency(n int) BatchOption {
	return batchOptionFunc(func(o *batchOptions) {
		o.maxConcurrency = n
		o.maxConcurrencySet = true
	})
}

// WithCompletion sets the batch's early-completion policy.
func WithCompletion(c CompletionConfig) BatchOption {
	return batchOptionFunc(func(o *batchOptions) { o.completion = c })
}

// WithItemNamer sets display names for the items of a [Map] operation.
// The namer receives the item's zero-based index; close over the input
// slice to derive a name from the item value:
//
//	durable.Map(ctx, "process", orders, processOrder,
//	    durable.WithItemNamer(func(i int) string { return orders[i].ID }))
//
// namer must be a deterministic function of its argument.
//
// For [Parallel] branches, set [Branch].Name directly instead.
func WithItemNamer(namer func(index int) string) BatchOption {
	return batchOptionFunc(func(o *batchOptions) { o.itemNamer = namer })
}

// WithBatchSerdes overrides the serializer for each item's result within
// the batch (the per-item serdes).
func WithBatchSerdes(s Serdes) BatchOption {
	return batchOptionFunc(func(o *batchOptions) { o.itemSerdes = s })
}

// WithBatchResultSerdes overrides the serializer for the entire batch
// result when checkpointing the parent context's terminal event. This is
// the operation-level serdes: it serializes and deserializes the whole
// [BatchResult] rather than individual items.
func WithBatchResultSerdes(s Serdes) BatchOption {
	return batchOptionFunc(func(o *batchOptions) { o.resultSerdes = s })
}

// WithNesting sets the nesting mode for the batch. [NestingFlat] causes
// items to run in virtual contexts without per-item context events.
func WithNesting(m NestingMode) BatchOption {
	return batchOptionFunc(func(o *batchOptions) { o.nesting = m })
}

// CompletionConfig is a batch early-completion policy. Zero values leave
// the corresponding threshold unset. Thresholds may be combined — when
// multiple are set, the first threshold to fire wins.
type CompletionConfig struct {
	// MinSuccessful completes the batch early once this many items
	// succeed.
	MinSuccessful int

	// ToleratedFailureCount fails the batch once more than this many
	// items fail. Nil disables count-based tolerance; an explicit zero
	// fails the batch on the first failure.
	ToleratedFailureCount *int

	// ToleratedFailurePercentage fails the batch once the failure
	// percentage strictly exceeds this threshold.
	ToleratedFailurePercentage int
}

type batchOptions struct {
	maxConcurrency    int
	maxConcurrencySet bool // true when user explicitly set via WithMaxConcurrency
	completion        CompletionConfig
	itemNamer         func(index int) string
	itemSerdes        Serdes
	resultSerdes      Serdes
	nesting           NestingMode
}

type batchOptionFunc func(*batchOptions)

func (f batchOptionFunc) applyBatch(o *batchOptions) { f(o) }

func resolveBatchOptions(ec *execContext, opts []BatchOption) batchOptions {
	o := batchOptions{
		maxConcurrency: -1, // unlimited by default
		itemSerdes:     ec.serdes,
	}
	for _, opt := range opts {
		opt.applyBatch(&o)
	}
	return o
}

// executeBatchItems runs the core batch loop: schedule items up to max
// concurrency, collect results, check completion conditions.
//
// The runItem function receives the child context and the item index and
// must return the item result or error.
func executeBatchItems[I, O any](
	ec *execContext,
	parentID, parentName string,
	totalItems int,
	options batchOptions,
	parentSubType, childSubType string,
	runItem func(childCtx Context, index int) (O, error),
) (BatchResult[O], error) {
	// Items collects results in input order. We allocate for all items
	// but only fill the ones that actually start.
	results := make([]BatchItem[O], 0, totalItems)

	var (
		successCount int
		failureCount int
		reason       = CompletionAllCompleted
		reasonLocked bool // set at the moment the completion decision is made
	)

	// Sequential execution path (max-concurrency = 1 or all items sequential).
	// Also used when max-concurrency >= totalItems (effectively unlimited).
	concurrency := options.maxConcurrency
	if concurrency < 0 {
		concurrency = totalItems
	}

	if concurrency == 1 {
		// Sequential path: simpler, no goroutines needed.

		// For FLAT nesting, create a shared context whose prefix is the
		// parent batch ID. All iterations share this context so their
		// operations are minted sequentially under the parent (no per-
		// iteration context events).
		var flatCtx *execContext
		if options.nesting == NestingFlat {
			flatMode := childReplayMode(ec, parentID, ec.state.get(parentID))
			flatCtx = ec.child(parentID, ec.owner, flatMode)
		}

		for i := 0; i < totalItems; i++ {
			// Check if we should stop scheduling.
			if reasonLocked {
				break
			}

			itemName := itemNameForIndex(options, i)
			var result BatchItem[O]
			var err error
			if options.nesting == NestingFlat {
				result, err = runFlatBatchItemShared[O](flatCtx, parentID, i, itemName, options, runItem)
			} else {
				result, err = runNestedBatchItem[O](ec, parentID, parentName, itemName, i, options, childSubType, runItem)
			}
			if err != nil {
				// Suspension propagates.
				if errors.Is(err, errSuspendExecution) {
					return BatchResult[O]{}, err
				}
				return BatchResult[O]{}, err
			}

			results = append(results, result)
			switch result.Status {
			case BatchItemSucceeded:
				successCount++
			case BatchItemFailed:
				failureCount++
			}

			// Check completion conditions AFTER recording the result.
			if shouldStopMin(options.completion, successCount) {
				reason = CompletionMinSuccessfulReached
				reasonLocked = true
			} else if shouldStopFailure(options.completion, failureCount, totalItems) {
				reason = CompletionFailureToleranceExceeded
				reasonLocked = true
			}
		}
	} else {
		// Concurrent path: a coordinator loop on the calling goroutine
		// owns all completion state, so no lock is needed. Worker
		// goroutines only run an item body and report its outcome on a
		// channel. Child operation ids are claimed up front in index
		// order so ids mint deterministically across invocations, but a
		// branch is checkpointed STARTED and dispatched only when the
		// coordinator admits it. Once the completion decision fires the
		// coordinator stops admitting and abandons the branches still in
		// flight: it stops awaiting them, marks them started, and does not
		// count them. It still drains every dispatched worker before
		// returning, so no branch outlives the invocation and the parent
		// SUCCEEDED checkpoint is written only after every child
		// checkpoint has landed.
		type itemOutcome struct {
			index int
			item  BatchItem[O]
			err   error
		}

		type preClaimedItem struct {
			index    int
			name     string
			childID  string
			op       *operation
			terminal bool
		}

		preClaimed := make([]preClaimedItem, 0, totalItems)
		for i := 0; i < totalItems; i++ {
			itemName := itemNameForIndex(options, i)
			childID, claimErr := ec.claimOperation()
			if claimErr != nil {
				return BatchResult[O]{}, claimErr
			}
			op := ec.state.get(childID)
			preClaimed = append(preClaimed, preClaimedItem{
				index: i, name: itemName, childID: childID, op: op,
				terminal: op != nil && op.status.terminal(),
			})
		}

		abandon := new(atomic.Bool)
		ec.suspend.registerHandle(abandon, ec.abandon)
		outcomeCh := make(chan itemOutcome, totalItems)
		var wg sync.WaitGroup

		accepted := make(map[int]BatchItem[O], totalItems)
		startedIdx := make(map[int]struct{}, totalItems)
		nextToAdmit := 0
		inFlight := 0  // goroutines not yet drained from outcomeCh
		slotsUsed := 0 // concurrency slots held (terminal release only)
		var admitErr error

		admit := func() {
			for nextToAdmit < len(preClaimed) && slotsUsed < concurrency && !reasonLocked && admitErr == nil {
				pc := preClaimed[nextToAdmit]
				nextToAdmit++

				// Checkpoint the child START for a fresh NORMAL branch on
				// this goroutine before dispatch, so start events mint in
				// index order. A branch that is never admitted (early
				// completion) leaves no checkpoint and is omitted.
				if options.nesting != NestingFlat && !pc.terminal && pc.op == nil {
					update := batchChildUpdate(ec, pc.childID, pc.name, childSubType, parentID, types.OperationActionStart)
					if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
						admitErr = err
						return
					}
				}

				startedIdx[pc.index] = struct{}{}
				inFlight++
				slotsUsed++
				// Register the worker as an active branch before launching
				// its goroutine, so the coordinator's view of the count
				// never races the goroutine start. The token is released on
				// worker unwind and by a pending callback's pre-result hook;
				// its sync.Once makes that exactly one deregistration.
				tok := ec.suspend.registerBranchToken()
				wg.Add(1)
				go func(pc preClaimedItem, tok *branchToken) {
					defer wg.Done()
					defer tok.release()
					var item BatchItem[O]
					var runErr error
					func() {
						defer func() {
							if r := recover(); r != nil {
								runErr = fmt.Errorf("durable: batch item %d panicked: %v", pc.index, r)
							}
						}()
						item, runErr = runPreClaimedBatchItem[O](ec, parentID, pc.childID, pc.name, pc.index, pc.op, pc.terminal, options, childSubType, runItem, abandon, tok)
					}()
					if runErr != nil {
						outcomeCh <- itemOutcome{index: pc.index, err: runErr}
						return
					}
					outcomeCh <- itemOutcome{index: pc.index, item: item}
				}(pc, tok)
			}
		}

		admit()

		var fatalErr error
		sawSuspend := false
		for inFlight > 0 {
			out := <-outcomeCh
			inFlight--
			switch {
			case out.err != nil:
				if errors.Is(out.err, errSuspendExecution) {
					// Suspended (or abandoned via the abandon signal,
					// which reuses errSuspendExecution). The branch
					// retains its concurrency slot so no replacement is
					// admitted.
					sawSuspend = true
				} else {
					// Non-suspension error: terminal from the batch's
					// perspective, release the slot.
					slotsUsed--
					if fatalErr == nil {
						fatalErr = out.err
					}
				}
			case reasonLocked:
				// A terminal outcome that raced the completion decision:
				// the branch is abandoned (reported started, not counted).
				slotsUsed--
			default:
				accepted[out.index] = out.item
				slotsUsed--
				switch out.item.Status {
				case BatchItemSucceeded:
					successCount++
				case BatchItemFailed:
					failureCount++
				}
				if shouldStopMin(options.completion, successCount) {
					reason = CompletionMinSuccessfulReached
					reasonLocked = true
				} else if shouldStopFailure(options.completion, failureCount, totalItems) {
					reason = CompletionFailureToleranceExceeded
					reasonLocked = true
				}
				if reasonLocked {
					// Stop awaiting the branches still in flight: they
					// unwind at their next operation without starting new
					// work.
					abandon.Store(true)
				}
			}
			if fatalErr == nil {
				admit()
			}
		}
		// Every dispatched worker has reported; ensure none is still
		// unwinding before the parent context is checkpointed terminal.
		wg.Wait()

		// A branch abandoned after early completion may have already
		// committed the invocation to PENDING (a wait, invoke, callback
		// or retry that suspended before the completion decision fired).
		// Now that every worker has drained, retire those commitments so
		// abandoned work does not force the whole invocation to PENDING.
		if reasonLocked {
			ec.suspend.retireCommitment(abandon)
		} else if !sawSuspend {
			// The batch completed with no outstanding commitment under its
			// handle. Drop the parentage entry so it does not accumulate
			// across the many batches of a long-lived invocation. A batch
			// that suspends across invocations keeps its entry so an
			// enclosing batch can still cascade retirement to it.
			ec.suspend.forgetHandle(abandon)
		}

		if admitErr != nil {
			return BatchResult[O]{}, admitErr
		}
		if fatalErr != nil {
			return BatchResult[O]{}, fatalErr
		}
		// Completion was not reached but a branch suspended: the batch
		// spans invocations. Suspend and resume later; nothing is
		// abandoned yet.
		if !reasonLocked && sawSuspend {
			return BatchResult[O]{}, errSuspendExecution
		}

		// Assemble in input order: terminal outcomes are counted;
		// started-but-abandoned branches are reported STARTED; branches
		// that never started are omitted.
		for i := 0; i < totalItems; i++ {
			if item, ok := accepted[i]; ok {
				results = append(results, item)
				continue
			}
			if _, ok := startedIdx[i]; ok {
				results = append(results, BatchItem[O]{
					Index:  i,
					Name:   itemNameForIndex(options, i),
					Status: BatchItemStarted,
				})
			}
		}
	}

	batchResult := BatchResult[O]{
		Items:  results,
		Reason: reason,
	}

	return checkpointBatchSuccess(ec, parentID, parentName, parentSubType, batchResult, options)
}

// runPreClaimedBatchItem runs a batch item whose operation ID was already
// claimed on the owning goroutine. Used by the concurrent path.
func runPreClaimedBatchItem[O any](
	ec *execContext,
	parentID, childID, itemName string,
	index int,
	op *operation,
	terminal bool,
	options batchOptions,
	childSubType string,
	runItem func(childCtx Context, index int) (O, error),
	abandon *atomic.Bool,
	tok *branchToken,
) (BatchItem[O], error) {
	if err := validateReplayConsistency(op, string(types.OperationTypeContext), childSubType, itemName); err != nil {
		return BatchItem[O]{}, err
	}
	if options.nesting == NestingFlat {
		// FLAT mode: run in a virtual child context.
		mode := childReplayMode(ec, childID, op)
		virtualChild := ec.child(childID, currentGoroutineOwner(), mode)
		virtualChild.abandon = abandon
		virtualChild.branchTok = tok
		result, fnErr := runItem(virtualChild, index)
		if fnErr != nil {
			if errors.Is(fnErr, errSuspendExecution) {
				return BatchItem[O]{}, fnErr
			}
			return BatchItem[O]{
				Index:  index,
				Name:   itemName,
				Status: BatchItemFailed,
				Err:    fnErr,
			}, nil
		}
		serialized, serErr := options.itemSerdes.Marshal(ec.Context, ec.serdesCtx(childID), result)
		if serErr != nil {
			return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: serialize result: %w", index, serErr)
		}
		var out O
		if err := options.itemSerdes.Unmarshal(ec.Context, ec.serdesCtx(childID), serialized, &out); err != nil {
			return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: deserialize result: %w", index, err)
		}
		return BatchItem[O]{
			Index:  index,
			Name:   itemName,
			Status: BatchItemSucceeded,
			Result: out,
		}, nil
	}

	// NORMAL mode: child context with full checkpointing.
	if terminal {
		return replayTerminalChildItem[O](ec, op, childID, itemName, index, options, childSubType, runItem)
	}

	// The child context runs on this goroutine; capture ownership here.
	mode := childReplayMode(ec, childID, op)
	child := ec.child(childID, currentGoroutineOwner(), mode)
	child.abandon = abandon
	child.branchTok = tok

	result, fnErr := runItem(child, index)
	if fnErr != nil {
		if errors.Is(fnErr, errSuspendExecution) {
			return BatchItem[O]{}, fnErr
		}
		update := batchChildUpdate(ec, childID, itemName, childSubType, parentID, types.OperationActionFail)
		update.Error = errorObject(fnErr)
		update.Error.ErrorData = encodeChildErrorData(fnErr)
		if cerr := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); cerr != nil {
			return BatchItem[O]{}, cerr
		}
		return BatchItem[O]{
			Index:  index,
			Name:   itemName,
			Status: BatchItemFailed,
			Err:    &ChildContextError{Name: itemName, Err: fnErr},
		}, nil
	}

	serialized, serErr := options.itemSerdes.Marshal(ec.Context, ec.serdesCtx(childID), result)
	if serErr != nil {
		return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: serialize result: %w", index, serErr)
	}
	update := batchChildUpdate(ec, childID, itemName, childSubType, parentID, types.OperationActionSucceed)
	if len(serialized) > checkpointSizeLimitBytes {
		update.ContextOptions = &types.ContextOptions{ReplayChildren: aws.Bool(true)}
	} else {
		update.Payload = aws.String(string(serialized))
	}
	if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
		return BatchItem[O]{}, err
	}
	var out O
	if err := options.itemSerdes.Unmarshal(ec.Context, ec.serdesCtx(childID), serialized, &out); err != nil {
		return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: deserialize result: %w", index, err)
	}
	return BatchItem[O]{
		Index:  index,
		Name:   itemName,
		Status: BatchItemSucceeded,
		Result: out,
	}, nil
}

// runFlatBatchItem runs an item in FLAT nesting mode: no child context,
// operations are checkpointed under the parent's ID space using a virtual
// child (no ContextStarted/Succeeded events).
func runFlatBatchItem[O any](
	ec *execContext,
	parentID string,
	index int,
	itemName string,
	options batchOptions,
	runItem func(childCtx Context, index int) (O, error),
) (BatchItem[O], error) {
	// In FLAT mode, the item's operations are minted under the parent
	// context's ID space. We create a virtual child that shares the
	// parent's opIDs prefix but does NOT checkpoint its own context events.
	// The ID for the virtual child is just the next parent-level op ID.
	childID, err := ec.claimOperation()
	if err != nil {
		return BatchItem[O]{}, err
	}

	// Check for replay: if this virtual child's first op is checkpointed,
	// it's in replay mode.
	mode := childReplayMode(ec, childID, ec.state.get(childID))
	virtualChild := ec.child(childID, ec.owner, mode)

	result, fnErr := runItem(virtualChild, index)
	if fnErr != nil {
		if errors.Is(fnErr, errSuspendExecution) {
			return BatchItem[O]{}, fnErr
		}
		return BatchItem[O]{
			Index:  index,
			Name:   itemName,
			Status: BatchItemFailed,
			Err:    fnErr,
		}, nil
	}

	// Round-trip through serdes.
	serialized, serErr := options.itemSerdes.Marshal(ec.Context, ec.serdesCtx(childID), result)
	if serErr != nil {
		return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: serialize result: %w", index, serErr)
	}
	var out O
	if err := options.itemSerdes.Unmarshal(ec.Context, ec.serdesCtx(childID), serialized, &out); err != nil {
		return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: deserialize result: %w", index, err)
	}

	return BatchItem[O]{
		Index:  index,
		Name:   itemName,
		Status: BatchItemSucceeded,
		Result: out,
	}, nil
}

// runFlatBatchItemShared runs an item in FLAT nesting mode using a shared
// context whose prefix is the parent batch's entity ID. Operations inside
// the item are minted sequentially under the parent, with no per-iteration
// context events. The shared context's counter advances across iterations.
func runFlatBatchItemShared[O any](
	flatCtx *execContext,
	parentID string,
	index int,
	itemName string,
	options batchOptions,
	runItem func(childCtx Context, index int) (O, error),
) (BatchItem[O], error) {
	var result O
	var fnErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				fnErr = fmt.Errorf("durable: batch item %d panicked: %v", index, r)
			}
		}()
		result, fnErr = runItem(flatCtx, index)
	}()
	if fnErr != nil {
		if errors.Is(fnErr, errSuspendExecution) {
			return BatchItem[O]{}, fnErr
		}
		return BatchItem[O]{
			Index:  index,
			Name:   itemName,
			Status: BatchItemFailed,
			Err:    fnErr,
		}, nil
	}

	// Round-trip through serdes.
	serialized, serErr := options.itemSerdes.Marshal(flatCtx.Context, flatCtx.serdesCtx(parentID), result)
	if serErr != nil {
		return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: serialize result: %w", index, serErr)
	}
	var out O
	if err := options.itemSerdes.Unmarshal(flatCtx.Context, flatCtx.serdesCtx(parentID), serialized, &out); err != nil {
		return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: deserialize result: %w", index, err)
	}

	return BatchItem[O]{
		Index:  index,
		Name:   itemName,
		Status: BatchItemSucceeded,
		Result: out,
	}, nil
}

// runNestedBatchItem runs an item in NORMAL nesting mode: full child context
// with ContextStarted/ContextSucceeded or ContextFailed events.
func runNestedBatchItem[O any](
	ec *execContext,
	parentID, parentName, itemName string,
	index int,
	options batchOptions,
	childSubType string,
	runItem func(childCtx Context, index int) (O, error),
) (item BatchItem[O], retErr error) {
	// Claim the child's operation ID from the parent.
	childID, err := ec.claimOperation()
	if err != nil {
		return BatchItem[O]{}, err
	}

	op := ec.state.get(childID)
	if err := validateReplayConsistency(op, string(types.OperationTypeContext), childSubType, itemName); err != nil {
		return BatchItem[O]{}, err
	}
	if op != nil && op.status.terminal() {
		return replayTerminalChildItem[O](ec, op, childID, itemName, index, options, childSubType, runItem)
	}

	// Checkpoint child context START.
	if op == nil {
		update := batchChildUpdate(ec, childID, itemName, childSubType, parentID, types.OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
			return BatchItem[O]{}, err
		}
	}

	mode := childReplayMode(ec, childID, op)
	child := ec.child(childID, ec.owner, mode)

	// Recover panics in the item function so they become failures, not
	// process crashes.
	var result O
	var fnErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				fnErr = fmt.Errorf("durable: batch item %d panicked: %v", index, r)
			}
		}()
		result, fnErr = runItem(child, index)
	}()

	if fnErr != nil {
		if errors.Is(fnErr, errSuspendExecution) {
			return BatchItem[O]{}, fnErr
		}
		// Checkpoint the failure.
		update := batchChildUpdate(ec, childID, itemName, childSubType, parentID, types.OperationActionFail)
		update.Error = errorObject(fnErr)
		update.Error.ErrorData = encodeChildErrorData(fnErr)
		if cerr := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); cerr != nil {
			return BatchItem[O]{}, cerr
		}
		return BatchItem[O]{
			Index:  index,
			Name:   itemName,
			Status: BatchItemFailed,
			Err:    &ChildContextError{Name: itemName, Err: fnErr},
		}, nil
	}

	// Serialize and checkpoint the child success.
	serialized, serErr := options.itemSerdes.Marshal(ec.Context, ec.serdesCtx(childID), result)
	if serErr != nil {
		return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: serialize result: %w", index, serErr)
	}

	update := batchChildUpdate(ec, childID, itemName, childSubType, parentID, types.OperationActionSucceed)
	if len(serialized) > checkpointSizeLimitBytes {
		update.ContextOptions = &types.ContextOptions{ReplayChildren: aws.Bool(true)}
	} else {
		update.Payload = aws.String(string(serialized))
	}
	if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
		return BatchItem[O]{}, err
	}

	// Round-trip through serdes for live == replay consistency.
	var out O
	if err := options.itemSerdes.Unmarshal(ec.Context, ec.serdesCtx(childID), serialized, &out); err != nil {
		return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: deserialize result: %w", index, err)
	}

	return BatchItem[O]{
		Index:  index,
		Name:   itemName,
		Status: BatchItemSucceeded,
		Result: out,
	}, nil
}

// replayTerminalChildItem resolves a child item that already has a terminal
// status in the checkpoint log.
func replayTerminalChildItem[O any](
	ec *execContext,
	op *operation,
	childID, itemName string,
	index int,
	options batchOptions,
	childSubType string,
	runItem func(childCtx Context, index int) (O, error),
) (BatchItem[O], error) {
	switch op.status {
	case statusSucceeded:
		if op.childCtx == nil {
			return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: checkpointed SUCCEEDED with no context details", index)
		}
		if op.childCtx.replayChildren {
			mode := modeReplaySucceededContext
			child := ec.child(childID, ec.owner, mode)
			result, err := runItem(child, index)
			if err != nil {
				return BatchItem[O]{}, err
			}
			return BatchItem[O]{
				Index:  index,
				Name:   itemName,
				Status: BatchItemSucceeded,
				Result: result,
			}, nil
		}
		var out O
		if err := options.itemSerdes.Unmarshal(ec.Context, ec.serdesCtx(childID), []byte(op.childCtx.result), &out); err != nil {
			return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: deserialize result: %w", index, err)
		}
		return BatchItem[O]{
			Index:  index,
			Name:   itemName,
			Status: BatchItemSucceeded,
			Result: out,
		}, nil

	case statusFailed:
		errType := "Error"
		errMessage := "item failed"
		var errData string
		if op.childCtx != nil {
			errType = op.childCtx.errType
			errMessage = op.childCtx.errMessage
			errData = op.childCtx.errData
		}
		return BatchItem[O]{
			Index:  index,
			Name:   itemName,
			Status: BatchItemFailed,
			Err:    &ChildContextError{Name: itemName, Err: reconstructInnerError(errType, errMessage, childErrorData{}, errData)},
		}, nil

	default:
		return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: unexpected terminal status %s", index, op.status)
	}
}

// replayTerminalBatch handles a batch whose parent context is already
// terminal in the checkpoint log.
func replayTerminalBatch[I, O any](
	ec *execContext,
	op *operation,
	id, name string,
	items []I,
	fn func(Context, I, int) (O, error),
	options batchOptions,
	parentSubType, childSubType string,
) (BatchResult[O], error) {
	switch op.status {
	case statusSucceeded:
		if op.childCtx == nil {
			return BatchResult[O]{}, fmt.Errorf("durable: batch %q: checkpointed SUCCEEDED with no context details", name)
		}
		// ReplayChildren mode: the full aggregate was too large to store,
		// so reconstruct from the children. With a decision record and
		// NORMAL nesting, replay exactly the recorded admitted set so the
		// result matches the live shape (including abandoned branches).
		// Otherwise (FLAT, or an older checkpoint without a record) fall
		// back to sequential re-execution.
		if op.childCtx.replayChildren {
			if options.nesting != NestingFlat {
				if record, ok := parseBatchReplayRecord(op.childCtx.result); ok {
					return replayBatchChildrenFromRecord[I, O](ec, record, items, fn, options, childSubType)
				}
			}
			mode := modeReplaySucceededContext
			child := ec.child(id, ec.owner, mode)
			return replayBatchChildren[I, O](child, id, name, items, fn, options, parentSubType, childSubType)
		}
		// If an operation-level serdes is configured, use it to
		// deserialize the whole batch result.
		if options.resultSerdes != nil {
			var result BatchResult[O]
			if err := options.resultSerdes.Unmarshal(ec.Context, ec.serdesCtx(id), []byte(op.childCtx.result), &result); err != nil {
				return BatchResult[O]{}, fmt.Errorf("durable: batch %q: deserialize batch result: %w", name, err)
			}
			return result, nil
		}
		// Normal replay: deserialize the stored aggregate result.
		// The parent checkpoint stores a JSON-serialized batch summary
		// using the batchCheckpointPayload envelope (lowercase "results"
		// and "reason" JSON keys).
		var payload batchCheckpointPayload
		if err := json.Unmarshal([]byte(op.childCtx.result), &payload); err != nil {
			// Fall back to re-executing children if the stored
			// payload is not the batch summary (could be legacy).
			mode := modeReplaySucceededContext
			child := ec.child(id, ec.owner, mode)
			return replayBatchChildren[I, O](child, id, name, items, fn, options, parentSubType, childSubType)
		}
		return toBatchResult[O](ec.Context, payload, options.itemSerdes, ec.serdesCtx(id))

	case statusFailed:
		errType := "Error"
		errMessage := "batch failed"
		var errData string
		if op.childCtx != nil {
			errType = op.childCtx.errType
			errMessage = op.childCtx.errMessage
			errData = op.childCtx.errData
		}
		return BatchResult[O]{}, &ChildContextError{Name: name, Err: reconstructInnerError(errType, errMessage, childErrorData{}, errData)}

	default:
		return BatchResult[O]{}, fmt.Errorf("durable: batch %q: unexpected terminal status %s", name, op.status)
	}
}

// replayBatchChildren re-executes batch children to reconstruct the batch
// result from child replay.
func replayBatchChildren[I, O any](
	ec *execContext,
	parentID, parentName string,
	items []I,
	fn func(Context, I, int) (O, error),
	options batchOptions,
	parentSubType, childSubType string,
) (BatchResult[O], error) {
	// Determine total items based on whether we have items (Map) or not (Parallel).
	totalItems := len(items)
	if totalItems == 0 {
		return BatchResult[O]{Items: nil, Reason: CompletionAllCompleted}, nil
	}

	results := make([]BatchItem[O], 0, totalItems)
	var successCount, failureCount int
	reason := CompletionAllCompleted

	for i := 0; i < totalItems; i++ {
		itemName := itemNameForIndex(options, i)
		var result BatchItem[O]
		var err error

		if options.nesting == NestingFlat {
			result, err = runFlatBatchItem[O](ec, parentID, i, itemName, options, func(ctx Context, idx int) (O, error) {
				if fn != nil && items != nil {
					return fn(ctx, items[idx], idx)
				}
				var zero O
				return zero, fmt.Errorf("durable: cannot replay parallel branches without branch functions")
			})
		} else {
			result, err = runNestedBatchItem[O](ec, parentID, parentName, itemName, i, options, childSubType, func(ctx Context, idx int) (O, error) {
				if fn != nil && items != nil {
					return fn(ctx, items[idx], idx)
				}
				var zero O
				return zero, fmt.Errorf("durable: cannot replay parallel branches without branch functions")
			})
		}

		if err != nil {
			return BatchResult[O]{}, err
		}

		results = append(results, result)
		switch result.Status {
		case BatchItemSucceeded:
			successCount++
		case BatchItemFailed:
			failureCount++
		}

		if shouldStopMin(options.completion, successCount) {
			reason = CompletionMinSuccessfulReached
			break
		}
		if shouldStopFailure(options.completion, failureCount, totalItems) {
			reason = CompletionFailureToleranceExceeded
			break
		}
	}

	return BatchResult[O]{Items: results, Reason: reason}, nil
}

// checkpointBatchSuccess checkpoints the parent batch context as SUCCEEDED
// and returns the final BatchResult.
func checkpointBatchSuccess[O any](
	ec *execContext,
	id, name, subType string,
	result BatchResult[O],
	options batchOptions,
) (BatchResult[O], error) {
	// Serialize the result. If an operation-level serdes is provided,
	// use it; otherwise serialize a default JSON summary.
	var serialized []byte
	var serErr error

	if options.resultSerdes != nil {
		serialized, serErr = options.resultSerdes.Marshal(ec.Context, ec.serdesCtx(id), result)
	} else {
		payload, payloadErr := fromBatchResult(ec.Context, result, options.itemSerdes, ec.serdesCtx(id))
		if payloadErr != nil {
			return BatchResult[O]{}, payloadErr
		}
		serialized, serErr = json.Marshal(payload)
	}
	if serErr != nil {
		return BatchResult[O]{}, fmt.Errorf("durable: batch %q: serialize result: %w", name, serErr)
	}

	update := batchParentUpdate(ec, id, name, subType, types.OperationActionSucceed)
	if len(serialized) > checkpointSizeLimitBytes {
		update.ContextOptions = &types.ContextOptions{ReplayChildren: aws.Bool(true)}
		// The full aggregate is too large to store, so record a
		// size-independent decision record alongside ReplayChildren. It
		// carries the completion reason and which admitted branches were
		// abandoned (STARTED) versus terminal, so replay reconstructs the
		// exact live shape instead of re-deriving it. Only NORMAL children
		// have per-child checkpoints to reconstruct from; FLAT batches fall
		// back to sequential re-execution on replay.
		if options.nesting != NestingFlat {
			recordBytes, recordErr := json.Marshal(newBatchReplayRecord(result))
			if recordErr != nil {
				return BatchResult[O]{}, fmt.Errorf("durable: batch %q: serialize replay record: %w", name, recordErr)
			}
			update.Payload = aws.String(string(recordBytes))
		}
	} else {
		update.Payload = aws.String(string(serialized))
	}
	if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
		return BatchResult[O]{}, err
	}

	return result, nil
}

// batchCheckpointPayload is the JSON structure stored as the parent batch
// context's checkpoint payload. It uses checkpoint-safe item representations
// that avoid interface fields (error) which cannot round-trip through JSON.
type batchCheckpointPayload struct {
	Results []batchCheckpointItem `json:"results"`
	Reason  CompletionReason      `json:"reason"`
}

// batchCheckpointItem is the per-item representation in the checkpoint
// payload. Unlike [BatchItem], it replaces the error interface with
// serializable error type/message strings.
//
// The embedded childErrorData carries inner wrapper metadata, persisted
// when ErrType is a known SDK wrapper (StepError, WaitForConditionError,
// CallbackError, SerdesError) so replay can reconstruct the concrete type
// with matching field values, causes, and sentinels. Its exported fields
// flatten into this object's JSON, keeping the historical layout. The
// item's own Name (JSON "name") shadows the wrapper name (JSON
// "stepName"); access the latter as cp.childErrorData.Name.
type batchCheckpointItem struct {
	Index      int             `json:"index"`
	Name       string          `json:"name,omitempty"`
	Status     BatchItemStatus `json:"status"`
	Result     string          `json:"result,omitempty"`
	ErrType    string          `json:"errType,omitempty"`
	ErrMessage string          `json:"errMessage,omitempty"`

	childErrorData
}

// toBatchResult converts a deserialized checkpoint payload back into a
// typed [BatchResult] using the provided item serdes for result values.
func toBatchResult[O any](ctx context.Context, payload batchCheckpointPayload, itemSerdes Serdes, sctx SerdesContext) (BatchResult[O], error) {
	items := make([]BatchItem[O], len(payload.Results))
	for i, cp := range payload.Results {
		items[i] = BatchItem[O]{
			Index:  cp.Index,
			Name:   cp.Name,
			Status: cp.Status,
		}
		switch cp.Status {
		case BatchItemSucceeded:
			var out O
			if cp.Result != "" {
				if err := itemSerdes.Unmarshal(ctx, sctx, []byte(cp.Result), &out); err != nil {
					return BatchResult[O]{}, fmt.Errorf("durable: batch item %d: deserialize checkpointed result: %w", i, err)
				}
			}
			items[i].Result = out
		case BatchItemFailed:
			items[i].Err = &ChildContextError{
				Name: cp.Name,
				Err:  reconstructInnerError(cp.ErrType, cp.ErrMessage, cp.childErrorData, ""),
			}
		}
	}
	return BatchResult[O]{Items: items, Reason: payload.Reason}, nil
}

// maxInnerErrMessageBytes is the ceiling for the persisted inner error
// message. Applied to both ErrorData (wire) and batchCheckpointItem
// (aggregate payload) so the two routes stay consistent.
const maxInnerErrMessageBytes = 1024

// truncateUTF8 returns s truncated to at most maxBytes, cutting on a
// rune boundary so the result is always valid UTF-8.
func truncateUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	// Walk backward from the limit to find the last valid rune start.
	for maxBytes > 0 && !utf8.RuneStart(s[maxBytes]) {
		maxBytes--
	}
	return s[:maxBytes]
}

// childErrorData carries inner wrapper metadata through ErrorData on child
// FAIL updates. The same fields are persisted in the aggregate checkpoint
// payload (batchCheckpointItem); this struct is used for the per-child
// ErrorObject.ErrorData so Route B (wire-only replay) can reconstruct the
// same concrete wrapper chain as Route A.
//
// Name and Attempts keep the historical "stepName"/"stepAttempts" JSON keys
// (from when StepError was the only reconstructed wrapper) so checkpoints
// written by earlier builds keep reconstructing; they now carry the
// corresponding field of whichever wrapper failed the item.
type childErrorData struct {
	// Name is the wrapper operation's name: StepError.Name,
	// WaitForConditionError.Name, CallbackError.Name, or
	// SerdesError.Operation.
	Name string `json:"stepName,omitempty"`

	// Attempts is StepError.Attempts or WaitForConditionError.Attempts.
	Attempts int `json:"stepAttempts,omitempty"`

	// CallbackID is CallbackError.CallbackID.
	CallbackID string `json:"callbackId,omitempty"`

	// Direction is SerdesError.Direction.
	Direction string `json:"direction,omitempty"`

	// TimedOut records that a CallbackError matched [ErrCallbackTimedOut],
	// so replay re-attaches the sentinel for errors.Is.
	TimedOut bool `json:"timedOut,omitempty"`

	InnerErrType    string `json:"innerErrType,omitempty"`
	InnerErrMessage string `json:"innerErrMessage,omitempty"`
}

// isZero reports whether no metadata field is set.
func (d childErrorData) isZero() bool {
	return d == childErrorData{}
}

// setInner records the wrapper's cause as the reconstructable leaf.
func (d *childErrorData) setInner(cause error) {
	if cause == nil {
		return
	}
	d.InnerErrType = errorTypeName(cause)
	d.InnerErrMessage = truncateUTF8(cause.Error(), maxInnerErrMessageBytes)
}

// wrapperErrorData extracts reconstructable metadata from a known SDK
// wrapper error. It switches on the error's outermost concrete type — the
// same identity [errorTypeName] records as the checkpointed ErrorType — so
// the metadata always describes the type that [reconstructInnerError] will
// rebuild. The second result is false for other error types.
func wrapperErrorData(err error) (childErrorData, bool) {
	var d childErrorData
	switch e := err.(type) {
	case *StepError:
		d.Name = e.Name
		d.Attempts = e.Attempts
		d.setInner(e.Err)
	case *WaitForConditionError:
		d.Name = e.Name
		d.Attempts = e.Attempts
		d.setInner(e.Err)
	case *CallbackError:
		d.Name = e.Name
		d.CallbackID = e.CallbackID
		d.TimedOut = errors.Is(e, ErrCallbackTimedOut)
		d.setInner(e.Err)
	case *SerdesError:
		d.Name = e.Operation
		d.Direction = e.Direction
		d.setInner(e.Err)
	default:
		return childErrorData{}, false
	}
	return d, true
}

// encodeChildErrorData marshals inner wrapper metadata as a JSON string
// suitable for ErrorObject.ErrorData. Returns nil if the error does not
// carry reconstructable inner wrapper data.
func encodeChildErrorData(err error) *string {
	d, ok := wrapperErrorData(err)
	if !ok {
		return nil
	}
	raw, marshalErr := json.Marshal(d)
	if marshalErr != nil {
		return nil
	}
	s := string(raw)
	return &s
}

// reconstructInnerError rebuilds the concrete SDK wrapper type from
// checkpointed error metadata. Known SDK wrapper types ([StepError],
// [WaitForConditionError], [CallbackError], [SerdesError]) are
// reconstructed so errors.As — and, for a timed-out callback,
// errors.Is(err, [ErrCallbackTimedOut]) — succeed after replay, matching
// live behavior. Unknown or user-defined error types remain as
// replayedError carrying the type name as a string.
//
// meta carries the wrapper field values from the aggregate checkpoint
// payload; errData carries the JSON-encoded childErrorData from the
// ErrorObject.ErrorData field. When meta is empty and errData is present
// and parseable (Route B, wire-only replay), errData supplies the values.
// When both are absent or malformed, fields fall back to zero values —
// the function never fails.
func reconstructInnerError(errType, errMessage string, meta childErrorData, errData string) error {
	if errData != "" && meta.isZero() {
		var d childErrorData
		if json.Unmarshal([]byte(errData), &d) == nil {
			meta = d
		}
	}

	leaf := func() *replayedError {
		if meta.InnerErrType != "" || meta.InnerErrMessage != "" {
			return &replayedError{errType: meta.InnerErrType, message: meta.InnerErrMessage}
		}
		return &replayedError{errType: "Error", message: errMessage}
	}

	switch errType {
	case "StepError":
		return &StepError{Name: meta.Name, Attempts: meta.Attempts, Err: leaf()}
	case "WaitForConditionError":
		return &WaitForConditionError{Name: meta.Name, Attempts: meta.Attempts, Err: leaf()}
	case "CallbackError":
		cause := leaf()
		if meta.TimedOut {
			cause.sentinel = ErrCallbackTimedOut
		}
		return &CallbackError{Name: meta.Name, CallbackID: meta.CallbackID, Err: cause}
	case "SerdesError":
		return &SerdesError{Operation: meta.Name, Direction: meta.Direction, Err: leaf()}
	default:
		return &replayedError{errType: errType, message: errMessage}
	}
}

// fromBatchResult converts a live [BatchResult] into the checkpoint payload
// format for serialization. Item results are pre-serialized through the
// provided item serdes.
func fromBatchResult[O any](ctx context.Context, result BatchResult[O], itemSerdes Serdes, sctx SerdesContext) (batchCheckpointPayload, error) {
	cpItems := make([]batchCheckpointItem, len(result.Items))
	for i, item := range result.Items {
		cpItems[i] = batchCheckpointItem{
			Index:  item.Index,
			Name:   item.Name,
			Status: item.Status,
		}
		switch item.Status {
		case BatchItemSucceeded:
			raw, err := itemSerdes.Marshal(ctx, sctx, item.Result)
			if err != nil {
				return batchCheckpointPayload{}, fmt.Errorf("durable: batch item %d: serialize result for checkpoint: %w", i, err)
			}
			cpItems[i].Result = string(raw)
		case BatchItemFailed:
			if item.Err != nil {
				cpItems[i].ErrType = errorTypeName(item.Err)
				cpItems[i].ErrMessage = item.Err.Error()
				// Extract inner error details for child context errors.
				var childErr *ChildContextError
				if errors.As(item.Err, &childErr) && childErr.Err != nil {
					cpItems[i].ErrType = errorTypeName(childErr.Err)
					cpItems[i].ErrMessage = childErr.Err.Error()
					// Persist inner wrapper metadata for known SDK types
					// so replay reconstructs the concrete wrapper chain
					// with matching fields, causes, and sentinels.
					if meta, ok := wrapperErrorData(childErr.Err); ok {
						cpItems[i].childErrorData = meta
					}
				}
			}
		}
	}
	return batchCheckpointPayload{Results: cpItems, Reason: result.Reason}, nil
}

// Index-set discriminators for batchReplayRecord.
const (
	replayIndexSetStarted   = "started"
	replayIndexSetCompleted = "completed"
)

// batchReplayRecord is the size-independent decision record stored on a
// batch parent context whose full aggregate exceeded the checkpoint size
// limit and was replaced by ReplayChildren. It captures the completion
// reason and, in index order, which admitted branches were abandoned
// (reported STARTED) versus reached a terminal status, so replay
// reconstructs the exact live result shape rather than re-deriving it.
//
// The admitted branches are the prefix [0, StartedTotal). Indexes holds
// whichever of the started (abandoned) or completed index sets is smaller,
// selected by IndexSet, so the record stays bounded by branch count and
// never by item payload size.
type batchReplayRecord struct {
	Reason       CompletionReason `json:"completionReason"`
	StartedTotal int              `json:"totalCount"`
	IndexSet     string           `json:"indexSet"`
	Indexes      []int            `json:"indexes"`
}

// newBatchReplayRecord builds the decision record from a live batch result.
// Never-started branches are already excluded from result.Items, so the
// item count is the started total.
func newBatchReplayRecord[O any](result BatchResult[O]) batchReplayRecord {
	var started, completed []int
	for i := range result.Items {
		if result.Items[i].Status == BatchItemStarted {
			started = append(started, result.Items[i].Index)
		} else {
			completed = append(completed, result.Items[i].Index)
		}
	}
	record := batchReplayRecord{
		Reason:       result.Reason,
		StartedTotal: len(result.Items),
	}
	if len(started) <= len(completed) {
		record.IndexSet = replayIndexSetStarted
		record.Indexes = started
	} else {
		record.IndexSet = replayIndexSetCompleted
		record.Indexes = completed
	}
	if record.Indexes == nil {
		record.Indexes = []int{}
	}
	return record
}

// parseBatchReplayRecord parses a decision record from a parent context's
// stored payload. The second result is false for absent, malformed, or
// out-of-range payloads (including checkpoints written before the record
// existed), so the caller can fall back to sequential re-execution.
func parseBatchReplayRecord(payload string) (batchReplayRecord, bool) {
	if payload == "" {
		return batchReplayRecord{}, false
	}
	var record batchReplayRecord
	if err := json.Unmarshal([]byte(payload), &record); err != nil {
		return batchReplayRecord{}, false
	}
	if record.StartedTotal < 0 {
		return batchReplayRecord{}, false
	}
	if record.IndexSet != replayIndexSetStarted && record.IndexSet != replayIndexSetCompleted {
		return batchReplayRecord{}, false
	}
	for _, idx := range record.Indexes {
		if idx < 0 || idx >= record.StartedTotal {
			return batchReplayRecord{}, false
		}
	}
	return record, true
}

// abandonedSet returns the set of admitted branch indexes that were
// reported STARTED (abandoned before reaching a terminal status).
func (r batchReplayRecord) abandonedSet() map[int]bool {
	set := make(map[int]bool, len(r.Indexes))
	if r.IndexSet == replayIndexSetStarted {
		for _, idx := range r.Indexes {
			set[idx] = true
		}
		return set
	}
	completed := make(map[int]bool, len(r.Indexes))
	for _, idx := range r.Indexes {
		completed[idx] = true
	}
	for i := 0; i < r.StartedTotal; i++ {
		if !completed[i] {
			set[i] = true
		}
	}
	return set
}

// replayBatchChildrenFromRecord reconstructs a batch result from the decision
// record: it replays exactly the recorded admitted set in index order rather
// than re-deriving completion from a threshold. Terminal branches are
// resolved from their own child checkpoints; abandoned branches are reported
// STARTED without re-running their bodies.
//
// Batch children mint their operation ids as siblings in the enclosing
// context, claimed in index order. This mirrors that id sequence with a
// detached minter so the enclosing counter is untouched; the caller advances
// it once for the whole batch.
func replayBatchChildrenFromRecord[I, O any](
	ec *execContext,
	record batchReplayRecord,
	items []I,
	fn func(Context, I, int) (O, error),
	options batchOptions,
	childSubType string,
) (BatchResult[O], error) {
	abandoned := record.abandonedSet()
	sib := &opIDs{prefix: ec.ids.prefix, counter: ec.ids.counter}
	runItem := func(childCtx Context, index int) (O, error) {
		if fn != nil && items != nil {
			return fn(childCtx, items[index], index)
		}
		var zero O
		return zero, fmt.Errorf("durable: cannot replay parallel branches without branch functions")
	}
	results := make([]BatchItem[O], 0, record.StartedTotal)
	for i := 0; i < record.StartedTotal; i++ {
		childID := sib.next()
		itemName := itemNameForIndex(options, i)
		if abandoned[i] {
			results = append(results, BatchItem[O]{
				Index:  i,
				Name:   itemName,
				Status: BatchItemStarted,
			})
			continue
		}
		op := ec.state.get(childID)
		if op == nil || !op.status.terminal() {
			return BatchResult[O]{}, fmt.Errorf("durable: batch item %d: replay record marks it terminal but no terminal checkpoint was found", i)
		}
		item, err := replayTerminalChildItem[O](ec, op, childID, itemName, i, options, childSubType, runItem)
		if err != nil {
			return BatchResult[O]{}, err
		}
		results = append(results, item)
	}
	return BatchResult[O]{Items: results, Reason: record.Reason}, nil
}

// batchParentUpdate builds an operation update for the parent batch context.
func batchParentUpdate(ec *execContext, id, name, subType string, action types.OperationAction) types.OperationUpdate {
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

// batchChildUpdate builds an operation update for a batch child (iteration
// or branch).
func batchChildUpdate(ec *execContext, childID, childName, childSubType, parentID string, action types.OperationAction) types.OperationUpdate {
	update := types.OperationUpdate{
		Id:       aws.String(hashID(childID)),
		Type:     types.OperationTypeContext,
		SubType:  aws.String(childSubType),
		Action:   action,
		ParentId: aws.String(hashID(parentID)),
	}
	if childName != "" {
		update.Name = aws.String(childName)
	}
	return update
}

// itemNameForIndex returns the name for a batch item at the given index.
func itemNameForIndex(options batchOptions, index int) string {
	if options.itemNamer != nil {
		return options.itemNamer(index)
	}
	return ""
}

// shouldStopMin checks if the min-successful threshold has been met.
func shouldStopMin(cfg CompletionConfig, successCount int) bool {
	if cfg.MinSuccessful <= 0 {
		return false
	}
	return successCount >= cfg.MinSuccessful
}

// shouldStopFailure checks if the failure tolerance has been exceeded.
func shouldStopFailure(cfg CompletionConfig, failureCount, totalItems int) bool {
	// Check count-based tolerance.
	if cfg.ToleratedFailureCount != nil && failureCount > *cfg.ToleratedFailureCount {
		return true
	}
	// Check percentage-based tolerance.
	if cfg.ToleratedFailurePercentage > 0 && totalItems > 0 {
		pct := (failureCount * 100) / totalItems
		if pct > cfg.ToleratedFailurePercentage {
			return true
		}
	}
	return false
}
