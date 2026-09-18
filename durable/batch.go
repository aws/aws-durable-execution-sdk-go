package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
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
// item values. MaxConcurrency bounds in-flight items.
//
// # Completion and failure
//
// The default completion policy is fail-fast: with no [WithCompletion]
// option, the first item failure completes the batch with
// [CompletionFailureToleranceExceeded] and the items not yet started are
// omitted from the result. [CompletionConfig] documents the thresholds
// that tolerate failures or complete the batch early, and the
// [CompletionReason] each one produces.
//
// When at least one item failed, Map returns a [BatchError] as err and
// still returns the populated [BatchResult], so the partial results remain
// available for compensation. The error's Reason is the batch's completion
// reason and its Errors are the per-item errors in input order. This holds
// whether or not the failures were within a configured tolerance; check
// [BatchError.Reason] to distinguish an exceeded tolerance from tolerated
// failures. Every other non-nil err (invalid options, suspension, replay
// divergence, a checkpoint failure) is returned with a zero result and
// must be propagated unchanged:
//
//	result, err := durable.Map(ctx, "reserve", items, fn, durable.WithCompletion(cfg))
//	var berr *durable.BatchError
//	switch {
//	case err == nil:
//		// use result
//	case errors.As(err, &berr):
//		// items failed; result is populated for compensation
//	default:
//		return err // suspension or SDK failure: propagate unchanged
//	}
//
// The batch's own checkpoint records the batch operation as SUCCEEDED
// regardless: the batch operation completed, and the failure of its items
// is recorded in the result it stores. Only the Go return value reports
// the failure. Replaying a checkpointed batch returns the same result and
// the same [BatchError], rebuilt from the stored items and reason.
func Map[I, O any](ctx Context, name string, items []I, fn func(ctx Context, item I, index int) (O, error), opts ...BatchOption) (BatchResult[O], error) {
	ec, ok := ctx.(*execContext)
	if !ok {
		return BatchResult[O]{}, fmt.Errorf("durable: Map %q: Context was not created by the SDK", name)
	}

	options := resolveBatchOptions(ec, opts)

	if options.maxConcurrencySet && options.maxConcurrency <= 0 {
		return BatchResult[O]{}, fmt.Errorf("durable: Map %q: max concurrency must be positive, got %d", name, options.maxConcurrency)
	}
	if err := options.completion.validate(); err != nil {
		return BatchResult[O]{}, fmt.Errorf("durable: Map %q: %w", name, err)
	}

	id, err := ec.claimOperation()
	if err != nil {
		return BatchResult[O]{}, err
	}
	options.itemID = batchItemIDs(ec, id, options.nesting)

	// Check if the batch is already checkpointed as a terminal operation.
	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(OperationTypeContext), operationSubTypeMap, name); err != nil {
		return BatchResult[O]{}, err
	}
	if ec.unfinishedInSucceededContext(op) {
		return BatchResult[O]{}, ec.parkUnfinishedReplay(op, id, string(OperationTypeContext), operationSubTypeMap, name)
	}
	if op != nil && op.status.terminal() {
		result, err := replayTerminalBatch[I, O](ec, op, id, name, items, fn, options, operationSubTypeMap, operationSubTypeMapIteration)
		if err != nil {
			return BatchResult[O]{}, err
		}
		ec.ids.advance(batchReplayAdvance(options, len(items), len(result.Items)))
		return batchOutcome(name, result)
	}

	// Checkpoint the parent Map context START.
	if op == nil {
		update := batchParentUpdate(ec, id, name, operationSubTypeMap, OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
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
	result, err := executeBatchItems[I, O](ec, id, name, totalItems, options, operationSubTypeMap, operationSubTypeMapIteration, func(childCtx Context, index int) (O, []string, error) {
		return runBatchItemFunc(childCtx, index, fn, func() (O, error) {
			return fn(childCtx, items[index], index)
		})
	})
	if err != nil {
		return BatchResult[O]{}, err
	}
	return batchOutcome(name, result)
}

// Parallel executes branches concurrently, each in its own child context,
// and returns the collected results. All branches must produce the same
// type; for heterogeneous fan-out, use [Go] with futures of different
// types.
//
// Each branch runs in a ParallelBranch child context named by [Branch].Name.
// MaxConcurrency bounds in-flight branches.
//
// Completion and failure follow the same rules as [Map]: the default policy
// is fail-fast, [CompletionConfig] documents the thresholds and the
// [CompletionReason] each produces, and when at least one branch failed
// Parallel returns a [BatchError] as err together with the populated
// [BatchResult]. The batch's checkpoint records the operation as SUCCEEDED
// regardless, and replay returns the same error.
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
	if err := options.completion.validate(); err != nil {
		return BatchResult[O]{}, fmt.Errorf("durable: Parallel %q: %w", name, err)
	}

	id, err := ec.claimOperation()
	if err != nil {
		return BatchResult[O]{}, err
	}
	options.itemID = batchItemIDs(ec, id, options.nesting)

	// Check if the batch is already checkpointed as a terminal operation.
	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(OperationTypeContext), operationSubTypeParallel, name); err != nil {
		return BatchResult[O]{}, err
	}
	if ec.unfinishedInSucceededContext(op) {
		return BatchResult[O]{}, ec.parkUnfinishedReplay(op, id, string(OperationTypeContext), operationSubTypeParallel, name)
	}
	if op != nil && op.status.terminal() {
		placeholders := make([]struct{}, len(branches))
		result, err := replayTerminalBatch[struct{}, O](ec, op, id, name, placeholders, func(ctx Context, _ struct{}, index int) (O, error) {
			return branches[index].Func(ctx)
		}, options, operationSubTypeParallel, operationSubTypeParallelBranch)
		if err != nil {
			return BatchResult[O]{}, err
		}
		ec.ids.advance(batchReplayAdvance(options, len(branches), len(result.Items)))
		return batchOutcome(name, result)
	}

	// Checkpoint the parent Parallel context START.
	if op == nil {
		update := batchParentUpdate(ec, id, name, operationSubTypeParallel, OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
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

	result, err := executeBatchItems[struct{}, O](ec, id, name, totalItems, options, operationSubTypeParallel, operationSubTypeParallelBranch, func(childCtx Context, index int) (O, []string, error) {
		return runBatchItemFunc(childCtx, index, branches[index].Func, func() (O, error) {
			return branches[index].Func(childCtx)
		})
	})
	if err != nil {
		return BatchResult[O]{}, err
	}
	return batchOutcome(name, result)
}

// Branch is one branch of a [Parallel] operation.
type Branch[O any] struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

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
	// BatchItemNotStarted is the zero value. It never appears in a
	// [BatchResult]: items that never started are omitted from Items. It
	// appears only in a [BatchProgress] snapshot, for the items the batch
	// has not yet admitted when a custom completion callback runs.
	BatchItemNotStarted BatchItemStatus = 0

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
	//
	// In a [BatchProgress] snapshot it marks an item that is in flight.
	BatchItemStarted BatchItemStatus = 4
)

// String returns the wire representation of the status.
func (s BatchItemStatus) String() string {
	switch s {
	case BatchItemNotStarted:
		return "NOT_STARTED"
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
	_ [0]func() // blocks unkeyed literals; keeps fields addable

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
	_ [0]func() // blocks unkeyed literals; keeps fields addable

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

// Started returns the items that were started and then abandoned when the
// batch completed early, in input order. These are the items with status
// [BatchItemStarted]. They are included in [BatchResult.TotalCount] but
// are neither successes nor failures. Items that never started are not
// in Items and so are not returned.
func (r BatchResult[O]) Started() []BatchItem[O] {
	var out []BatchItem[O]
	for i := range r.Items {
		if r.Items[i].Status == BatchItemStarted {
			out = append(out, r.Items[i])
		}
	}
	return out
}

// StartedCount returns the number of items that were started and then
// abandoned when the batch completed early. [BatchResult.TotalCount] is
// SuccessCount + FailureCount + StartedCount.
func (r BatchResult[O]) StartedCount() int {
	n := 0
	for i := range r.Items {
		if r.Items[i].Status == BatchItemStarted {
			n++
		}
	}
	return n
}

// Item returns the item or branch with the given name, or nil if no item
// has that name. When several items share the name, it returns the first
// in input order. The pointer refers into Items.
func (r BatchResult[O]) Item(name string) *BatchItem[O] {
	for i := range r.Items {
		if r.Items[i].Name == name {
			return &r.Items[i]
		}
	}
	return nil
}

// Result returns the successful result of the item or branch with the
// given name. ok is false if no item has that name or the item did not
// succeed. When several items share the name, the first in input order is
// used, whatever its status.
func (r BatchResult[O]) Result(name string) (value O, ok bool) {
	item := r.Item(name)
	if item == nil || item.Status != BatchItemSucceeded {
		var zero O
		return zero, false
	}
	return item.Result, true
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

// batchOutcome is the return value of [Map] and [Parallel] for a
// completed batch: the populated result and, when [BatchResult.Status] is
// [BatchItemFailed], a [BatchError] describing the failure. It is derived
// from the result's Items and Reason alone, so the first invocation and
// every replay return the same error for the same checkpointed batch.
func batchOutcome[O any](name string, result BatchResult[O]) (BatchResult[O], error) {
	if result.Status() != BatchItemFailed {
		return result, nil
	}
	return result, &BatchError{Name: name, Reason: result.Reason, Errors: result.Errors()}
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

// Status returns the overall batch status. A custom completion decision
// is authoritative: [CompletionCustomFailed] yields [BatchItemFailed] and
// [CompletionCustomSucceeded] yields [BatchItemSucceeded], whatever the
// item outcomes. Otherwise Status is [BatchItemFailed] if any item failed
// or the batch-level completion indicates failure, and
// [BatchItemSucceeded] if not.
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
	// A custom completion decision decides the outcome even when it
	// disagrees with the items: a custom failure may have no failed item,
	// and a custom success may carry failed items.
	switch reason {
	case CompletionCustomFailed, CompletionFailureToleranceExceeded:
		return BatchItemFailed
	case CompletionCustomSucceeded:
		return BatchItemSucceeded
	}
	for i := range items {
		if items[i].Status == BatchItemFailed {
			return BatchItemFailed
		}
	}
	return BatchItemSucceeded
}

// CompletionReason records why a batch operation completed.
type CompletionReason int

// Batch completion reasons. Values are persisted in checkpoints, so they
// are pinned explicitly rather than derived from iota ordering.
const (
	// CompletionAllCompleted indicates every item ran to completion.
	CompletionAllCompleted CompletionReason = 1

	// CompletionMinSuccessfulReached indicates the batch completed early
	// because the MinSuccessful threshold was met.
	CompletionMinSuccessfulReached CompletionReason = 2

	// CompletionFailureToleranceExceeded indicates the batch failed early
	// because more items failed than the tolerance allows.
	CompletionFailureToleranceExceeded CompletionReason = 3

	// CompletionCustomSucceeded indicates a custom completion decision
	// completed the batch early as succeeded.
	CompletionCustomSucceeded CompletionReason = 4

	// CompletionCustomFailed indicates a custom completion decision
	// completed the batch early as failed.
	CompletionCustomFailed CompletionReason = 5
)

// completionReasonUnknown is the string form of a [CompletionReason] that
// is zero or not one of the defined reasons.
const completionReasonUnknown = "UNKNOWN"

// String returns the wire representation of the completion reason. A zero
// or unrecognized value returns "UNKNOWN".
func (r CompletionReason) String() string {
	switch r {
	case CompletionAllCompleted:
		return "ALL_COMPLETED"
	case CompletionMinSuccessfulReached:
		return "MIN_SUCCESSFUL_REACHED"
	case CompletionFailureToleranceExceeded:
		return "FAILURE_TOLERANCE_EXCEEDED"
	case CompletionCustomSucceeded:
		return "CUSTOM_COMPLETION_SUCCEEDED"
	case CompletionCustomFailed:
		return "CUSTOM_COMPLETION_FAILED"
	default:
		return completionReasonUnknown
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
//
// Changing n between deployments does not move the operation IDs of a
// batch's items or of the operations inside them, so an execution that is
// in flight inside the batch keeps replaying correctly. Items are
// identified by input index whether they run one at a time or
// concurrently: a [NestingFlat] item is numbered under the batch, and a
// [NestingNormal] item takes the next operation ID after the batch in the
// enclosing context.
//
// A [NestingFlat] batch also consumes the same number of enclosing-context
// IDs for every n, so changing n never affects the operations that follow
// it. A [NestingNormal] batch consumes one enclosing-context ID per item
// that starts: with n == 1 only the items that actually start, with n > 1
// every item. Those counts differ only when the completion policy (the
// fail-fast default or a [WithCompletion] policy) stops the batch early.
// For a [NestingNormal] batch that can stop early, changing n shifts the
// IDs of the operations after the batch for executions that are in
// flight; treat that combination as a breaking change for in-flight
// executions.
func WithMaxConcurrency(n int) BatchOption {
	return batchOptionFunc(func(o *batchOptions) {
		o.maxConcurrency = n
		o.maxConcurrencySet = true
	})
}

// WithCompletion sets the batch's completion policy: thresholds or a
// custom [CompletionConfig].ShouldComplete callback. Without it the batch
// is fail-fast; see [CompletionConfig].
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

// CompletionConfig is a batch completion policy for [Map] and [Parallel].
//
// The default is fail-fast. When no threshold is set (no [WithCompletion]
// option, or a zero CompletionConfig), the first item failure completes
// the batch with [CompletionFailureToleranceExceeded]; items not yet
// started are omitted from the result. Tolerating failures requires
// setting ToleratedFailureCount or ToleratedFailurePercentage explicitly.
// Setting only MinSuccessful also disables fail-fast: every item runs
// unless the MinSuccessful threshold completes the batch first.
//
// Thresholds may be combined. The first threshold to fire decides the
// [CompletionReason]:
//
//   - No threshold fires: [CompletionAllCompleted].
//   - MinSuccessful items have succeeded: [CompletionMinSuccessfulReached].
//     Items not yet started are omitted.
//   - More items failed than the tolerance allows (or, with no tolerance
//     set, one item failed): [CompletionFailureToleranceExceeded]. Items
//     not yet started are omitted.
//
// The completion reason describes why scheduling stopped. Whether the
// batch is returned as an error is decided separately: Map and Parallel
// return a [BatchError] whenever at least one item failed, whatever the
// reason. See [Map].
//
// # Custom completion
//
// ShouldComplete replaces the thresholds with a callback that decides
// completion from the batch's progress. It is mutually exclusive with the
// threshold fields: a CompletionConfig that sets ShouldComplete together
// with MinSuccessful, ToleratedFailureCount, or ToleratedFailurePercentage
// is rejected, and Map or Parallel returns an error before any item runs.
// With ShouldComplete set there is no fail-fast: an item failure by itself
// never stops the batch. The callback decides, and the batch completes with
// [CompletionAllCompleted] once every item has finished if the callback
// never completed it.
type CompletionConfig struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// MinSuccessful completes the batch early once this many items
	// succeed. Zero leaves the threshold unset.
	MinSuccessful int

	// ToleratedFailureCount fails the batch once more than this many
	// items fail. Nil leaves the threshold unset; an explicit zero fails
	// the batch on the first failure.
	ToleratedFailureCount *int

	// ToleratedFailurePercentage fails the batch once the failure
	// percentage, computed against the total item count, strictly
	// exceeds this value. Nil leaves the threshold unset; an explicit
	// zero fails the batch on the first failure. The comparison is
	// exact: with three items and a threshold of 33, one failure
	// (33.3%) exceeds the threshold.
	ToleratedFailurePercentage *int

	// ShouldComplete decides completion programmatically. It is called
	// after each item reaches a terminal state, with a [BatchProgress]
	// snapshot taken at that moment, and returns [ContinueBatch] to keep
	// going or [CompleteBatch] to complete the batch now. Completing early
	// leaves the items still in flight with status [BatchItemStarted] and
	// omits the items that never started, exactly as a threshold does.
	// The batch's [CompletionReason] is then [CompletionCustomSucceeded]
	// or [CompletionCustomFailed], by the decision's [CompletionOutcome];
	// that outcome alone decides [BatchResult.Status] and whether Map or
	// Parallel returns a [BatchError].
	//
	// The callback must be deterministic: it must depend only on its
	// argument, and it must return the same decision for the same
	// progress every time it is called. The decision it makes is
	// checkpointed with the batch, so replaying a completed batch reads
	// the recorded reason and does not call the callback again. A
	// callback that reads state outside its argument can decide
	// differently on one invocation than the checkpoint of an earlier
	// invocation recorded, and the execution then diverges from its
	// checkpoint log.
	//
	// Items can finish between two evaluations, so the batch may complete
	// with more terminal items than the snapshot that decided it showed.
	// A callback that panics, or that completes the batch with an outcome
	// that is not a defined [CompletionOutcome], fails the batch operation
	// with an error that is not a BatchError.
	ShouldComplete func(BatchProgress) CompletionDecision
}

// validate reports the configuration errors of a CompletionConfig. A
// custom callback and a threshold are two policies for one decision, so
// setting both is rejected rather than resolved in favour of one.
func (c CompletionConfig) validate() error {
	if c.ShouldComplete != nil && c.hasThreshold() {
		return errors.New("CompletionConfig.ShouldComplete is mutually exclusive with MinSuccessful, ToleratedFailureCount, and ToleratedFailurePercentage")
	}
	return nil
}

// BatchProgress is the snapshot of a batch's progress passed to
// [CompletionConfig].ShouldComplete. It is taken directly after one item
// reached a terminal state, before any further item is admitted.
type BatchProgress struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// TotalCount is the number of items in the batch, started or not.
	TotalCount int

	// CompletedCount is the number of items that have reached a terminal
	// state: SuccessCount + FailureCount.
	CompletedCount int

	// SuccessCount is the number of items that have succeeded so far.
	SuccessCount int

	// FailureCount is the number of items that have failed so far.
	FailureCount int

	// Items holds one entry per item of the batch, in input order, so
	// Items[i] is the item at input index i whatever the order in which
	// items finish. Its length is TotalCount.
	Items []BatchItemProgress
}

// BatchItemProgress is the state of one item in a [BatchProgress]
// snapshot.
type BatchItemProgress struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// Index is the zero-based position of the item in the input slice.
	Index int

	// Name identifies the item or branch. It is empty when the item has
	// no name.
	Name string

	// Status is [BatchItemNotStarted] for an item the batch has not yet
	// admitted, [BatchItemStarted] for an item in flight, and
	// [BatchItemSucceeded] or [BatchItemFailed] once it is terminal.
	Status BatchItemStatus
}

// CompletionOutcome is the outcome a custom completion decision assigns to
// the batch it completes.
type CompletionOutcome int

// Completion outcomes.
const (
	// CompletionOutcomeSucceeded completes the batch as succeeded, with
	// [CompletionCustomSucceeded], even if items failed.
	CompletionOutcomeSucceeded CompletionOutcome = 1

	// CompletionOutcomeFailed completes the batch as failed, with
	// [CompletionCustomFailed], even if no item failed. Map and Parallel
	// then return a [BatchError] whose Errors holds the failed items'
	// errors, which may be empty.
	CompletionOutcomeFailed CompletionOutcome = 2
)

// String returns the name of the outcome.
func (o CompletionOutcome) String() string {
	switch o {
	case CompletionOutcomeSucceeded:
		return "SUCCEEDED"
	case CompletionOutcomeFailed:
		return "FAILED"
	default:
		return completionReasonUnknown
	}
}

// CompletionDecision is the value a [CompletionConfig].ShouldComplete
// callback returns. Build it with [ContinueBatch] or [CompleteBatch]. The
// zero value is the decision [ContinueBatch] returns: it keeps the batch
// running.
type CompletionDecision struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	complete bool
	outcome  CompletionOutcome
}

// ContinueBatch returns the decision to keep the batch running. It is the
// zero CompletionDecision.
func ContinueBatch() CompletionDecision {
	return CompletionDecision{}
}

// CompleteBatch returns the decision to complete the batch now with the
// given outcome. The batch stops admitting items and stops awaiting the
// items in flight. outcome must be [CompletionOutcomeSucceeded] or
// [CompletionOutcomeFailed]; any other value fails the batch operation when
// the decision is applied.
func CompleteBatch(outcome CompletionOutcome) CompletionDecision {
	return CompletionDecision{complete: true, outcome: outcome}
}

// Complete reports whether the decision completes the batch.
func (d CompletionDecision) Complete() bool { return d.complete }

// Outcome returns the outcome of a completing decision. It is zero for a
// decision built by [ContinueBatch].
func (d CompletionDecision) Outcome() CompletionOutcome { return d.outcome }

// reason maps a completing decision to the batch's completion reason. ok
// is false when the outcome is not a defined [CompletionOutcome].
func (d CompletionDecision) reason() (reason CompletionReason, ok bool) {
	switch d.outcome {
	case CompletionOutcomeSucceeded:
		return CompletionCustomSucceeded, true
	case CompletionOutcomeFailed:
		return CompletionCustomFailed, true
	default:
		return 0, false
	}
}

type batchOptions struct {
	maxConcurrency    int
	maxConcurrencySet bool // true when user explicitly set via WithMaxConcurrency
	completion        CompletionConfig
	itemNamer         func(index int) string
	itemSerdes        Serdes
	resultSerdes      Serdes
	nesting           NestingMode

	// itemID returns the operation ID of the item at an input index. Map
	// and Parallel set it once the batch's own ID is claimed; see
	// batchItemIDs. It keys every serdes call for an item's result, so
	// each item's [SerdesContext.OperationID] is distinct.
	itemID func(index int) string
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

// batchItemFunc runs one batch item in its child context and returns the
// item result or error. On failure the middle result is the stack trace
// captured where the item's user function failed, as [runUserFunc]
// captures it; it is nil on success or when capture is disabled.
type batchItemFunc[O any] func(childCtx Context, index int) (O, []string, error)

// runBatchItemFunc runs one call into a batch item's user function
// through [runUserFunc], so a panic becomes a failure of that item and a
// failure's trace names userFn. childCtx is the item's child context.
func runBatchItemFunc[O any](childCtx Context, index int, userFn any, call func() (O, error)) (O, []string, error) {
	ec, _ := childCtx.(*execContext)
	return runUserFunc(ec, userFn, fmt.Sprintf("durable: batch item %d panicked", index), call)
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
	runItem batchItemFunc[O],
) (BatchResult[O], error) {
	// Items collects results in input order. We allocate for all items
	// but only fill the ones that actually start.
	results := make([]BatchItem[O], 0, totalItems)

	var (
		reason       = CompletionAllCompleted
		reasonLocked bool // set at the moment the completion decision is made
	)
	decider := newBatchDecider(parentName, options, totalItems)

	// Sequential execution path (max-concurrency = 1 or all items sequential).
	// Also used when max-concurrency >= totalItems (effectively unlimited).
	concurrency := options.maxConcurrency
	if concurrency < 0 {
		concurrency = totalItems
	}

	if concurrency == 1 {
		// Sequential path: simpler, no goroutines needed.
		for i := 0; i < totalItems; i++ {
			// Check if we should stop scheduling.
			if reasonLocked {
				break
			}

			itemName := itemNameForIndex(options, i)
			var result BatchItem[O]
			var err error
			if options.nesting == NestingFlat {
				childID, virtualChild := flatItemContext(ec, parentID, i, ec.owner)
				result, err = runFlatBatchItem[O](virtualChild, childID, i, itemName, options, runItem)
			} else {
				result, err = runNestedBatchItem[O](ec, parentID, parentName, itemName, i, options, childSubType, runItem)
			}
			if err != nil {
				// Suspension and every other error propagate unchanged.
				return BatchResult[O]{}, err
			}

			results = append(results, result)

			// Evaluate the completion policy AFTER recording the result.
			decided, stop, derr := decider.terminal(i, result.Status)
			if derr != nil {
				return BatchResult[O]{}, derr
			}
			if stop {
				reason = decided
				reasonLocked = true
			}
		}
	} else {
		// Concurrent path: a coordinator loop on the calling goroutine
		// owns all completion state, so no lock is needed. Worker
		// goroutines only run an item body and report its outcome on a
		// channel. Child operation ids are assigned up front in index
		// order so ids mint deterministically across invocations: NORMAL
		// items are claimed from the enclosing context, FLAT items are
		// numbered under the batch (see flatItemID). A branch is
		// checkpointed STARTED and dispatched only when the coordinator
		// admits it. Once the completion decision fires the coordinator
		// stops admitting and abandons the branches still in flight: it
		// stops awaiting them, marks them started, and does not count
		// them. An abandoned NORMAL branch writes no terminal checkpoint,
		// so its child context stays STARTED in the log, matching the
		// batch result. It still drains every dispatched worker before
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
			var childID string
			if options.nesting == NestingFlat {
				childID = flatItemID(parentID, i)
			} else {
				var claimErr error
				childID, claimErr = ec.claimOperation()
				if claimErr != nil {
					return BatchResult[O]{}, claimErr
				}
			}
			op := ec.state.get(childID)
			preClaimed = append(preClaimed, preClaimedItem{
				index: i, name: itemName, childID: childID, op: op,
				terminal: op != nil && op.status.terminal(),
			})
		}

		// The handle is minted beneath the enclosing subtree's handle (nil
		// at the top level), so any enclosing batch's abandonment reaches
		// every context under this one through the chain.
		abandon := newAbandonHandle(ec.abandon)
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
					update := batchChildUpdate(ec, pc.childID, pc.name, childSubType, parentID, OperationActionStart)
					if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
						admitErr = err
						return
					}
				}

				startedIdx[pc.index] = struct{}{}
				decider.started(pc.index)
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
				decided, stop, derr := decider.terminal(out.index, out.item.Status)
				if derr != nil {
					// The policy itself failed (a ShouldComplete callback
					// panicked or returned an invalid decision). Treat it
					// like any other fatal error: stop admitting and drain.
					if fatalErr == nil {
						fatalErr = derr
					}
				} else if stop {
					reason = decided
					reasonLocked = true
				}
				if reasonLocked {
					// Stop awaiting the branches still in flight: they
					// unwind at their next operation without starting new
					// work.
					abandon.abandon()
				}
			}
			if fatalErr == nil {
				admit()
			}
		}
		// Every dispatched worker has reported; ensure none is still
		// unwinding before the parent context is checkpointed terminal.
		// This joins the item workers only. A branch launched by
		// durable.Go inside an item is not joined here; see
		// retireCommitment for how its commitment is handled.
		wg.Wait()

		// A branch abandoned after early completion may have already
		// committed the invocation to PENDING (a wait, invoke, callback
		// or retry that suspended before the completion decision fired).
		// Now that every worker has drained, retire those commitments so
		// abandoned work does not force the whole invocation to PENDING.
		// A commitment made after this point by any context under this
		// handle, including a durable.Go branch that outlives the workers
		// and any batch it starts, is dropped by commitPending because
		// its handle reaches this one through the chain.
		if reasonLocked {
			ec.suspend.retireCommitment(abandon)
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
	runItem batchItemFunc[O],
	abandon *abandonHandle,
	tok *branchToken,
) (BatchItem[O], error) {
	if err := validateReplayConsistency(op, string(OperationTypeContext), childSubType, itemName); err != nil {
		return BatchItem[O]{}, err
	}
	if options.nesting == NestingFlat {
		// FLAT mode: run in a virtual child context. The item's ID was
		// numbered under the batch by flatItemID; flatItemContext derives
		// the same ID again from the index.
		_, virtualChild := flatItemContext(ec, parentID, index, currentGoroutineOwner())
		virtualChild.abandon = abandon
		virtualChild.adoptBranchToken(tok)
		return runFlatBatchItem[O](virtualChild, childID, index, itemName, options, runItem)
	}

	// NORMAL mode: child context with full checkpointing.
	if terminal {
		return replayTerminalChildItem[O](ec, op, childID, itemName, index, options, childSubType, runItem)
	}

	// The child context runs on this goroutine; capture ownership here.
	mode := childReplayMode(ec, childID, op)
	child := ec.child(childID, currentGoroutineOwner(), mode)
	child.abandon = abandon
	child.adoptBranchToken(tok)

	result, fnTrace, fnErr := runItem(child, index)

	// The batch may have completed while the item body ran. The batch then
	// no longer awaits this item and reports it STARTED. So the item's
	// checkpoint must stay STARTED too: a SUCCEEDED or FAILED child record
	// would disagree with the batch result the parent checkpoint stores.
	// Unwind the worker the way an abandoned operation does, without a
	// checkpoint. The coordinator treats this outcome as abandoned.
	if abandon.abandoned() {
		return BatchItem[O]{}, errSuspendExecution
	}

	// From here to the checkpoint of the item's completion the item is an
	// executing span: its outcome belongs to this invocation, so a handler
	// blocked on a pending operation waits for it to be recorded before it
	// suspends. See awaitDrain.
	ec.suspend.enterExecuting()
	defer ec.suspend.exitExecuting()

	if fnErr != nil {
		if errors.Is(fnErr, errSuspendExecution) {
			return BatchItem[O]{}, fnErr
		}
		update := batchChildUpdate(ec, childID, itemName, childSubType, parentID, OperationActionFail)
		update.Error = errorObjectFromRecord(recordOf(fnErr).withTrace(fnTrace))
		update.Error.ErrorData = encodeChildErrorData(fnErr)
		if cerr := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); cerr != nil {
			return BatchItem[O]{}, cerr
		}
		return BatchItem[O]{
			Index:  index,
			Name:   itemName,
			Status: BatchItemFailed,
			Err:    liveBatchItemError(itemName, fnErr, update.Error.StackTrace),
		}, nil
	}

	serialized, serErr := options.itemSerdes.Marshal(ec.Context, ec.serdesCtx(childID), result)
	if serErr != nil {
		return BatchItem[O]{}, newSerdesError(batchItemOpName(itemName, index), serdesDirectionMarshal, serErr)
	}
	update := batchChildUpdate(ec, childID, itemName, childSubType, parentID, OperationActionSucceed)
	if len(serialized) > checkpointSizeLimitBytes {
		update.ContextOptions = &ContextOptions{ReplayChildren: aws.Bool(true)}
	} else {
		update.Payload = aws.String(string(serialized))
	}
	if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
		return BatchItem[O]{}, err
	}
	var out O
	if err := options.itemSerdes.Unmarshal(ec.Context, ec.serdesCtx(childID), serialized, &out); err != nil {
		return BatchItem[O]{}, newSerdesError(batchItemOpName(itemName, index), serdesDirectionUnmarshal, err)
	}
	return BatchItem[O]{
		Index:  index,
		Name:   itemName,
		Status: BatchItemSucceeded,
		Result: out,
	}, nil
}

// flatItemID returns the operation ID of FLAT item index of the batch whose
// operation ID is parentID.
//
// This is the one ID scheme for FLAT items. A FLAT item runs in a virtual
// child context: it is never checkpointed itself, but it owns an
// operation-ID namespace so that the operations inside it have stable IDs.
// Items are numbered as children of the batch, in index order: item 0 is
// "<parentID>-1", item 1 is "<parentID>-2", and the k-th operation inside
// item i is "<parentID>-<i+1>-<k>". Every path that runs FLAT items
// (sequential, concurrent, and replay of a batch too large to store) derives
// item IDs from this function, so:
//
//   - a FLAT batch consumes exactly one operation ID from the enclosing
//     context, its own, on live execution and on replay alike;
//   - changing the batch's concurrency does not move any item's operations
//     to a different ID, so an in-flight execution keeps replaying
//     correctly across a deployment that changes [WithMaxConcurrency];
//   - each item has a distinct ID to key per-item serdes files on.
//
// The operations inside an item record the batch, not the virtual item, as
// their ParentId, because the batch is the nearest checkpointed ancestor;
// see [execContext.virtualChild].
func flatItemID(parentID string, index int) string {
	return (&opIDs{prefix: parentID}).format(index + 1)
}

// batchItemIDs returns the function that maps an input index to the
// operation ID of that item of the batch id. It must be called on the
// enclosing context ec directly after the batch's own ID is claimed, before
// any item is claimed, because NORMAL items take the IDs that follow the
// batch in the enclosing context, in index order: the sequential path
// claims one as each item starts and the concurrent path claims them all up
// front, so item i is always the (i+1)-th ID after the batch. FLAT items are
// numbered under the batch by flatItemID.
func batchItemIDs(ec *execContext, id string, nesting NestingMode) func(index int) string {
	if nesting == NestingFlat {
		return func(index int) string { return flatItemID(id, index) }
	}
	sib := &opIDs{prefix: ec.ids.prefix}
	base := ec.ids.counter
	return func(index int) string { return sib.format(base + 1 + index) }
}

// batchItemSerdesCtx returns the function that builds the [SerdesContext]
// for an item's result from the item's input index, keyed on the item's
// own operation ID.
func batchItemSerdesCtx(ec *execContext, options batchOptions) func(index int) SerdesContext {
	return func(index int) SerdesContext { return ec.serdesCtx(options.itemID(index)) }
}

// flatItemContext returns the ID of FLAT item index of the batch parentID
// and the virtual child context that runs it. The item replays when its
// first operation is already checkpointed. When ec itself replays inside a
// context whose overall result is already recorded, the item inherits that
// mode so its unfinished operations park instead of re-executing.
func flatItemContext(ec *execContext, parentID string, index int, owner goroutineOwner) (string, *execContext) {
	childID := flatItemID(parentID, index)
	mode := childReplayMode(ec, childID, nil)
	if executionMode(ec.mode.Load()) == modeReplaySucceededContext {
		mode = modeReplaySucceededContext
	}
	return childID, ec.virtualChild(childID, parentID, owner, mode)
}

// batchReplayAdvance returns how many operation IDs a terminal batch
// consumed from the enclosing context, beyond its own ID, on the invocation
// that ran it. Replay of the terminal batch advances the enclosing counter
// by this amount so the operations after the batch keep their IDs.
//
// FLAT items are numbered under the batch (see flatItemID), so a FLAT batch
// consumes none. NORMAL items are claimed from the enclosing context: the
// sequential path claims one per started item, and the concurrent path
// claims one per item up front.
func batchReplayAdvance(options batchOptions, totalItems, startedItems int) int {
	if options.nesting == NestingFlat {
		return 0
	}
	concurrency := options.maxConcurrency
	if concurrency < 0 {
		concurrency = totalItems
	}
	if concurrency == 1 {
		return startedItems
	}
	return totalItems
}

// runFlatBatchItem runs one item in FLAT nesting mode inside virtualChild,
// the item's virtual context from flatItemContext, and round-trips the
// result through the item serdes keyed on childID, the item's own
// operation ID. No per-item context events are checkpointed.
func runFlatBatchItem[O any](
	virtualChild *execContext,
	childID string,
	index int,
	itemName string,
	options batchOptions,
	runItem batchItemFunc[O],
) (BatchItem[O], error) {
	result, fnTrace, fnErr := runItem(virtualChild, index)
	if fnErr != nil {
		if errors.Is(fnErr, errSuspendExecution) {
			return BatchItem[O]{}, fnErr
		}
		return BatchItem[O]{
			Index:  index,
			Name:   itemName,
			Status: BatchItemFailed,
			Err:    flatItemError(fnErr, fnTrace),
		}, nil
	}

	// Round-trip through serdes for live == replay consistency.
	sctx := virtualChild.serdesCtx(childID)
	serialized, serErr := options.itemSerdes.Marshal(virtualChild.Context, sctx, result)
	if serErr != nil {
		return BatchItem[O]{}, newSerdesError(batchItemOpName(itemName, index), serdesDirectionMarshal, serErr)
	}
	var out O
	if err := options.itemSerdes.Unmarshal(virtualChild.Context, sctx, serialized, &out); err != nil {
		return BatchItem[O]{}, newSerdesError(batchItemOpName(itemName, index), serdesDirectionUnmarshal, err)
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
	runItem batchItemFunc[O],
) (item BatchItem[O], retErr error) {
	// Claim the child's operation ID from the parent.
	childID, err := ec.claimOperation()
	if err != nil {
		return BatchItem[O]{}, err
	}

	op := ec.state.get(childID)
	if err := validateReplayConsistency(op, string(OperationTypeContext), childSubType, itemName); err != nil {
		return BatchItem[O]{}, err
	}
	if op != nil && op.status.terminal() {
		return replayTerminalChildItem[O](ec, op, childID, itemName, index, options, childSubType, runItem)
	}

	// Checkpoint child context START.
	if op == nil {
		update := batchChildUpdate(ec, childID, itemName, childSubType, parentID, OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
			return BatchItem[O]{}, err
		}
	}

	mode := childReplayMode(ec, childID, op)
	child := ec.child(childID, ec.owner, mode)

	result, fnTrace, fnErr := runItem(child, index)

	// From here to the checkpoint of the item's completion the item is an
	// executing span; see runPreClaimedBatchItem and awaitDrain.
	ec.suspend.enterExecuting()
	defer ec.suspend.exitExecuting()

	if fnErr != nil {
		if errors.Is(fnErr, errSuspendExecution) {
			return BatchItem[O]{}, fnErr
		}
		// Checkpoint the failure.
		update := batchChildUpdate(ec, childID, itemName, childSubType, parentID, OperationActionFail)
		update.Error = errorObjectFromRecord(recordOf(fnErr).withTrace(fnTrace))
		update.Error.ErrorData = encodeChildErrorData(fnErr)
		if cerr := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); cerr != nil {
			return BatchItem[O]{}, cerr
		}
		return BatchItem[O]{
			Index:  index,
			Name:   itemName,
			Status: BatchItemFailed,
			Err:    liveBatchItemError(itemName, fnErr, update.Error.StackTrace),
		}, nil
	}

	// Serialize and checkpoint the child success.
	serialized, serErr := options.itemSerdes.Marshal(ec.Context, ec.serdesCtx(childID), result)
	if serErr != nil {
		return BatchItem[O]{}, newSerdesError(batchItemOpName(itemName, index), serdesDirectionMarshal, serErr)
	}

	update := batchChildUpdate(ec, childID, itemName, childSubType, parentID, OperationActionSucceed)
	if len(serialized) > checkpointSizeLimitBytes {
		update.ContextOptions = &ContextOptions{ReplayChildren: aws.Bool(true)}
	} else {
		update.Payload = aws.String(string(serialized))
	}
	if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
		return BatchItem[O]{}, err
	}

	// Round-trip through serdes for live == replay consistency.
	var out O
	if err := options.itemSerdes.Unmarshal(ec.Context, ec.serdesCtx(childID), serialized, &out); err != nil {
		return BatchItem[O]{}, newSerdesError(batchItemOpName(itemName, index), serdesDirectionUnmarshal, err)
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
	runItem batchItemFunc[O],
) (BatchItem[O], error) {
	switch op.status {
	case statusSucceeded:
		if op.childCtx == nil {
			return BatchItem[O]{}, fmt.Errorf("durable: batch item %d: checkpointed SUCCEEDED with no context details", index)
		}
		if op.childCtx.replayChildren {
			mode := modeReplaySucceededContext
			child := ec.child(childID, ec.owner, mode)
			result, _, err := runItem(child, index)
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
			return BatchItem[O]{}, newSerdesError(batchItemOpName(itemName, index), serdesDirectionUnmarshal, err)
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
		var trace []string
		if op.childCtx != nil {
			errType = op.childCtx.errType
			errMessage = op.childCtx.errMessage
			errData = op.childCtx.errData
			trace = op.childCtx.stackTrace
		}
		cerr := batchItemError(itemName, errType, errMessage, childErrorData{}, errData)
		cerr.StackTrace = trace
		return BatchItem[O]{
			Index:  index,
			Name:   itemName,
			Status: BatchItemFailed,
			Err:    cerr,
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
		// so reconstruct from the children. With a decision record, replay
		// exactly the recorded admitted set so the result matches the live
		// shape (including abandoned items) and reuses the recorded
		// reason. Without a record (a checkpoint written before the record
		// existed) fall back to sequential re-execution, which re-derives
		// the reason.
		if op.childCtx.replayChildren {
			mode := modeReplaySucceededContext
			if record, ok := parseBatchReplayRecord(op.childCtx.result); ok {
				if record.StartedTotal > len(items) {
					return BatchResult[O]{}, fmt.Errorf("durable: batch %q: replay record admits %d items but the batch has %d", name, record.StartedTotal, len(items))
				}
				if options.nesting == NestingFlat {
					child := ec.child(id, ec.owner, mode)
					return replayFlatBatchChildrenFromRecord[I, O](child, id, record, items, fn, options)
				}
				return replayBatchChildrenFromRecord[I, O](ec, record, items, fn, options, childSubType)
			}
			child := ec.child(id, ec.owner, mode)
			return replayBatchChildren[I, O](child, id, name, items, fn, options, parentSubType, childSubType)
		}
		// If an operation-level serdes is configured, use it to
		// deserialize the whole batch result.
		if options.resultSerdes != nil {
			var result BatchResult[O]
			if err := options.resultSerdes.Unmarshal(ec.Context, ec.serdesCtx(id), []byte(op.childCtx.result), &result); err != nil {
				return BatchResult[O]{}, newSerdesError(name, serdesDirectionUnmarshal, err)
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
		return toBatchResult[O](ec.Context, payload, options.itemSerdes, batchItemSerdesCtx(ec, options))

	case statusFailed:
		errType := "Error"
		errMessage := "batch failed"
		var errData string
		var trace []string
		if op.childCtx != nil {
			errType = op.childCtx.errType
			errMessage = op.childCtx.errMessage
			errData = op.childCtx.errData
			trace = op.childCtx.stackTrace
		}
		cerr := batchItemError(name, errType, errMessage, childErrorData{}, errData)
		cerr.StackTrace = trace
		return BatchResult[O]{}, cerr

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
	reason := CompletionAllCompleted
	// This path serves checkpoints written before the decision record
	// existed, so the recorded reason is not available and the policy is
	// re-derived from the replayed outcomes. For a custom policy that
	// calls ShouldComplete again.
	decider := newBatchDecider(parentName, options, totalItems)

	runItem := replayRunItem(items, fn)

	for i := 0; i < totalItems; i++ {
		itemName := itemNameForIndex(options, i)
		var result BatchItem[O]
		var err error

		if options.nesting == NestingFlat {
			childID, virtualChild := flatItemContext(ec, parentID, i, ec.owner)
			result, err = runFlatBatchItem[O](virtualChild, childID, i, itemName, options, runItem)
		} else {
			result, err = runNestedBatchItem[O](ec, parentID, parentName, itemName, i, options, childSubType, runItem)
		}

		if err != nil {
			return BatchResult[O]{}, err
		}

		results = append(results, result)

		decided, stop, derr := decider.terminal(i, result.Status)
		if derr != nil {
			return BatchResult[O]{}, derr
		}
		if stop {
			reason = decided
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
	// Every item has reported. From here to the checkpoint of the parent's
	// completion the batch is an executing span: its outcome belongs to
	// this invocation, so a handler blocked on a pending operation waits
	// for it to be recorded before it suspends. See awaitDrain.
	ec.suspend.enterExecuting()
	defer ec.suspend.exitExecuting()

	// Serialize the result. If an operation-level serdes is provided,
	// use it; otherwise serialize a default JSON summary.
	var serialized []byte
	var serErr error

	if options.resultSerdes != nil {
		serialized, serErr = options.resultSerdes.Marshal(ec.Context, ec.serdesCtx(id), result)
		if serErr != nil {
			return BatchResult[O]{}, newSerdesError(name, serdesDirectionMarshal, serErr)
		}
	} else {
		payload, payloadErr := fromBatchResult(ec.Context, result, options.itemSerdes, batchItemSerdesCtx(ec, options))
		if payloadErr != nil {
			return BatchResult[O]{}, payloadErr
		}
		serialized, serErr = json.Marshal(payload)
	}
	if serErr != nil {
		return BatchResult[O]{}, fmt.Errorf("durable: batch %q: serialize result: %w", name, serErr)
	}

	update := batchParentUpdate(ec, id, name, subType, OperationActionSucceed)
	if len(serialized) > checkpointSizeLimitBytes {
		update.ContextOptions = &ContextOptions{ReplayChildren: aws.Bool(true)}
		// The full aggregate is too large to store, so record a
		// size-independent decision record alongside ReplayChildren. It
		// carries the completion reason and which admitted items were
		// abandoned (STARTED) versus terminal, so replay reconstructs the
		// exact live shape instead of re-deriving it. The record is written
		// for both nesting modes: a NORMAL item is rebuilt from its own
		// checkpoint, a FLAT item from the operations recorded under the
		// batch. Either way the recorded reason is reused, so a custom
		// completion callback is not called again on replay.
		recordBytes, recordErr := json.Marshal(newBatchReplayRecord(result))
		if recordErr != nil {
			return BatchResult[O]{}, fmt.Errorf("durable: batch %q: serialize replay record: %w", name, recordErr)
		}
		update.Payload = aws.String(string(recordBytes))
	} else {
		update.Payload = aws.String(string(serialized))
	}
	if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
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

	// StackTrace holds the stack trace recorded for a failed item, one
	// frame per string, innermost first. Persisting it in the aggregate
	// payload keeps the item's user-code trace across replay.
	StackTrace []string `json:"stackTrace,omitempty"`

	childErrorData
}

// toBatchResult converts a deserialized checkpoint payload back into a
// typed [BatchResult] using the provided item serdes for result values.
// itemSctx returns the serdes context for the item at an input index.
func toBatchResult[O any](ctx context.Context, payload batchCheckpointPayload, itemSerdes Serdes, itemSctx func(index int) SerdesContext) (BatchResult[O], error) {
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
				if err := itemSerdes.Unmarshal(ctx, itemSctx(cp.Index), []byte(cp.Result), &out); err != nil {
					return BatchResult[O]{}, newSerdesError(batchItemOpName(cp.Name, cp.Index), serdesDirectionUnmarshal, err)
				}
			}
			items[i].Result = out
		case BatchItemFailed:
			cerr := batchItemError(cp.Name, cp.ErrType, cp.ErrMessage, cp.childErrorData, "")
			cerr.StackTrace = cp.StackTrace
			items[i].Err = cerr
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

	// ErrorData is the payload attached with [WithErrorData] anywhere in
	// the item's failure chain. Batch children use the wire ErrorData field
	// for this metadata object, so the user payload travels inside it.
	ErrorData string `json:"errorData,omitempty"`
}

// isZero reports whether no metadata field is set.
func (d childErrorData) isZero() bool {
	return d == childErrorData{}
}

// setInner records the wrapper's cause as the reconstructable leaf. The
// cause is normally a stand-in, so its record supplies the type and the
// message without the stand-in's type prefix.
func (d *childErrorData) setInner(cause error) {
	if cause == nil {
		return
	}
	rec := recordOf(cause)
	d.InnerErrType = rec.errType
	d.InnerErrMessage = truncateUTF8(rec.message, maxInnerErrMessageBytes)
}

// wrapperErrorData extracts reconstructable metadata from a known SDK
// wrapper error. It switches on the outermost SDK error in the chain — the
// same identity [wireErrorType] records as the checkpointed ErrorType — so
// the metadata always describes the type that [reconstructInnerError] will
// rebuild. The second result is false for other error types.
func wrapperErrorData(err error) (childErrorData, bool) {
	d := childErrorData{ErrorData: errorDataOf(err)}
	switch e := outermostSDKError(err).(type) {
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
	case *CallbackExternalError:
		d.Name = e.Name
		d.CallbackID = e.CallbackID
		d.setInner(e.Err)
	case *CallbackTimeoutError:
		d.Name = e.Name
		d.CallbackID = e.CallbackID
		d.TimedOut = true
		d.setInner(e.Err)
	case *CallbackSubmitterError:
		d.Name = e.Name
		d.CallbackID = e.CallbackID
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
// suitable for ErrorObject.ErrorData. Returns nil if the error carries
// neither reconstructable inner wrapper data nor a [WithErrorData]
// payload.
func encodeChildErrorData(err error) *string {
	d, ok := wrapperErrorData(err)
	if !ok {
		if data := errorDataOf(err); data != "" {
			d = childErrorData{ErrorData: data}
			ok = true
		}
	}
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
// [WaitForConditionError], [CallbackError] and its subtypes,
// [SerdesError]) are reconstructed so errors.As — and, for a timed-out
// callback, errors.Is(err, [ErrCallbackTimedOut]) — succeed after replay,
// matching live behavior. Any other type is rebuilt by
// [errorRecord.cause]: an SDK type by name, or the leaf stand-in.
//
// meta carries the wrapper field values from the aggregate checkpoint
// payload; errData carries the JSON-encoded childErrorData from the
// ErrorObject.ErrorData field. When meta is empty and errData is present
// and parseable (Route B, wire-only replay), errData supplies the values.
// When both are absent or malformed, fields fall back to zero values —
// the function never fails.
func reconstructInnerError(errType, errMessage string, meta childErrorData, errData string) error {
	meta = meta.merged(errData)

	inner := errorRecord{errType: meta.InnerErrType, message: meta.InnerErrMessage, data: meta.ErrorData}
	if inner.errType == "" && inner.message == "" {
		inner = errorRecord{errType: "Error", message: errMessage, data: meta.ErrorData}
	}

	switch errType {
	case "StepError":
		return newStepError(meta.Name, meta.Attempts, inner)
	case "WaitForConditionError":
		return newWaitForConditionError(meta.Name, meta.Attempts, inner)
	case "CallbackError":
		var sentinel error
		if meta.TimedOut {
			sentinel = ErrCallbackTimedOut
		}
		return &CallbackError{
			Name: meta.Name, CallbackID: meta.CallbackID,
			ErrorType: inner.errType, Message: inner.message, ErrorData: inner.data,
			Err: inner.standIn(sentinel),
		}
	case "CallbackExternalError":
		return newCallbackExternalError(meta.Name, meta.CallbackID, inner)
	case "CallbackTimeoutError":
		return newCallbackTimeoutError(meta.Name, meta.CallbackID, inner)
	case "CallbackSubmitterError":
		return newCallbackSubmitterError(meta.Name, meta.CallbackID, inner)
	case "SerdesError":
		return &SerdesError{Operation: meta.Name, Direction: meta.Direction, Err: inner.standIn(nil)}
	default:
		return errorRecord{errType: errType, message: errMessage, data: meta.ErrorData}.cause("", nil)
	}
}

// merged returns d with the values parsed from errData when d is empty and
// errData holds a childErrorData object.
func (d childErrorData) merged(errData string) childErrorData {
	if errData != "" && d.isZero() {
		var parsed childErrorData
		if json.Unmarshal([]byte(errData), &parsed) == nil {
			return parsed
		}
	}
	return d
}

// batchItemError builds the [ChildContextError] for a failed batch item
// from its recorded failure. The same function serves the live path (via
// [liveBatchItemError]) and every replay route, so the error a caller
// inspects has the same type chain, field values, and message on the first
// invocation and on replay.
func batchItemError(itemName, errType, errMessage string, meta childErrorData, errData string) *ChildContextError {
	meta = meta.merged(errData)
	return &ChildContextError{
		Name: itemName, ErrorType: errType, Message: errMessage, ErrorData: meta.ErrorData,
		Err: reconstructInnerError(errType, errMessage, meta, ""),
	}
}

// liveBatchItemError builds the [ChildContextError] for a batch item that
// failed on this invocation, by recording the failure the same way the
// checkpoint does and rebuilding from that record. trace is the stack
// trace the checkpoint recorded for the failure.
func liveBatchItemError(itemName string, fnErr error, trace []string) *ChildContextError {
	meta, _ := wrapperErrorData(fnErr)
	if meta.isZero() {
		meta = childErrorData{ErrorData: errorDataOf(fnErr)}
	}
	cerr := batchItemError(itemName, wireErrorType(fnErr), fnErr.Error(), meta, "")
	cerr.StackTrace = trace
	return cerr
}

// flatItemTraceError carries the stack trace recorded for a FLAT-mode
// batch item failure. FLAT mode keeps the item function's own error as
// the item error — no child context wraps it — so the error has no trace
// field of its own. The wrapper supplies the trace through the StackTrace
// method, which trace capture reads from an error's chain, so the failure
// still points at the item's user code when the handler returns it. The
// wrapper contributes no type and no message: [unwrapErrorData] strips it
// wherever the wire ErrorType or message is derived.
type flatItemTraceError struct {
	err   error
	trace []string
}

func (e *flatItemTraceError) Error() string { return e.err.Error() }

func (e *flatItemTraceError) Unwrap() error { return e.err }

// StackTrace returns the recorded trace, one frame per string, innermost
// first. [suppliedStackTrace] finds it through this method.
func (e *flatItemTraceError) StackTrace() []string { return e.trace }

// itemErrorTrace returns the stack trace the SDK recorded for a failed
// item's error: the trace on the outermost SDK error in its chain, or the
// one a [flatItemTraceError] wrapper carries. Both were produced by the
// SDK's own capture, so they honour [WithStackTraces] and the
// [MaxStackTraceFrames] bound. A trace that a user error supplies through
// its own StackTrace method is not read here; it reaches the record only
// after capture has bounded it, through the wrapper. It returns nil when
// the chain carries no recorded trace.
func itemErrorTrace(err error) []string {
	if trace := recordOf(err).stackTrace; len(trace) > 0 {
		return trace
	}
	var wrapped *flatItemTraceError
	if errors.As(err, &wrapped) {
		return wrapped.trace
	}
	return nil
}

// flatItemError returns the error recorded for a FLAT-mode item failure.
// An error whose chain already carries an SDK-recorded trace keeps it:
// that trace was recorded closer to the failure's origin. Otherwise
// fnTrace, the trace capture produced when the item function handed the
// SDK the failure, is carried in a [flatItemTraceError] wrapper. fnTrace
// is already bounded and is nil when capture is disabled, so the wrapper
// is the only route by which an item error's own trace is recorded.
func flatItemError(fnErr error, fnTrace []string) error {
	if len(fnTrace) == 0 || len(recordOf(fnErr).stackTrace) > 0 {
		return fnErr
	}
	return &flatItemTraceError{err: fnErr, trace: fnTrace}
}

// fromBatchResult converts a live [BatchResult] into the checkpoint payload
// format for serialization. Item results are pre-serialized through the
// provided item serdes; itemSctx returns the serdes context for the item at
// an input index.
func fromBatchResult[O any](ctx context.Context, result BatchResult[O], itemSerdes Serdes, itemSctx func(index int) SerdesContext) (batchCheckpointPayload, error) {
	cpItems := make([]batchCheckpointItem, len(result.Items))
	for i, item := range result.Items {
		cpItems[i] = batchCheckpointItem{
			Index:  item.Index,
			Name:   item.Name,
			Status: item.Status,
		}
		switch item.Status {
		case BatchItemSucceeded:
			raw, err := itemSerdes.Marshal(ctx, itemSctx(item.Index), item.Result)
			if err != nil {
				return batchCheckpointPayload{}, newSerdesError(batchItemOpName(item.Name, item.Index), serdesDirectionMarshal, err)
			}
			cpItems[i].Result = string(raw)
		case BatchItemFailed:
			if item.Err != nil {
				cpItems[i].StackTrace = itemErrorTrace(item.Err)
				cpItems[i].ErrType = wireErrorType(item.Err)
				cpItems[i].ErrMessage = item.Err.Error()
				// Extract inner error details for child context errors.
				var childErr *ChildContextError
				if errors.As(item.Err, &childErr) && childErr.Err != nil {
					cpItems[i].ErrType = childErr.ErrorType
					cpItems[i].ErrMessage = childErr.Message
					if cpItems[i].ErrType == "" {
						cpItems[i].ErrType = wireErrorType(childErr.Err)
						cpItems[i].ErrMessage = childErr.Err.Error()
					}
					// Persist inner wrapper metadata for known SDK types
					// so replay reconstructs the concrete wrapper chain
					// with matching fields, causes, and sentinels.
					if meta, ok := wrapperErrorData(childErr.Err); ok {
						cpItems[i].childErrorData = meta
					}
					if cpItems[i].ErrorData == "" {
						cpItems[i].ErrorData = childErr.ErrorData
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

// replayRunItem returns the item function a replay path runs over a batch's
// recorded operations. Map replays with the caller's items and fn. Parallel
// replays with placeholder items and a fn that dispatches to the branch.
func replayRunItem[I, O any](items []I, fn func(Context, I, int) (O, error)) batchItemFunc[O] {
	return func(childCtx Context, index int) (O, []string, error) {
		if fn != nil && items != nil {
			return runBatchItemFunc(childCtx, index, fn, func() (O, error) {
				return fn(childCtx, items[index], index)
			})
		}
		var zero O
		return zero, nil, fmt.Errorf("durable: cannot replay parallel branches without branch functions")
	}
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
	runItem := replayRunItem(items, fn)
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

// replayFlatBatchChildrenFromRecord reconstructs a FLAT batch result from
// the decision record. A FLAT item has no checkpoint of its own; the
// operations inside it are recorded under the batch. So each item the record
// marks terminal is run again in its virtual context, where every recorded
// operation replays without executing, and its outcome is rebuilt from those
// operations. Each item the record marks abandoned is reported STARTED and
// is not run. The recorded reason is returned as is, so a custom completion
// callback is not called again. ec is the batch's own context in
// modeReplaySucceededContext, and parentID is the batch's operation ID.
func replayFlatBatchChildrenFromRecord[I, O any](
	ec *execContext,
	parentID string,
	record batchReplayRecord,
	items []I,
	fn func(Context, I, int) (O, error),
	options batchOptions,
) (BatchResult[O], error) {
	abandoned := record.abandonedSet()
	runItem := replayRunItem(items, fn)
	results := make([]BatchItem[O], 0, record.StartedTotal)
	for i := 0; i < record.StartedTotal; i++ {
		itemName := itemNameForIndex(options, i)
		if abandoned[i] {
			results = append(results, BatchItem[O]{
				Index:  i,
				Name:   itemName,
				Status: BatchItemStarted,
			})
			continue
		}
		childID, virtualChild := flatItemContext(ec, parentID, i, ec.owner)
		item, err := runFlatBatchItem[O](virtualChild, childID, i, itemName, options, runItem)
		if err != nil {
			return BatchResult[O]{}, err
		}
		results = append(results, item)
	}
	return BatchResult[O]{Items: results, Reason: record.Reason}, nil
}

// batchParentUpdate builds an operation update for the parent batch context.
func batchParentUpdate(ec *execContext, id, name, subType string, action OperationAction) OperationUpdate {
	update := OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    OperationTypeContext,
		SubType: aws.String(subType),
		Action:  action,
	}
	if name != "" {
		update.Name = aws.String(name)
	}
	if parent := ec.parentOperationID(); parent != "" {
		update.ParentId = aws.String(hashID(parent))
	}
	return update
}

// batchChildUpdate builds an operation update for a batch child (iteration
// or branch).
func batchChildUpdate(ec *execContext, childID, childName, childSubType, parentID string, action OperationAction) OperationUpdate {
	update := OperationUpdate{
		Id:       aws.String(hashID(childID)),
		Type:     OperationTypeContext,
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

// batchItemOpName returns the operation name recorded for a batch item's
// serdes failures: the item's configured name, or an index-based fallback
// for unnamed items.
func batchItemOpName(itemName string, index int) string {
	if itemName != "" {
		return itemName
	}
	return fmt.Sprintf("item %d", index)
}

// batchDecider evaluates a batch's completion policy as its items reach
// terminal states. One decider serves one run of a batch: it tracks the
// per-item status the policy is judged on and fires the decision once.
//
// A threshold policy is judged on the success and failure counts. A
// custom policy is judged by the ShouldComplete callback on a
// [BatchProgress] snapshot built from the per-item statuses. Both are
// evaluated at the same moment, directly after an item's outcome is
// recorded, so the two kinds of policy see the same progress.
type batchDecider struct {
	name         string
	cfg          CompletionConfig
	names        func(index int) string
	status       []BatchItemStatus
	successCount int
	failureCount int
}

func newBatchDecider(name string, options batchOptions, totalItems int) *batchDecider {
	return &batchDecider{
		name:   name,
		cfg:    options.completion,
		names:  func(index int) string { return itemNameForIndex(options, index) },
		status: make([]BatchItemStatus, totalItems),
	}
}

// started records that the item at index was admitted and is in flight.
func (d *batchDecider) started(index int) {
	d.status[index] = BatchItemStarted
}

// terminal records the item's terminal status and evaluates the policy.
// stop is true when the batch must complete now, with reason as its
// completion reason. err reports a ShouldComplete callback that panicked
// or returned an invalid decision; the batch operation fails with it.
func (d *batchDecider) terminal(index int, status BatchItemStatus) (reason CompletionReason, stop bool, err error) {
	d.status[index] = status
	switch status {
	case BatchItemSucceeded:
		d.successCount++
	case BatchItemFailed:
		d.failureCount++
	}

	if d.cfg.ShouldComplete == nil {
		if shouldStopMin(d.cfg, d.successCount) {
			return CompletionMinSuccessfulReached, true, nil
		}
		if shouldStopFailure(d.cfg, d.failureCount, len(d.status)) {
			return CompletionFailureToleranceExceeded, true, nil
		}
		return CompletionAllCompleted, false, nil
	}

	decision, err := d.callShouldComplete()
	if err != nil {
		return 0, false, err
	}
	if !decision.complete {
		return CompletionAllCompleted, false, nil
	}
	reason, ok := decision.reason()
	if !ok {
		return 0, false, fmt.Errorf("durable: batch %q: ShouldComplete returned an invalid CompletionOutcome %d; use CompleteBatch with CompletionOutcomeSucceeded or CompletionOutcomeFailed", d.name, decision.outcome)
	}
	return reason, true, nil
}

// progress builds the snapshot passed to ShouldComplete. The slice is
// freshly allocated on every call because the callback may keep it.
func (d *batchDecider) progress() BatchProgress {
	items := make([]BatchItemProgress, len(d.status))
	for i, s := range d.status {
		items[i] = BatchItemProgress{Index: i, Name: d.names(i), Status: s}
	}
	return BatchProgress{
		TotalCount:     len(d.status),
		CompletedCount: d.successCount + d.failureCount,
		SuccessCount:   d.successCount,
		FailureCount:   d.failureCount,
		Items:          items,
	}
}

// callShouldComplete runs the user's callback. A panic in the callback
// becomes an error so it fails the batch operation instead of unwinding
// the coordinator with items still in flight.
func (d *batchDecider) callShouldComplete() (decision CompletionDecision, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("durable: batch %q: ShouldComplete panicked: %v", d.name, r)
		}
	}()
	return d.cfg.ShouldComplete(d.progress()), nil
}

// shouldStopMin checks if the min-successful threshold has been met.
func shouldStopMin(cfg CompletionConfig, successCount int) bool {
	if cfg.MinSuccessful <= 0 {
		return false
	}
	return successCount >= cfg.MinSuccessful
}

// hasThreshold reports whether any completion threshold is set. A config
// with no threshold is the fail-fast default.
func (c CompletionConfig) hasThreshold() bool {
	return c.MinSuccessful > 0 || c.ToleratedFailureCount != nil || c.ToleratedFailurePercentage != nil
}

// shouldStopFailure checks if the failure tolerance has been exceeded.
//
// With no threshold set the policy is fail-fast: the first failure
// exceeds it. Otherwise only the tolerances that are set are checked, so a
// config that sets only MinSuccessful tolerates every failure.
func shouldStopFailure(cfg CompletionConfig, failureCount, totalItems int) bool {
	if !cfg.hasThreshold() {
		return failureCount > 0
	}
	if cfg.ToleratedFailureCount != nil && failureCount > *cfg.ToleratedFailureCount {
		return true
	}
	// The percentage comparison is exact. Comparing failureCount*100
	// against pct*totalItems is the same test as failureCount/totalItems
	// > pct/100 without dividing, so 1 of 3 failures (33.3%) exceeds a
	// threshold of 33.
	if cfg.ToleratedFailurePercentage != nil && totalItems > 0 {
		if failureCount*100 > *cfg.ToleratedFailurePercentage*totalItems {
			return true
		}
	}
	return false
}
