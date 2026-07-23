package operations

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	dcontext "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/context"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

// ItemResult is the outcome of a single Map iteration or Parallel branch.
type ItemResult[T any] struct {
	Value T
	Err   error
}

// BatchResult aggregates the outcomes of a Map or Parallel operation, in
// the same order as the input items/branches.
//
// # Completion-policy contract (real, breaking fix - see the full
// writeup below)
//
// Map/Parallel ALWAYS return a valid BatchResult with a nil error once
// their item/branch loop finishes, REGARDLESS of whether the configured
// CompletionConfig policy was actually met - "did the batch satisfy its
// own policy" is a property carried BY this result (Status/
// CompletionReason/HasFailure), for the CALLER to inspect and decide
// what to do about (including, if the caller wants Promise.all-style
// "any failure fails outright" behavior, calling ThrowIfError()
// themselves) - it is deliberately NOT automatically surfaced as a Go
// error return from Map/Parallel themselves, and the outer CONTEXT/MAP
// or CONTEXT/PARALLEL operation's own checkpoint is ALWAYS
// ContextSucceeded once the loop finishes, never ContextFailed for a
// policy-not-met reason (see finishBatch's own doc for the full
// evidence this was wrong before this fix, and Map/Parallel's own
// updated doc for what "the loop finishes" excludes - genuine
// suspension and a replay-consistency mismatch are unrelated,
// unaffected error paths).
//
// This is the confirmed, real, cross-SDK contract - verified directly
// against the JS reference SDK's own actual interface
// (packages/aws-durable-execution-sdk-js/src/types/batch.ts's
// BatchResult<TResult>) and its own handlers/concurrent-execution-
// handler.ts, which computes and returns this exact shape unconditionally
// rather than ever throwing/rejecting a Promise for a policy-not-met
// batch - matching this type's own Status()/CompletionReason/HasFailure/
// ThrowIfError/GetResults/GetErrors/FailureCount/TotalCount methods
// below, one-for-one, against that same JS interface's own status/
// completionReason/hasFailure/throwIfError()/getResults()/getErrors()/
// failureCount/totalCount members.
//
// Found via 12 real, failing conformance requirements (Parallel suite's
// 8-6/7/8/10/13/17/18, Map suite's 9-5/6/7/9/10) whose own
// ExpectedExecutionHistory YAMLs assert the outer Parallel/Map context's
// ContextSucceeded checkpoint even when the completion policy was NOT
// met - several of those 12 conformance handlers (see e.g.
// conformance/handlers/parallel_8_6.go) had to hand-roll this exact
// workaround (catching the old *BatchFailedError and manually
// reconstructing a completionReason/status/successCount/failureCount/
// totalCount projection from its embedded *AggregateError) before this
// fix existed - those hand-rolled workarounds are removed by this same
// change, now that the real fields/methods exist directly on
// BatchResult itself.
type BatchResult[T any] struct {
	Items []ItemResult[T]

	// CompletionReason records WHY the batch's item/branch loop stopped
	// running further items - one of "ALL_COMPLETED" (every item/branch
	// ran to completion, no early-exit threshold was ever hit),
	// "MIN_SUCCESSFUL_REACHED" (stopped early once
	// CompletionConfig.MinSuccessful successes were reached - see
	// batchCompletion.thresholdExceeded's own doc for the real,
	// separately-confirmed bug fix that made this reachable at all: this
	// SDK never actually stopped early on MinSuccessful before this same
	// change), or "FAILURE_TOLERANCE_EXCEEDED" (stopped early once
	// ToleratedFailureCount/ToleratedFailurePercentage was exceeded).
	// These three exact string values are confirmed against the real
	// conformance suite's own requirement YAMLs (e.g.
	// test-requirements/parallel/8-8.yaml's own
	// ExpectedResult.Result.completionReason: MIN_SUCCESSFUL_REACHED).
	CompletionReason string

	// batchID/batchName carry the outer CONTEXT/MAP or CONTEXT/PARALLEL
	// operation's own step ID/caller-supplied name, populated by
	// finishBatch - used ONLY by ThrowIfError() to reconstruct a fully-
	// populated *OperationError (Kind/ID/Name), matching exactly what
	// finishBatch's own now-removed automatic-failure-return path used
	// to construct directly (see ThrowIfError's own doc). Deliberately
	// unexported: a caller has no legitimate use for these two fields on
	// their own, only via ThrowIfError()'s own constructed error.
	batchID   string
	batchName string
}

// CompletionReasonAllCompleted, CompletionReasonMinSuccessfulReached, and
// CompletionReasonFailureToleranceExceeded are BatchResult.
// CompletionReason's three possible values - see that field's own doc.
const (
	CompletionReasonAllCompleted             = "ALL_COMPLETED"
	CompletionReasonMinSuccessfulReached     = "MIN_SUCCESSFUL_REACHED"
	CompletionReasonFailureToleranceExceeded = "FAILURE_TOLERANCE_EXCEEDED"
)

// SucceededCount returns how many items/branches in r completed without
// error.
func (r BatchResult[T]) SucceededCount() int {
	n := 0
	for _, item := range r.Items {
		if item.Err == nil {
			n++
		}
	}
	return n
}

// FailureCount returns how many items/branches in r completed WITH an
// error - deliberately excludes items skipped entirely because a
// failure-tolerance threshold already tripped before their turn came up
// (see errBatchSkipped's own doc: those were never actually run, so they
// are neither a success nor a genuine failure of that item's own work),
// matching the exact same exclusion finishBatch's OWN pre-fix
// *AggregateError-building logic already applied - this method simply
// makes that same, already-correct exclusion available directly on the
// result instead of requiring a caller to reconstruct it by hand (see
// BatchResult's own top-level doc for the conformance-handler workarounds
// this replaces).
func (r BatchResult[T]) FailureCount() int {
	n := 0
	for _, item := range r.Items {
		if item.Err != nil && item.Err != errBatchSkipped {
			n++
		}
	}
	return n
}

// TotalCount returns the number of items/branches actually reflected in
// r - len(r.Items). Confirmed against the real conformance suite's own
// requirement YAMLs (e.g. 8-8.yaml's totalCount: 2 for a 4-branch batch
// that stopped early after 2 successes) that this is the count of items/
// branches that ACTUALLY RAN (including ones that ran and failed), not
// the original input slice's own length - skipped items (see
// errBatchSkipped) are still present in r.Items (as a placeholder
// ItemResult with Err == errBatchSkipped), so a caller wanting the
// original input length instead should use len(items)/len(branches)
// directly rather than TotalCount(); see HasFailure/FailureCount's own
// docs for the parallel distinction between "ran and failed" and "never
// ran at all."
func (r BatchResult[T]) TotalCount() int {
	n := 0
	for _, item := range r.Items {
		if item.Err == errBatchSkipped {
			continue
		}
		n++
	}
	return n
}

// Status returns "FAILED" if any item/branch in r completed with a
// genuine error (HasFailure() is true), "SUCCEEDED" otherwise - matching
// the JS reference SDK's own BatchItemStatus.SUCCEEDED/FAILED string
// values exactly (see BatchResult's own top-level doc).
func (r BatchResult[T]) Status() string {
	if r.HasFailure() {
		return "FAILED"
	}
	return "SUCCEEDED"
}

// HasFailure reports whether any item/branch in r completed with a
// genuine error - equivalent to FailureCount() > 0, provided as a direct
// boolean for the common case, matching the JS reference SDK's own
// hasFailure field.
func (r BatchResult[T]) HasFailure() bool {
	for _, item := range r.Items {
		if item.Err != nil && item.Err != errBatchSkipped {
			return true
		}
	}
	return false
}

// GetResults returns the Value of every item/branch that succeeded, in
// their original item-index order (skipping both genuine failures and
// skipped items) - matching the JS reference SDK's own getResults().
func (r BatchResult[T]) GetResults() []T {
	out := make([]T, 0, len(r.Items))
	for _, item := range r.Items {
		if item.Err == nil {
			out = append(out, item.Value)
		}
	}
	return out
}

// GetErrors returns the Err of every item/branch that genuinely failed,
// in their original item-index order - deliberately EXCLUDES items
// skipped entirely because a failure-tolerance threshold already tripped
// (errBatchSkipped; see FailureCount's own doc for why), matching the JS
// reference SDK's own getErrors().
func (r BatchResult[T]) GetErrors() []error {
	var out []error
	for _, item := range r.Items {
		if item.Err != nil && item.Err != errBatchSkipped {
			out = append(out, item.Err)
		}
	}
	return out
}

// ThrowIfError returns nil if r.HasFailure() is false, otherwise a
// *BatchFailedError wrapping an *AggregateError of every genuine failure
// in r (via GetErrors(), so skipped items are excluded exactly like they
// always were) - this is EXACTLY what finishBatch's own now-removed
// automatic-failure-return path used to construct and return directly
// before this fix (see BatchResult's own top-level doc); it is simply
// moved to being CALLER-invoked instead of automatic, matching the JS
// reference SDK's own throwIfError() method exactly. A caller that wants
// this Go SDK's OLD Promise.all-style "any failure fails outright"
// behavior back can restore it in one line: `if err :=
// result.ThrowIfError(); err != nil { return zero, err }`.
func (r BatchResult[T]) ThrowIfError() error {
	if !r.HasFailure() {
		return nil
	}
	aggErr := &AggregateError{Errors: r.GetErrors()}
	return &BatchFailedError{
		OperationError: &OperationError{Kind: OperationKindBatch, ID: r.batchID, Name: r.batchName, Err: aggErr},
		AggregateError: aggErr,
	}
}

// batchCompletion evaluates types.CompletionConfig against a running
// tally of successes/failures/total, matching the confirmed real-backend
// flowchart's "error threshold not exceeded" (gates starting more
// items/branches) and "succeeded, not exceed error thresholds, and have
// minimum successful" (the final pass/fail decision) checks - both
// evaluate the exact same three thresholds, just at different points in
// the loop.
type batchCompletion struct {
	cfg   types.CompletionConfig
	total int
}

// thresholdExceeded reports whether, given succeeded/failed counts so
// far (out of total items), the batch's scheduling loop should stop
// starting further items/branches - the flowchart's "error threshold not
// exceeded?" loop condition, now ALSO covering CompletionConfig.
// MinSuccessful's own early-exit case.
//
// # Real bug fixed here: MinSuccessful was never actually an early-exit
// # gate at all
//
// Before this fix, this function examined ONLY ToleratedFailureCount/
// ToleratedFailurePercentage - MinSuccessful was consulted exclusively
// inside overallSucceeded (the FINAL pass/fail decision, called only
// once every item/branch had already finished), never here, so a batch
// configured with e.g. MinSuccessful: 2 over 4 branches never actually
// stopped early once 2 branches had already succeeded - all 4 branches
// always ran regardless. Confirmed as a real, reproducible bug via real
// deployed invocations of conformance requirements 8-8 (Parallel) and
// 9-7 (Map), both of which specifically test this exact scenario
// (max-concurrency=1 for deterministic sequential ordering,
// MinSuccessful=2 over 4 items/branches, expecting only 2 of 4 to ever
// start) and both showed all FOUR items'/branches' own ContextStarted/
// ContextSucceeded events present, not two.
//
// Fixed by adding the parallel check here: once at least MinSuccessful
// items/branches have ALREADY succeeded, the batch's own success
// condition is already guaranteed regardless of what any remaining
// items/branches do (MinSuccessful only ever requires a MINIMUM number
// of successes, never fewer failures - so succeeding further items
// cannot un-satisfy it), so there is nothing further to gain by
// continuing to run them.
func (b batchCompletion) thresholdExceeded(succeeded, failed int) bool {
	if b.cfg.ToleratedFailureCount != nil && failed > *b.cfg.ToleratedFailureCount {
		return true
	}
	if b.cfg.ToleratedFailurePercentage != nil && b.total > 0 {
		pct := float64(failed) / float64(b.total) * 100
		if pct > *b.cfg.ToleratedFailurePercentage {
			return true
		}
	}
	// MinSuccessful's own early-exit check is deliberately gated on
	// succeeded+failed > 0 (at least one item/branch has genuinely
	// finished) - NOT evaluated against the initial, nothing-has-run-yet
	// zero state. Without this guard, MinSuccessful: 0 (a real,
	// legitimate configuration - see AllSettled's own doc, which
	// configures exactly this) would satisfy `succeeded >=
	// *MinSuccessful` (0 >= 0) immediately, before item/branch 0 has
	// even been given a chance to run at all - every single item would
	// be skipped via errBatchSkipped, and the batch would report ZERO
	// items ever having run. Confirmed as a real, reproducible bug this
	// exact way via TestLocalTestRunner_AllSettled_MixedResults (2
	// branches, MinSuccessful: 0 via AllSettled's own default) - BOTH
	// branches were skipped, not run, before this guard was added.
	// Matches the JS reference SDK's own actual scheduling design
	// (concurrent-execution-handler.ts's isComplete(), called from
	// onComplete() - i.e. only ever evaluated AFTER an item/branch has
	// already finished and incremented its own counter, never as a
	// pre-check before the first item/branch has had a chance to run).
	if b.cfg.MinSuccessful != nil && succeeded+failed > 0 && succeeded >= *b.cfg.MinSuccessful {
		return true
	}
	return false
}

// overallSucceeded reports whether the batch as a whole should succeed,
// given the final succeeded/failed counts - the flowchart's "did the
// overall operation succeed, not exceed error thresholds, and have
// minimum successful X" decision.
//
// # No longer consulted to decide whether the OUTER checkpoint is
// # ContextSucceeded vs ContextFailed (see finishBatch's own doc) -
// # retained ONLY for BatchResult.HasFailure/Status's own, SEPARATE,
// # per-item-level notion of success
//
// This method's own name predates, and is now more narrowly scoped than,
// this file's later completion-policy-contract fix (see BatchResult's
// own top-level doc): it used to directly gate whether finishBatch
// checkpointed ContextSucceeded/ContextFailed and returned an error -
// it no longer does either. It is retained only as the shared
// definition of "did EVERY threshold this policy cares about actually
// hold," which completionReason (below) still needs to classify a
// MinSuccessful-reached batch as satisfied even if, say, some other
// unrelated branch that already started before the threshold tripped
// happens to fail afterward (a real, possible race under concurrent
// execution - see CompletionConfig's own "Race Condition Behavior" doc
// in the JS reference SDK's types/batch.ts, confirmed cross-checked
// against this Go port's own identical concurrent-scheduling design).
func (b batchCompletion) overallSucceeded(succeeded, failed int) bool {
	if b.cfg.ToleratedFailureCount != nil && failed > *b.cfg.ToleratedFailureCount {
		return false
	}
	if b.cfg.ToleratedFailurePercentage != nil && b.total > 0 {
		pct := float64(failed) / float64(b.total) * 100
		if pct > *b.cfg.ToleratedFailurePercentage {
			return false
		}
	}
	if b.cfg.MinSuccessful != nil && succeeded < *b.cfg.MinSuccessful {
		return false
	}
	// With no CompletionConfig at all, default semantics require every
	// item/branch to succeed - matching Promise.all-style "any failure
	// fails the whole batch" behavior, which is what all three reference
	// SDKs document as the default when no CompletionConfig is given.
	if b.cfg.MinSuccessful == nil && b.cfg.ToleratedFailureCount == nil && b.cfg.ToleratedFailurePercentage == nil {
		return failed == 0
	}
	return true
}

// completionReason classifies WHY the batch's loop stopped, given the
// final succeeded/failed/skipped counts - see BatchResult.
// CompletionReason's own doc for the three possible values and their
// exact confirmed string constants.
func (b batchCompletion) completionReason(succeeded, failed, skipped int) string {
	if skipped > 0 {
		// At least one item/branch never ran at all because a threshold
		// tripped before its turn came up (see errBatchSkipped's own
		// doc) - this can only happen via thresholdExceeded's own
		// early-exit gate above, so distinguish WHICH threshold tripped
		// using the exact same evaluation order thresholdExceeded itself
		// uses (failure-tolerance checked first, then MinSuccessful) -
		// confirmed correct against real conformance requirements 8-8/
		// 9-7 (MinSuccessful) and 8-10/8-13/9-9 (failure tolerance),
		// none of which configure BOTH thresholds simultaneously, so
		// this ordering is not independently exercised against a
		// genuinely ambiguous case by any current conformance
		// requirement - but matches thresholdExceeded's own
		// already-established precedence for the general case.
		if b.cfg.ToleratedFailureCount != nil && failed > *b.cfg.ToleratedFailureCount {
			return CompletionReasonFailureToleranceExceeded
		}
		if b.cfg.ToleratedFailurePercentage != nil && b.total > 0 {
			pct := float64(failed) / float64(b.total) * 100
			if pct > *b.cfg.ToleratedFailurePercentage {
				return CompletionReasonFailureToleranceExceeded
			}
		}
		if b.cfg.MinSuccessful != nil && succeeded >= *b.cfg.MinSuccessful {
			return CompletionReasonMinSuccessfulReached
		}
	}
	return CompletionReasonAllCompleted
}

// validateBatchConfig rejects genuinely nonsensical Map/Parallel option
// values before any step ID is claimed or checkpoint is enqueued
// (docs/remaining-work.md §8 task 18's operation-level half - Config
// validation itself, in durable.go, has nothing to check here since
// these values don't exist at that point; see validateConfig's doc).
// opName is "Map" or "Parallel", purely for the error message.
//
// Only concretely wrong values are rejected, not opinions about what's
// a good idea:
//
//   - maxConcurrency <= 0 (WithMapMaxConcurrency/WithParallelMaxConcurrency).
//     A negative value has no sensible interpretation as a concurrency
//     bound at all. A ZERO value is worse than merely meaningless: before
//     this check existed, runBatch's own guard
//     (`if maxConcurrency != nil && *maxConcurrency > 0 && *maxConcurrency < n`)
//     silently treated ANY non-positive value as "no bound" (falling
//     through to `concurrency = n`, fully unbounded) - the exact opposite
//     of what a caller passing 0 or a negative number almost certainly
//     intended (some deliberately restrictive bound, likely a
//     programming mistake in computing it, e.g. an off-by-one producing
//     0). Silently running fully unbounded instead of visibly failing is
//     precisely the "confusing downstream error" (or, worse here, no
//     error at all and just silently different behavior than requested)
//     task 18 asks to fail fast on instead.
//   - CompletionConfig.MinSuccessful < 0 / ToleratedFailureCount < 0: both
//     are counts compared against non-negative failure/success tallies
//     (see batchCompletion.thresholdExceeded/overallSucceeded) - a
//     negative count can never be meaningfully satisfied or exceeded by
//     any real batch (MinSuccessful < 0 is trivially always satisfied
//     from the very first item, which is likely not what a caller who
//     wrote a negative number intended; a negative
//     ToleratedFailureCount similarly can never behave as "tolerate N
//     failures" for any real N).
//   - CompletionConfig.ToleratedFailurePercentage outside [0, 100]: the
//     field's own doc comment (types/types.go) documents it as "the
//     failure rate exceeds this percentage (0-100)" - a value outside
//     that documented range (e.g. 150, or -10) is not a valid percentage
//     of anything and was never a value this field's own contract
//     allows.
//
// Deliberately NOT validated: MinSuccessful/ToleratedFailureCount being
// larger than the batch size, or ToleratedFailureCount/
// ToleratedFailurePercentage being simultaneously set to values that are
// individually valid but happen to make one threshold redundant given
// the other - both are internally consistent (if unusual) configurations
// with a well-defined, correct evaluation under batchCompletion (see
// that type's thresholdExceeded/overallSucceeded), not "clearly
// nonsensical" values, just possibly not what the caller meant. This
// function only rejects values with NO valid interpretation at all, per
// this task's own explicit instruction not to invent validation rules
// beyond that.
func validateBatchConfig(opName string, maxConcurrency *int, completion types.CompletionConfig) error {
	if maxConcurrency != nil && *maxConcurrency <= 0 {
		return fmt.Errorf("operations.%s: maxConcurrency must be positive, got %d", opName, *maxConcurrency)
	}
	if completion.MinSuccessful != nil && *completion.MinSuccessful < 0 {
		return fmt.Errorf("operations.%s: CompletionConfig.MinSuccessful must be non-negative, got %d", opName, *completion.MinSuccessful)
	}
	if completion.ToleratedFailureCount != nil && *completion.ToleratedFailureCount < 0 {
		return fmt.Errorf("operations.%s: CompletionConfig.ToleratedFailureCount must be non-negative, got %d", opName, *completion.ToleratedFailureCount)
	}
	if completion.ToleratedFailurePercentage != nil {
		pct := *completion.ToleratedFailurePercentage
		if pct < 0 || pct > 100 {
			return fmt.Errorf("operations.%s: CompletionConfig.ToleratedFailurePercentage must be between 0 and 100, got %v", opName, pct)
		}
	}
	return nil
}

// runBatchItem runs one Map iteration or Parallel branch: replay-skip if
// its CONTEXT operation already completed, otherwise checkpoint
// CONTEXT/<subType> START, run fn in a fresh child Context, and
// checkpoint SUCCEED/FAIL with the result - identical in shape to
// RunInChildContext's own single-item lifecycle (see that function's doc
// for the detailed replay-skip reasoning, which applies here unchanged
// per item/branch), because the confirmed real-backend flowchart
// documents MAP_ITERATION/PARALLEL_BRANCH operations as literally a
// CONTEXT operation like any other, just with a different SubType.
//
// itemID must be THIS item's step ID, pre-claimed by the caller (see
// runBatch's doc for why: it must be assigned deterministically by index
// on the single parent goroutine BEFORE any branch goroutines are
// spawned, not minted here via c.NextStepID() from inside each branch's
// own concurrently-running goroutine).
//
// Must be called with the calling goroutine already registered active
// (see execmgr.Manager.Register's doc) - runBatchItem itself neither
// registers nor deregisters; that is the caller's responsibility, since
// nested operations inside fn (Step, Wait, further child contexts, etc.)
// will themselves deregister/register around any blocking they do, using
// this same goroutine's registration.
//
// # nesting - FLAT mode (see NestingMode's own doc for the full feature)
//
// When nesting is NestingModeFlat, this item/branch's own
// CONTEXT_STARTED/CONTEXT_SUCCEEDED/CONTEXT_FAILED checkpoint triple is
// skipped entirely - fn runs in a "virtual" child context
// (dcontext.Context.NewVirtualChildWithName) whose own ParentID (and
// thus every operation fn itself checkpoints) is reportedParentID (the
// OUTER Map/Parallel context's own ParentStepID(), passed through
// unchanged by Map/Parallel below) rather than itemID - exactly matching
// the JS reference SDK's own confirmed virtualContext mechanism
// (run-in-child-context-handler.ts's executeChildContext). Replay-skip
// is also skipped for a virtual item/branch: with no CONTEXT operation
// ever checkpointed for it, there is nothing in the operation log to
// look up by itemID in the first place, so fn always genuinely re-runs
// on every replay pass through the batch, relying entirely on fn's OWN
// nested Step/Wait/etc. checkpoints (independently replay-safe under
// itemID's own still-genuine, still-unique step-ID PREFIX - see
// NewVirtualChildWithName's own doc: only the reported ParentID
// changes, itemID is still minted and still used as this item's own
// namespace root) to make re-running fn itself replay-safe, exactly the
// same "re-running fn is safe because ITS OWN nested operations are
// independently replay-safe" property RunInChildContext's own top-level
// doc already establishes for its ordinary crash-resume case.
func runBatchItem[TOut any](c *dcontext.Context, itemID, name string, subType string, index int, serdes types.Serdes, nesting NestingMode, reportedParentID string, fn func(child types.DurableContext) (TOut, error)) (TOut, error) {
	var zero TOut

	if nesting == NestingModeFlat {
		child := c.NewVirtualChildWithName(itemID, name, reportedParentID)
		dinfo := operationKindDispatchInfo{ID: itemID, ParentID: reportedParentID, Name: name, Type: types.OperationTypeContext, SubType: subType}
		dispatchOperationStart(c, dinfo, 0)
		wrapped, err := wrapChildContextFn(c, dinfo, func() (any, error) {
			return fn(child)
		})
		var result TOut
		if wrapped != nil {
			result, _ = wrapped.(TOut)
		}
		if err != nil {
			if errors.Is(err, errSuspended) {
				return zero, errSuspended
			}
			dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusFailed, "", err)
			return zero, &BatchItemFailedError{
				OperationError: &OperationError{Kind: OperationKindBatchItem, ID: itemID, Name: name, Err: err},
				Reconstructed:  false,
				Original:       err,
				Index:          index,
			}
		}
		dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusSucceeded, "", nil)
		return result, nil
	}

	if existing, found := c.ExecManager().GetOperation(itemID); found {
		// Replay-consistency check (docs/remaining-work.md §4 task 11):
		// verify the checkpointed operation at this step ID really is a
		// CONTEXT operation with the EXPECTED subType (MAP_ITERATION or
		// PARALLEL_BRANCH, matching this call's caller) before reading
		// ContextDetails off of it below - see invoke.go's identical
		// check on RunInChildContext for why the SubType half of this
		// comparison is what actually distinguishes Map/Parallel/
		// RunInChildContext's otherwise-identical CONTEXT operations
		// from each other.
		if err := checkReplayConsistency(existing, types.OperationTypeContext, subType, OperationKindBatchItem, itemID, name); err != nil {
			return zero, err
		}

		switch existing.Status {
		case types.OperationStatusSucceeded:
			return deserializeContextResult[TOut](serdes, existing, itemID)
		case types.OperationStatusFailed:
			return zero, batchItemError(existing, name, index)
		}
		// STARTED/PENDING: an interrupted attempt from a prior invocation.
		// Re-running fn from scratch is safe for the same reason
		// RunInChildContext's doc explains: fn's own nested operations are
		// independently replay-safe under this item's step-ID prefix.
	} else {
		if err := c.Checkpoint().Enqueue(types.OperationUpdate{
			ID:       itemID,
			ParentID: c.ParentStepID(),
			Type:     types.OperationTypeContext,
			Name:     name,
			Action:   types.OperationActionStart,
			SubType:  subType,
		}); err != nil {
			return zero, fmt.Errorf("%s %q (id %s): checkpointing start: %w", subType, name, itemID, err)
		}
		startInfo := operationKindDispatchInfo{ID: itemID, ParentID: c.ParentStepID(), Name: name, Type: types.OperationTypeContext, SubType: subType}
		dispatchOperationStart(c, startInfo, 0)
	}

	child := c.NewChildWithName(itemID, name)
	dinfo := operationKindDispatchInfo{ID: itemID, ParentID: c.ParentStepID(), Name: name, Type: types.OperationTypeContext, SubType: subType}
	wrapped, err := wrapChildContextFn(c, dinfo, func() (any, error) {
		return fn(child)
	})
	var result TOut
	if wrapped != nil {
		result, _ = wrapped.(TOut)
	}
	if err != nil {
		if errors.Is(err, errSuspended) {
			// fn itself suspended (e.g. a Step inside this branch/item
			// hit a non-zero retry delay - see step.go's retryOrFail)
			// rather than genuinely failing. Propagate the sentinel
			// as-is: checkpointing a FAIL here would be wrong (this
			// item hasn't actually failed, it's just paused) and would
			// corrupt replay by permanently marking it Failed for what
			// should resume as Pending on a later invocation. The
			// caller (runBatch's scheduler) treats this exactly like
			// any other branch that hasn't completed yet.
			return zero, errSuspended
		}
		if ckErr := c.Checkpoint().Enqueue(types.OperationUpdate{
			ID:       itemID,
			ParentID: c.ParentStepID(),
			Type:     types.OperationTypeContext,
			Name:     name,
			Action:   types.OperationActionFail,
			SubType:  subType,
			Error:    checkpointErrorObject(err),
		}); ckErr != nil {
			return zero, fmt.Errorf("%s %q (id %s): checkpointing failure: %w", subType, name, itemID, ckErr)
		}
		dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusFailed, "", err)
		return zero, &BatchItemFailedError{
			OperationError: &OperationError{Kind: OperationKindBatchItem, ID: itemID, Name: name, Err: err},
			Reconstructed:  false,
			Original:       err,
			Index:          index,
		}
	}

	serialized, serErr := serdes.Serialize(result, itemID, c.ExecutionARN())
	if serErr != nil {
		return zero, newSerdesError("serialize", itemID, name, serErr)
	}

	// docs/remaining-work.md §6 task 16: reject an oversized per-item
	// result before checkpointing it - see checkResultSize's doc in
	// errors.go. This is a per-ITEM check (each Map iteration/Parallel
	// branch's own result), distinct from the OUTER BatchResult's own
	// separate 256KB-summarization behavior (confirmed real via the
	// official Developer Guide's "Keep concurrency results small"
	// section - see finishBatch/deserializeBatchResult for that
	// mechanism, which this task's scope does not touch).
	if err := checkResultSize(serialized, OperationKindBatchItem, itemID, name); err != nil {
		return zero, err
	}

	if err := c.Checkpoint().Enqueue(types.OperationUpdate{
		ID:       itemID,
		ParentID: c.ParentStepID(),
		Type:     types.OperationTypeContext,
		Name:     name,
		Action:   types.OperationActionSucceed,
		SubType:  subType,
		Payload:  &serialized,
	}); err != nil {
		return zero, fmt.Errorf("%s %q (id %s): checkpointing success: %w", subType, name, itemID, err)
	}

	dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusSucceeded, serialized, nil)

	// Return the serdes ROUND-TRIP, not the raw in-memory value - see
	// roundTripSerialized's doc (the JS reference SDK's map/parallel
	// items execute through its child-context handler, whose own doc
	// comment guarantees every returned value "has always passed
	// through the serdes round-trip").
	return roundTripSerialized[TOut](serdes, serialized, itemID, name, c.ExecutionARN())
}

// batchItemError reconstructs an error for a checkpointed
// CONTEXT/MAP_ITERATION or CONTEXT/PARALLEL_BRANCH operation that ended
// Failed, on replay - see contextError's doc (invoke.go) for why this is
// necessarily "Reconstructed: true, Original: nil"; index is the item's/
// branch's position, threaded through so a caller inspecting one error
// out of a BatchFailedError's collected Errors can identify which item it
// came from.
func batchItemError(op types.Operation, name string, index int) error {
	if op.ContextDetails != nil && op.ContextDetails.Error != nil {
		return &BatchItemFailedError{
			OperationError: &OperationError{Kind: OperationKindBatchItem, ID: op.ID, Name: name, Err: fmt.Errorf("%s", op.ContextDetails.Error.ErrorMessage)},
			Reconstructed:  true,
			Index:          index,
		}
	}
	return &BatchItemFailedError{
		OperationError: &OperationError{Kind: OperationKindBatchItem, ID: op.ID, Name: name, Err: fmt.Errorf("failed with no recorded error")},
		Reconstructed:  true,
		Index:          index,
	}
}

// runBatch fans branches out across goroutines (bounded by maxConcurrency
// if set), evaluating cfg's early-exit policy as results come in, exactly
// matching the confirmed real-backend flowchart's shared Map/Parallel
// loop structure ("more items to process and error threshold not
// exceeded?" ... "did the overall operation succeed, not exceed error
// thresholds, and have minimum successful"). branches[i] is run via
// runItem(i, itemIDs[i]) and its ItemResult is placed at results[i],
// preserving input order regardless of completion order (needed for
// deterministic replay of the aggregated BatchResult).
//
// Returns ok=false if the execution suspended before every branch
// finished (see "Concurrency and suspension" below) - the caller must
// propagate this exactly like a single WaitForOperation's ok=false,
// rather than trying to interpret the partially-filled results slice.
//
// # Concurrency and suspension
//
// Each spawned goroutine registers itself active (execmgr.Register)
// before running its item/branch and deregisters when done, mirroring the
// top-level handler goroutine's own lifecycle - a blocking nested
// operation inside one branch (e.g. a Wait) can therefore suspend the
// whole execution exactly like it would at the top level, and other
// branches keep making progress independently on their own goroutines in
// the meantime. The calling (parent) goroutine's own wait for every
// branch to finish is coordinated via batchScheduler (below), not a raw
// sync.WaitGroup - see that type's doc for why.
//
// Three real, deterministically-reproducible (not just -race-flaky)
// concurrency bugs were found while implementing and testing this
// function, before arriving at the current batchScheduler design:
//
//  1. An early draft deregistered the parent unconditionally around a
//     plain sync.WaitGroup's Wait() call: if every branch finished and
//     deregistered at circa the same moment the parent's own deregister
//     call landed, active count could transiently hit zero and fire the
//     Suspended signal WHILE the parent was about to unblock from
//     Wait() on its own - a false suspension.
//  2. A second draft tried layering a separately-closed "done" channel
//     (closed by a dedicated goroutine once the WaitGroup's Wait()
//     returned) alongside execmgr's own Suspended() channel, selecting
//     between the two. This was STILL racy: the dedicated goroutine
//     needs the Go scheduler to actually wake it up before it can close
//     the done channel, and that scheduling latency is unbounded
//     relative to the SAME branch goroutine immediately continuing on
//     to its own Deregister() call - so Deregister() (and the resulting
//     Suspended() close) could fire before the dedicated goroutine even
//     got scheduled, letting the parent's select observe Suspended()
//     ready first even though the batch had, in fact, fully completed.
//     No amount of reordering operations WITHIN either goroutine can
//     close a race that fundamentally exists BETWEEN two independently-
//     scheduled synchronization primitives with no ordering relationship
//     to each other.
//  3. A THIRD bug, unrelated to the above two and found via
//     TestLocalTestRunner_Parallel_GenuineSuspension flaking (a genuine,
//     silent correctness bug that go test -race cannot detect on its
//     own, since every access to the shared counter inside
//     Context.NextStepID is itself already mutex-guarded): each
//     branch's step ID must be assigned DETERMINISTICALLY by its index
//     i, not by whichever branch goroutine happens to call
//     Context.NextStepID() first. An earlier draft's runItem closures
//     (see Map/Parallel) called c.NextStepID() indirectly via
//     runBatchItem, from INSIDE each branch's own concurrently-running
//     goroutine - so which branch got which step ID depended on
//     goroutine-scheduling order, not on i. On a fresh invocation this
//     is harmless (nothing has been checkpointed yet to replay against),
//     but on a REPLAY invocation the scheduling order can differ from a
//     prior invocation's, so branch i could be assigned a DIFFERENT step
//     ID than it was assigned last time - silently picking up some OTHER
//     branch's checkpointed operation (and thus that other branch's
//     already-SUCCEEDED result, or its still-STARTED callback) instead
//     of its own. This is what caused
//     TestLocalTestRunner_Parallel_GenuineSuspension's exact two
//     observed symptoms: sometimes the callback branch would swap step
//     IDs with the already-finished "fast" branch and immediately see a
//     SUCCEEDED checkpoint instead of blocking on its own still-STARTED
//     callback (Parallel wrongly reporting SUCCEEDED without ever
//     suspending), and sometimes - after a genuine suspension did occur
//     - the SAME swap on the next replay would make the callback branch
//     skip straight to "fast" instead of resuming its own await (a
//     "fast+fast" result instead of "fast+slow"). Fixed by claiming
//     every branch's step ID up front, in index order, on this single
//     parent goroutine BEFORE any branch goroutine is spawned - see
//     itemIDs below.
//
// The final design (batchScheduler, below) replaces both the WaitGroup
// and the separately-closed done channel from bugs #1/#2 with a single
// mutex guarding both the batch's own completion bookkeeping AND the
// decision of whether the parent goroutine should register/deregister
// with execmgr.Manager, so there is exactly one lock involved in every
// suspend-related decision instead of two independent, unordered
// signals - see batchScheduler's doc for the full reasoning, which also
// mirrors the officially-confirmed Java reference SDK's own design (per
// an internal SDK deep-dive talk, "Under the hood: Deep dive into a
// Lambda durable functions SDK": "there's a dedicated scheduler thread
// that manages everything behind the scene... the scheduler follows the
// same deregister and re-register pattern... if the scheduler is
// waiting for the children to complete... the scheduler deregisters
// itself. This handoff mechanism prevents a race condition where the
// active set briefly becomes empty and triggers a forced suspension").
//
// batchScheduler coordinates a Map/Parallel batch's concurrent
// items/branches using a single mutex (guarding both the batch's own
// completion bookkeeping AND the decision of whether the calling
// goroutine should register/deregister with execmgr.Manager). See
// runBatch's doc above for the three real concurrency bugs (two of them
// specifically about the risk of using independent synchronization
// signals for "batch done" vs. "should suspend") that led to this
// single-lock design.
//
// # Why a single shared lock, not two independent signals
//
// batchScheduler.mu is the ONLY lock involved in deciding "is the
// parent should-be-registered-with-execmgr," and every branch's
// completion bookkeeping happens under that SAME lock, so the parent's
// decision to deregister and a branch's decision to register on the
// parent's behalf (the "handoff") are always serialized relative to
// each other - there is no window where both could believe the other
// has already handled the accounting, or where neither has.
type batchScheduler struct {
	mu        sync.Mutex
	remaining int
	// parentDeregistered is true whenever the calling (parent)
	// goroutine is NOT currently registered with execmgr.Manager (i.e.
	// it deregistered before waiting) AND no branch has yet performed
	// the compensating hand-off registration on its behalf. Guarded by
	// mu so the decision "should I deregister" (checked by the parent)
	// and "should I hand off a registration to cover the parent"
	// (checked by the last-finishing branch) can never race.
	parentDeregistered bool
	done               bool
	doneCh             chan struct{}
}

func newBatchScheduler(n int) *batchScheduler {
	return &batchScheduler{remaining: n, doneCh: make(chan struct{})}
}

// branchFinished records one branch's completion. If this was the last
// outstanding branch AND the parent had previously deregistered itself
// (see waitForCompletion), branchFinished performs the compensating
// execmgr.Manager.Register() hand-off atomically under the SAME lock
// used to decide the parent had deregistered in the first place - this
// is the piece that actually closes the race (see batchScheduler's doc).
func (s *batchScheduler) branchFinished(c *dcontext.Context) {
	s.mu.Lock()
	s.remaining--
	last := s.remaining == 0
	if last {
		s.done = true
	}
	needsHandoff := last && s.parentDeregistered
	if needsHandoff {
		s.parentDeregistered = false
	}
	s.mu.Unlock()

	if needsHandoff {
		// Register on the parent's behalf BEFORE this branch's own
		// Deregister call (in the caller's defer chain) runs, so the
		// active count never actually needs to touch zero across this
		// handoff - this branch's own still-active registration and the
		// new one for the parent overlap for one instant instead of
		// there being a gap.
		c.ExecManager().Register()
	}
	if last {
		close(s.doneCh)
	}
}

// waitForCompletion blocks the calling (parent) goroutine until every
// branch has called branchFinished, correctly participating in
// execmgr.Manager's suspend-or-complete accounting the whole time (see
// batchScheduler's doc for why this must all happen under s.mu rather
// than via independent signals). Returns ok=false if the execution
// suspended before every branch finished.
func (s *batchScheduler) waitForCompletion(c *dcontext.Context) (ok bool) {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return true
	}
	s.parentDeregistered = true
	s.mu.Unlock()

	c.ExecManager().Deregister()

	select {
	case <-s.doneCh:
		// The branch that closed doneCh already performed the
		// compensating Register() hand-off (see branchFinished) BEFORE
		// closing doneCh, under the same lock that set
		// parentDeregistered - so by the time doneCh is observably
		// closed here, the hand-off (if parentDeregistered was still
		// true at that moment) has unconditionally already happened.
		// This goroutine now "owns" that hand-off registration.
		return true
	case <-c.ExecManager().Suspended():
		// Genuinely suspended: every branch that hasn't finished is
		// itself correctly deregistered (blocked on some external
		// operation), and no hand-off was pending to save us - do not
		// re-register, this goroutine is abandoned exactly like
		// WaitForOperation's own suspended path.
		return false
	}
}

func runBatch[TOut any](c *dcontext.Context, n int, maxConcurrency *int, runItem func(i int, itemID string) (TOut, error)) ([]ItemResult[TOut], bool) {
	results := make([]ItemResult[TOut], n)
	if n == 0 {
		return results, true
	}

	// Claim every branch/item's step ID up front, in index order, on
	// this single parent goroutine - BEFORE any branch goroutine is
	// spawned below. This must happen here, not inside each branch's own
	// goroutine (which is what the first draft did, via runBatchItem
	// calling c.NextStepID() itself) - see runBatch's doc, bug #3, for
	// why letting concurrently-running branch goroutines race to call
	// NextStepID() makes the step ID a branch receives depend on
	// goroutine-scheduling order instead of its index, which silently
	// corrupts replay (a branch can be handed a DIFFERENT step ID, and
	// thus another branch's checkpointed state, on a later invocation).
	itemIDs := make([]string, n)
	for i := 0; i < n; i++ {
		itemIDs[i] = c.NextStepID()
	}

	concurrency := n
	if maxConcurrency != nil && *maxConcurrency > 0 && *maxConcurrency < n {
		concurrency = *maxConcurrency
	}

	sem := make(chan struct{}, concurrency)
	scheduler := newBatchScheduler(n)

	for i := 0; i < n; i++ {
		sem <- struct{}{}
		c.ExecManager().Register()
		go func(i int) {
			value, err := runItem(i, itemIDs[i])

			if errors.Is(err, errSuspended) {
				// This branch itself suspended (e.g. a Step inside it
				// hit a non-zero retry delay - see step.go's
				// retryOrFail) rather than completing. Crucially,
				// whatever operation inside runItem returned
				// errSuspended has ALREADY called
				// execManager.Deregister() as part of its own
				// WaitForOperation call (see that function's doc: it
				// deregisters BEFORE blocking, and does not re-register
				// if suspension wins the race) - so this goroutine must
				// NOT deregister again (that would double-decrement the
				// active count) and must NOT report branchFinished
				// (this branch has NOT finished; it's abandoned exactly
				// like the top-level handler goroutine is abandoned on
				// suspension - see durable.go's WithDurableExecution
				// doc). Simply return, releasing only the concurrency
				// semaphore slot.
				results[i] = ItemResult[TOut]{Err: errSuspended}
				<-sem
				return
			}

			defer func() { <-sem }()
			defer c.ExecManager().Deregister()
			defer scheduler.branchFinished(c)

			results[i] = ItemResult[TOut]{Value: value, Err: err}
		}(i)
	}

	if !scheduler.waitForCompletion(c) {
		return nil, false
	}
	return results, true
}

// NestingMode controls whether Map/Parallel checkpoint a genuine
// CONTEXT operation (CONTEXT_STARTED/CONTEXT_SUCCEEDED, SubType
// MAP_ITERATION/PARALLEL_BRANCH) for each item/branch, or run it in a
// "virtual" context that skips that pair of checkpoints entirely -
// matching the JS reference SDK's own real, documented, public
// NestingType enum (packages/aws-durable-execution-sdk-js/src/types/
// batch.ts) exactly, including its own ~30% operation-count reduction
// claim for NestingModeFlat (fewer checkpoints consumed per batch,
// higher achievable item/branch counts within the backend's own
// per-execution operation-count limits).
type NestingMode int

const (
	// NestingModeNested (the default - the zero value, and what every
	// existing Map/Parallel call already does before this option
	// existed) checkpoints a full CONTEXT_STARTED/CONTEXT_SUCCEEDED (or
	// _FAILED) pair for every item/branch, exactly as this SDK already
	// behaved before FLAT nesting was added - each item/branch appears
	// as its own, independently inspectable operation in the execution
	// history.
	NestingModeNested NestingMode = iota

	// NestingModeFlat runs every item/branch in a "virtual" context:
	// no CONTEXT_STARTED/CONTEXT_SUCCEEDED/CONTEXT_FAILED checkpoint is
	// ever sent for the item/branch itself, and every operation fn
	// checkpoints internally (Steps, nested child contexts, etc.)
	// reports the OUTER Map/Parallel context's own ParentID directly,
	// as if it had been checkpointed one level shallower than usual -
	// exactly matching the confirmed real mechanism in the JS reference
	// SDK's own run-in-child-context-handler.ts (virtualContext: true),
	// which this Go SDK's own dcontext.Context.NewVirtualChildWithName
	// and runBatchItem's own nestingMode parameter port directly (see
	// both of those for the low-level mechanics).
	NestingModeFlat
)

// MapOption configures a Map invocation.
type MapOption[TIn, TOut any] func(*mapConfig[TIn, TOut])

type mapConfig[TIn, TOut any] struct {
	maxConcurrency   *int
	completionConfig types.CompletionConfig
	serdes           types.Serdes
	resultSerdes     types.Serdes
	itemNamer        func(item TIn, index int) string
	nesting          NestingMode
}

// WithMapMaxConcurrency caps the number of concurrently in-flight
// iterations. If unset, all items are started concurrently at once.
func WithMapMaxConcurrency[TIn, TOut any](n int) MapOption[TIn, TOut] {
	return func(c *mapConfig[TIn, TOut]) { c.maxConcurrency = &n }
}

// WithMapCompletionConfig sets early-exit policy for the batch (minimum
// successes required, tolerated failure count/percentage). With no
// CompletionConfig at all, Map defaults to Promise.all-style semantics:
// any single item's failure fails the whole batch.
func WithMapCompletionConfig[TIn, TOut any](cfg types.CompletionConfig) MapOption[TIn, TOut] {
	return func(c *mapConfig[TIn, TOut]) { c.completionConfig = cfg }
}

// WithMapSerdes sets a custom serializer for each iteration's result.
func WithMapSerdes[TIn, TOut any](s types.Serdes) MapOption[TIn, TOut] {
	return func(c *mapConfig[TIn, TOut]) { c.serdes = s }
}

// WithMapResultSerdes sets a custom serializer for the OUTER Map
// context's own ContextSucceeded payload - a DISTINCT, whole-batch-level
// serializer, separate from WithMapSerdes's own per-item serializer.
//
// Confirmed against the JS reference SDK's own real, distinct
// MapConfig.serdes field (packages/aws-durable-execution-sdk-js/src/
// types/batch.ts: "Serialization/deserialization configuration for
// parent context", separate from itemSerdes's own "for each item" doc)
// and its own mechanism (map-handler.ts forwards config.serdes straight
// into executeConcurrently -> runInChildContext's OWN options.serdes -
// the exact same generic per-context serdes hook
// RunInChildContext/WithChildSerdes already implements one level down,
// not a Map/Parallel-specific mechanism at all). Ported here the same
// way: when set, this serdes's own Serialize/Deserialize REPLACES
// finishBatch/deserializeBatchResult's own default internal JSON wire
// encoding (batchResultWire[TOut]) for the outer context's payload only
// - per-item payloads (WithMapSerdes) and the outer CONTEXT/START,
// per-item CONTEXT/START/SUCCEED/FAIL checkpoints are entirely
// unaffected.
//
// Because a genuinely custom serdes is free to discard information (the
// conformance suite's own canonical example - test-requirements/map/
// 9-19.yaml - encodes only the ordered result values as "OPSERDE:X,Y",
// with no room for CompletionReason or per-item errors at all), the
// caller's own Deserialize is the SOLE source of truth for reconstructing
// a BatchResult[TOut] on replay: it must return either a BatchResult[TOut]
// value directly, or a []TOut of item values (which this SDK wraps into
// a synthetic ALL_COMPLETED BatchResult[TOut], sufficient for GetResults()
// but with no reconstructed per-item error/CompletionReason detail) - see
// deserializeBatchResult's own doc for the exact accepted shapes and the
// error returned for anything else.
func WithMapResultSerdes[TIn, TOut any](s types.Serdes) MapOption[TIn, TOut] {
	return func(c *mapConfig[TIn, TOut]) { c.resultSerdes = s }
}

// WithMapItemNamer overrides the default per-iteration checkpoint name
// (itemName's own "batchName[index]" default) with a caller-supplied
// function of the item value and its index - affects observability (the
// MAP_ITERATION operation's own Name) only, never replay determinism or
// results (each iteration's step-ID prefix, used for actual replay-skip
// matching, is always the index-based itemID regardless of this option).
//
// Confirmed against the JS reference SDK's own real, distinct
// MapConfig.itemNamer field (types/batch.ts: "Function to generate
// custom names for map items") and its own mechanism (map-handler.ts:
// `config?.itemNamer ? config.itemNamer(item, index) : undefined`,
// feeding straight into each iteration's own ConcurrentExecutionItem.name
// - simple enough to port directly with no other change needed).
// ParallelConfig has no analogous field in the JS reference SDK either
// (branches have no natural "item value" to name from - NamedParallelBranch's
// own optional per-branch name is a different, not-yet-requested
// mechanism), matching this option's own Map-only scope.
func WithMapItemNamer[TIn, TOut any](namer func(item TIn, index int) string) MapOption[TIn, TOut] {
	return func(c *mapConfig[TIn, TOut]) { c.itemNamer = namer }
}

// WithMapNesting sets Map's NestingMode - see that type's own doc for
// the full FLAT-vs-NESTED tradeoff. Defaults to NestingModeNested
// (unchanged, pre-existing behavior) if this option is never supplied.
func WithMapNesting[TIn, TOut any](mode NestingMode) MapOption[TIn, TOut] {
	return func(c *mapConfig[TIn, TOut]) { c.nesting = mode }
}

// Map applies fn to each item in items concurrently, each within its own
// isolated child context (see RunInChildContext), with optional
// concurrency control (WithMapMaxConcurrency) and early-exit policy
// (WithMapCompletionConfig). Per-item errors are isolated: a failure in
// one item does not itself stop other already-running items, though it
// counts toward the configured failure thresholds (see
// WithMapCompletionConfig).
//
// # Implementation notes
//
// Mirrors the confirmed real-backend flowchart exactly (internal SDK
// Operation Diagrams design doc, "Map"): an outer CONTEXT/MAP operation
// wrapping the whole call (checkpointed START once at the top,
// SUCCEED/FAIL once at the bottom, exactly like RunInChildContext's own
// CONTEXT/RUN_IN_CHILD_CONTEXT), containing N inner
// CONTEXT/MAP_ITERATION operations - one per item, each with the exact
// same single-item lifecycle as RunInChildContext (see runBatchItem) -
// run concurrently across goroutines (see runBatch). The overall
// MAP/SUCCEED vs MAP/FAIL decision, and whether the loop keeps starting
// further iterations, are both governed by the same CompletionConfig
// thresholds (see batchCompletion), matching the flowchart's "error
// threshold not exceeded?" loop guard and final "succeeded, not exceed
// error thresholds, and have minimum successful iterations?" decision
// diamond, which are the same three-threshold check applied at two
// different points.
//
// On replay, if the outer CONTEXT/MAP operation already reached a
// terminal status, its checkpointed BatchResult is returned directly
// without re-running anything (the outer replay-skip). Otherwise Map
// always re-runs the full item loop; each item's own CONTEXT/MAP_ITERATION
// checkpoint independently replay-skips before fn is invoked, exactly as
// RunInChildContext's doc describes for its single child context - so
// re-driving the whole loop on every replay is safe and does not
// re-execute already-completed items' side effects.
func Map[TIn, TOut any](dc types.DurableContext, id string, items []TIn, fn func(child types.DurableContext, item TIn, index int) (TOut, error), opts ...MapOption[TIn, TOut]) (BatchResult[TOut], error) {
	cfg := &mapConfig[TIn, TOut]{serdes: utils.DefaultSerdes(), resultSerdes: utils.DefaultSerdes()}
	for _, opt := range opts {
		opt(cfg)
	}

	var zero BatchResult[TOut]
	c, ok := dc.(*dcontext.Context)
	if !ok {
		return zero, fmt.Errorf("operations.Map: dc must be created by this SDK's runtime (got %T)", dc)
	}
	if err := validateBatchConfig("Map", cfg.maxConcurrency, cfg.completionConfig); err != nil {
		return zero, err
	}

	mapID := c.NextStepID()

	if existing, found := c.ExecManager().GetOperation(mapID); found {
		// Replay-consistency check (docs/remaining-work.md §4 task 11):
		// verify the checkpointed operation at this step ID really is a
		// CONTEXT/MAP operation before reading ContextDetails off of it.
		if err := checkReplayConsistency(existing, types.OperationTypeContext, subTypeMap, OperationKindBatch, mapID, id); err != nil {
			return zero, err
		}

		switch existing.Status {
		case types.OperationStatusSucceeded:
			return deserializeBatchResult[TOut](cfg.resultSerdes, existing, mapID)
		case types.OperationStatusFailed:
			return zero, contextError(existing, id, OperationKindBatch)
		}
	} else {
		if err := c.Checkpoint().Enqueue(types.OperationUpdate{
			ID:       mapID,
			ParentID: c.ParentStepID(),
			Type:     types.OperationTypeContext,
			Name:     id,
			Action:   types.OperationActionStart,
			SubType:  subTypeMap,
		}); err != nil {
			return zero, fmt.Errorf("map %q (id %s): checkpointing start: %w", id, mapID, err)
		}
	}

	mapCtx := c.NewChildWithName(mapID, id)

	completion := batchCompletion{cfg: cfg.completionConfig, total: len(items)}
	var succeededCount, failedCount int32Counter

	itemResults, allCompleted := runBatch[TOut](mapCtx, len(items), cfg.maxConcurrency, func(i int, itemID string) (TOut, error) {
		if completion.thresholdExceeded(succeededCount.snapshot(), failedCount.snapshot()) {
			var z TOut
			return z, errBatchSkipped
		}
		val, err := runBatchItem[TOut](mapCtx, itemID, mapItemName(cfg.itemNamer, id, items[i], i), subTypeMapIteration, i, cfg.serdes, cfg.nesting, mapCtx.ParentStepID(), func(child types.DurableContext) (TOut, error) {
			return fn(child, items[i], i)
		})
		if err != nil {
			failedCount.increment()
		} else {
			succeededCount.increment()
		}
		return val, err
	})
	if !allCompleted {
		return zero, errSuspended
	}

	return finishBatch(c, mapID, subTypeMap, id, cfg.resultSerdes, completion, itemResults)
}

// mapItemName resolves a single Map iteration's own checkpoint name:
// the caller-supplied WithMapItemNamer function if one was set (see that
// option's own doc), otherwise the shared itemName default also used by
// Parallel.
func mapItemName[TIn any](namer func(item TIn, index int) string, batchName string, item TIn, index int) string {
	if namer != nil {
		return namer(item, index)
	}
	return itemName(batchName, index)
}

func itemName(batchName string, index int) string {
	return fmt.Sprintf("%s[%d]", batchName, index)
}

// ParallelOption configures a Parallel invocation.
type ParallelOption[TOut any] func(*parallelConfig[TOut])

type parallelConfig[TOut any] struct {
	maxConcurrency   *int
	completionConfig types.CompletionConfig
	serdes           types.Serdes
	resultSerdes     types.Serdes
	nesting          NestingMode
}

// WithParallelMaxConcurrency caps the number of concurrently in-flight
// branches. If unset, all branches are started concurrently at once.
func WithParallelMaxConcurrency[TOut any](n int) ParallelOption[TOut] {
	return func(c *parallelConfig[TOut]) { c.maxConcurrency = &n }
}

// WithParallelCompletionConfig sets early-exit policy for the batch. With
// no CompletionConfig at all, Parallel defaults to Promise.all-style
// semantics: any single branch's failure fails the whole batch.
func WithParallelCompletionConfig[TOut any](cfg types.CompletionConfig) ParallelOption[TOut] {
	return func(c *parallelConfig[TOut]) { c.completionConfig = cfg }
}

// WithParallelSerdes sets a custom serializer for each branch's result.
func WithParallelSerdes[TOut any](s types.Serdes) ParallelOption[TOut] {
	return func(c *parallelConfig[TOut]) { c.serdes = s }
}

// WithParallelResultSerdes sets a custom serializer for the OUTER
// Parallel context's own ContextSucceeded payload - a DISTINCT,
// whole-batch-level serializer, separate from WithParallelSerdes's own
// per-branch serializer. See WithMapResultSerdes's own doc (Map's
// analogous option) for the full JS-reference-SDK mechanism and evidence
// this is built on - Parallel's own ParallelConfig.serdes field in the
// JS reference SDK is the identical mechanism, one level up.
func WithParallelResultSerdes[TOut any](s types.Serdes) ParallelOption[TOut] {
	return func(c *parallelConfig[TOut]) { c.resultSerdes = s }
}

// WithParallelNesting sets Parallel's NestingMode - see that type's own
// doc for the full FLAT-vs-NESTED tradeoff. Defaults to
// NestingModeNested (unchanged, pre-existing behavior) if this option is
// never supplied.
func WithParallelNesting[TOut any](mode NestingMode) ParallelOption[TOut] {
	return func(c *parallelConfig[TOut]) { c.nesting = mode }
}

// Parallel runs each of branches concurrently, each within its own
// isolated child context, with optional concurrency control
// (WithParallelMaxConcurrency) and early-exit policy
// (WithParallelCompletionConfig). See Map's doc for the shared
// implementation notes (Parallel is structurally identical, differing
// only in SubType: PARALLEL/PARALLEL_BRANCH instead of
// MAP/MAP_ITERATION, per the confirmed real-backend flowchart) and All /
// AllSettled / Any / Race below for the common Promise-combinator
// wrappers built on top of it.
func Parallel[TOut any](dc types.DurableContext, id string, branches []func(child types.DurableContext) (TOut, error), opts ...ParallelOption[TOut]) (BatchResult[TOut], error) {
	cfg := &parallelConfig[TOut]{serdes: utils.DefaultSerdes(), resultSerdes: utils.DefaultSerdes()}
	for _, opt := range opts {
		opt(cfg)
	}

	var zero BatchResult[TOut]
	c, ok := dc.(*dcontext.Context)
	if !ok {
		return zero, fmt.Errorf("operations.Parallel: dc must be created by this SDK's runtime (got %T)", dc)
	}
	if err := validateBatchConfig("Parallel", cfg.maxConcurrency, cfg.completionConfig); err != nil {
		return zero, err
	}

	parallelID := c.NextStepID()

	if existing, found := c.ExecManager().GetOperation(parallelID); found {
		// Replay-consistency check (docs/remaining-work.md §4 task 11):
		// verify the checkpointed operation at this step ID really is a
		// CONTEXT/PARALLEL operation before reading ContextDetails off of
		// it.
		if err := checkReplayConsistency(existing, types.OperationTypeContext, subTypeParallel, OperationKindBatch, parallelID, id); err != nil {
			return zero, err
		}

		switch existing.Status {
		case types.OperationStatusSucceeded:
			return deserializeBatchResult[TOut](cfg.resultSerdes, existing, parallelID)
		case types.OperationStatusFailed:
			return zero, contextError(existing, id, OperationKindBatch)
		}
	} else {
		if err := c.Checkpoint().Enqueue(types.OperationUpdate{
			ID:       parallelID,
			ParentID: c.ParentStepID(),
			Type:     types.OperationTypeContext,
			Name:     id,
			Action:   types.OperationActionStart,
			SubType:  subTypeParallel,
		}); err != nil {
			return zero, fmt.Errorf("parallel %q (id %s): checkpointing start: %w", id, parallelID, err)
		}
	}

	parallelCtx := c.NewChildWithName(parallelID, id)

	completion := batchCompletion{cfg: cfg.completionConfig, total: len(branches)}
	var succeededCount, failedCount int32Counter

	branchResults, allCompleted := runBatch[TOut](parallelCtx, len(branches), cfg.maxConcurrency, func(i int, itemID string) (TOut, error) {
		if completion.thresholdExceeded(succeededCount.snapshot(), failedCount.snapshot()) {
			var z TOut
			return z, errBatchSkipped
		}
		val, err := runBatchItem[TOut](parallelCtx, itemID, itemName(id, i), subTypeParallelBranch, i, cfg.serdes, cfg.nesting, parallelCtx.ParentStepID(), branches[i])
		if err != nil {
			failedCount.increment()
		} else {
			succeededCount.increment()
		}
		return val, err
	})
	if !allCompleted {
		return zero, errSuspended
	}

	return finishBatch(c, parallelID, subTypeParallel, id, cfg.resultSerdes, completion, branchResults)
}

// finishBatch checkpoints the outer MAP/PARALLEL operation's terminal
// SUCCEED, matching the flowchart's final decision diamond, and returns
// the assembled BatchResult (nil error, ALWAYS) to the caller.
//
// # Bug fixed here (real, breaking change): the outer checkpoint is now
// # ALWAYS ContextSucceeded, and this function never returns an error
//
// This function used to checkpoint ContextFailed and return a
// *BatchFailedError whenever completion.overallSucceeded(succeeded,
// failed) was false (i.e. whenever the batch's own CompletionConfig
// policy was not met) - but the real, confirmed cross-SDK contract
// (verified directly against the JS reference SDK's own actual
// interface, packages/aws-durable-execution-sdk-js/src/types/batch.ts's
// BatchResult<TResult>, and its own handlers/concurrent-execution-
// handler.ts, which never throws/rejects for a policy-not-met batch) is
// that "did the batch satisfy its policy" is a property CARRIED BY the
// returned BatchResult (Status/CompletionReason/HasFailure), not an
// execution-level failure - the outer CONTEXT/MAP or CONTEXT/PARALLEL
// operation's own checkpoint is ALWAYS ContextSucceeded once the
// item/branch loop finishes, for ANY reason (every item ran, or an
// early-exit threshold was hit).
//
// Confirmed as a real, previously-mismatched behavior via 12 failing
// conformance requirements (Parallel suite's 8-6/7/8/10/13/17/18, Map
// suite's 9-5/6/7/9/10), every one of whose own ExpectedExecutionHistory
// YAML asserts the outer Parallel/Map context's ContextSucceeded
// checkpoint even when the completion policy was NOT met (e.g. 8-6's
// own fail-fast, tolerated-failure-count=0 scenario) - several of the
// corresponding conformance handlers had to hand-roll this exact
// workaround (catching the old *BatchFailedError and manually
// reconstructing a completionReason/status/successCount/failureCount/
// totalCount projection from its embedded *AggregateError - see e.g.
// conformance/handlers/parallel_8_6.go, before this fix) precisely
// because this function's own old contract made that the only way to
// get at this information at all. See BatchResult's own top-level doc
// and ThrowIfError's own doc for the full API surface this fix adds and
// the CALLER-facing method that restores the old
// "throw/return-an-error-for-a-policy-violation" behavior for any
// caller that still wants it (e.g. All/Any's own updated implementation
// below, which deliberately DOES still want that Promise.all/any-style
// behavior for THEIR OWN, separate, unchanged public contracts).
//
// # Bug PREVIOUSLY fixed here (still true, unrelated, retained
// # unchanged): the outer BatchResult must NOT be serialized with the
// # caller's per-item serdes
//
// finishBatch used to accept the SAME types.Serdes passed to
// WithMapSerdes/WithParallelSerdes (cfg.serdes) and call
// serdes.Serialize(batchResult, ...) with it directly - but that serdes
// is documented (see WithParallelSerdes's own doc: "sets a custom
// serializer for EACH BRANCH'S result") and, by every existing caller's
// own reasonable implementation, written to (de)serialize a single TOut
// item value, not a BatchResult[TOut] wrapper struct. Fixed (in an
// earlier session) by always marshaling the outer BatchResult with
// plain encoding/json (via its own MarshalJSON) - unaffected by this
// session's own further change, still correct, unchanged here.
//
// # Feature added here: resultSerdes (WithMapResultSerdes/
// # WithParallelResultSerdes) - a DISTINCT, genuinely-caller-facing
// # whole-result serializer, separate from the per-item serdes bug fix
// # directly above
//
// resultSerdes is utils.DefaultSerdes() (a plain JSONSerdes) unless the
// caller explicitly set WithMapResultSerdes/WithParallelResultSerdes -
// see those options' own doc for the full JS-reference-SDK mechanism
// this ports. finishBatch distinguishes the two cases by TYPE, not by a
// separate boolean flag: when resultSerdes is exactly a
// utils.JSONSerdes (the untouched default), the pre-existing, unchanged
// batchResultWire[TOut] internal encoding is used (byte-for-byte
// identical to this function's own pre-existing behavior, preserving
// full CompletionReason/per-item-error fidelity on replay for every
// existing caller who never touches this new option at all); otherwise
// the caller's OWN Serialize is called directly on the assembled
// BatchResult[TOut] value, and its own string output becomes the
// checkpointed payload verbatim - exactly matching the conformance
// suite's own canonical example (test-requirements/map/9-19.yaml: the
// Map ContextSucceeded payload IS the custom serializer's own literal
// "OPSERDE:X,Y" output, not a JSON-wrapped envelope around it).
func finishBatch[TOut any](c *dcontext.Context, outerID, subType, name string, resultSerdes types.Serdes, completion batchCompletion, itemResults []ItemResult[TOut]) (BatchResult[TOut], error) {
	succeeded := 0
	failed := 0
	skipped := 0
	for _, item := range itemResults {
		switch {
		case item.Err == errBatchSkipped:
			skipped++
		case item.Err != nil:
			failed++
		default:
			succeeded++
		}
	}

	batchResult := BatchResult[TOut]{
		Items:            itemResults,
		CompletionReason: completion.completionReason(succeeded, failed, skipped),
		batchID:          outerID,
		batchName:        name,
	}

	var serialized string
	if _, isDefault := resultSerdes.(utils.JSONSerdes); isDefault || resultSerdes == nil {
		serializedBytes, serErr := batchResult.MarshalJSON()
		if serErr != nil {
			return BatchResult[TOut]{}, newSerdesError("serialize", outerID, name, serErr)
		}
		serialized = string(serializedBytes)
	} else {
		s, serErr := resultSerdes.Serialize(batchResult, outerID, c.ExecutionARN())
		if serErr != nil {
			return BatchResult[TOut]{}, newSerdesError("serialize", outerID, name, serErr)
		}
		serialized = s
	}
	if err := c.Checkpoint().Enqueue(types.OperationUpdate{
		ID:       outerID,
		ParentID: c.ParentStepID(),
		Type:     types.OperationTypeContext,
		Name:     name,
		Action:   types.OperationActionSucceed,
		SubType:  subType,
		Payload:  &serialized,
	}); err != nil {
		return BatchResult[TOut]{}, fmt.Errorf("%s %q (id %s): checkpointing success: %w", subType, name, outerID, err)
	}
	return batchResult, nil
}

// errBatchSkipped marks an item/branch that was never actually run
// because the failure threshold was already exceeded before its turn
// came up (only reachable when WithMapMaxConcurrency/
// WithParallelMaxConcurrency bounds concurrency below the batch size;
// with unbounded concurrency - the default - every item/branch is
// already running before any threshold could trip, matching how
// Promise-based fan-out in the reference SDKs can't truly "cancel"
// in-flight promises either). Excluded from AggregateError's collected
// errors since it is not a real failure of that item's work, just a
// bookkeeping placeholder.
var errBatchSkipped = fmt.Errorf("operations: skipped, batch failure threshold already exceeded")

// deserializeBatchResult deserializes a checkpointed MAP/PARALLEL
// operation's result back into a BatchResult[TOut]. BatchResult isn't a
// primitive JSON-friendly shape by default (it embeds per-item error
// values, which don't round-trip through JSON as Go errors), so with the
// default resultSerdes (utils.JSONSerdes, unless WithMapResultSerdes/
// WithParallelResultSerdes was set - see finishBatch's own doc for the
// identical by-type distinction on the serialize side) the checkpointed
// payload stores batchResultWire[TOut] - see that type's doc - and is
// decoded directly with encoding/json, unchanged from this function's
// pre-existing behavior.
//
// With a genuinely custom resultSerdes, this calls the caller's own
// Deserialize on the raw checkpointed payload and accepts exactly two
// return shapes (returning a descriptive error for anything else): a
// BatchResult[TOut] value directly (full fidelity - CompletionReason and
// per-item errors are preserved exactly as the caller's own Deserialize
// reconstructs them), or a []TOut (a bare slice of successful values,
// which this SDK wraps into a synthetic CompletionReasonAllCompleted
// BatchResult[TOut] with no per-item error detail - sufficient for
// GetResults()/TotalCount(), matching the conformance suite's own
// canonical example, whose custom deserializer only ever reconstructs
// the ordered value list, never per-item failure detail).
func deserializeBatchResult[TOut any](resultSerdes types.Serdes, op types.Operation, id string) (BatchResult[TOut], error) {
	var zero BatchResult[TOut]
	if op.ContextDetails == nil || op.ContextDetails.Result == nil {
		return zero, fmt.Errorf("batch %s: succeeded but no result recorded", id)
	}

	if _, isDefault := resultSerdes.(utils.JSONSerdes); isDefault || resultSerdes == nil {
		var wire batchResultWire[TOut]
		if err := json.Unmarshal([]byte(*op.ContextDetails.Result), &wire); err != nil {
			return zero, newSerdesError("deserialize", id, op.Name, err)
		}
		items := make([]ItemResult[TOut], len(wire.Items))
		for i, w := range wire.Items {
			items[i] = ItemResult[TOut]{Value: w.Value}
			if w.Err != "" {
				items[i].Err = fmt.Errorf("%s", w.Err)
			}
		}
		return BatchResult[TOut]{
			Items:            items,
			CompletionReason: wire.CompletionReason,
			batchID:          id,
			batchName:        op.Name,
		}, nil
	}

	decoded, deErr := resultSerdes.Deserialize(*op.ContextDetails.Result, id, "")
	if deErr != nil {
		return zero, newSerdesError("deserialize", id, op.Name, deErr)
	}
	switch v := decoded.(type) {
	case BatchResult[TOut]:
		v.batchID = id
		v.batchName = op.Name
		return v, nil
	case []TOut:
		items := make([]ItemResult[TOut], len(v))
		for i, val := range v {
			items[i] = ItemResult[TOut]{Value: val}
		}
		return BatchResult[TOut]{
			Items:            items,
			CompletionReason: CompletionReasonAllCompleted,
			batchID:          id,
			batchName:        op.Name,
		}, nil
	default:
		return zero, fmt.Errorf("batch %s: custom result serdes Deserialize returned %T, expected BatchResult[T] or []T", id, decoded)
	}
}

// itemResultWire is a single item/branch's on-the-wire encoding within
// batchResultWire: ItemResult[T]'s Err field is a Go error interface,
// which encoding/json cannot (de)serialize directly, so it's flattened
// to a plain string here (empty = no error) for checkpointing and
// restored to a generic error on deserialization - checkpointed batch
// results are only ever consulted for their success/failure shape and
// values on replay, never type-asserted back to a specific error type,
// so this lossy round-trip (loses the original error's concrete type)
// matches every other operation's error-checkpointing behavior in this
// SDK (see types.ErrorObject, which is similarly just a message + type
// string).
type itemResultWire[T any] struct {
	Value T      `json:"Value"`
	Err   string `json:"Err,omitempty"`
}

// batchResultWire is BatchResult's own outer checkpoint wire encoding -
// an object wrapping Items PLUS CompletionReason (added alongside the
// completion-policy-contract fix; the wire shape used to be a bare JSON
// array of itemResultWire, with no room for any sibling field - changed
// to an object specifically to make room for this, and any future
// BatchResult field, without another breaking wire change). This is an
// internal checkpoint format for this SDK's own use, not a public-facing
// schema - a live EXECUTION never crosses this schema boundary mid-flight
// in practice (every deployed function using it is redeployed as a whole
// unit, not upgraded under a live, already-suspended execution), so this
// breaking wire change is acceptable.
type batchResultWire[T any] struct {
	Items            []itemResultWire[T] `json:"Items"`
	CompletionReason string              `json:"CompletionReason,omitempty"`
}

// MarshalJSON implements BatchResult's checkpoint wire encoding (see
// batchResultWire's doc).
func (r BatchResult[T]) MarshalJSON() ([]byte, error) {
	wire := batchResultWire[T]{
		Items:            make([]itemResultWire[T], len(r.Items)),
		CompletionReason: r.CompletionReason,
	}
	for i, item := range r.Items {
		wire.Items[i] = itemResultWire[T]{Value: item.Value}
		if item.Err != nil {
			wire.Items[i].Err = item.Err.Error()
		}
	}
	return json.Marshal(wire)
}

// int32Counter is a tiny atomic-ish counter guarded by a mutex, used to
// track failures-so-far across concurrently running goroutines for the
// completion-threshold check. Not using sync/atomic directly to keep the
// read-then-compare in evaluateThreshold trivially race-safe without
// needing a CAS loop - contention here is bounded by maxConcurrency, not
// worth optimizing further.
type int32Counter struct {
	mu sync.Mutex
	n  int
}

func (c *int32Counter) increment() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

func (c *int32Counter) snapshot() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// AggregateError collects one error per failed branch/item in a batch
// operation, returned by Map/Parallel (when the batch overall fails) and
// by All/Any when the corresponding condition is not met.
type AggregateError struct {
	Errors []error
}

// Unwrap exposes the collected Errors slice to errors.Is/errors.As via
// the multi-error Unwrap() []error convention (supported since Go 1.20),
// so a caller can errors.As straight through an AggregateError (and, one
// level further out, a *BatchFailedError - see that type's own Unwrap)
// to find a specific *BatchItemFailedError/*StepFailedError/etc. nested
// inside without manually looping over Errors themselves. Added
// alongside docs/remaining-work.md §4 task 10 - AggregateError predates
// this task and had no Unwrap before, since nothing needed to look
// THROUGH it programmatically until BatchFailedError's own errors.As
// support required it.
func (e *AggregateError) Unwrap() []error { return e.Errors }

func (e *AggregateError) Error() string {
	if len(e.Errors) == 0 {
		return "aggregate error: no underlying errors"
	}
	msg := "aggregate error: "
	for i, err := range e.Errors {
		if i > 0 {
			msg += "; "
		}
		msg += err.Error()
	}
	return msg
}

// All runs branches via Parallel and returns the successful results only
// if every branch succeeds; otherwise it returns an *AggregateError
// collecting all branch errors. Mirrors Promise.all semantics.
//
// # Updated for the completion-policy-contract fix
//
// Parallel itself no longer returns an error for a policy-not-met batch
// (see BatchResult's own top-level doc and finishBatch's doc for the
// full writeup) - it always returns a valid BatchResult with a nil
// error once its branches finish. All's OWN public contract
// ("otherwise it returns an *AggregateError") is UNCHANGED - it simply
// now has to ask for that behavior explicitly via result.ThrowIfError(),
// rather than getting it automatically from Parallel's own old return
// shape. A genuine, unrelated error from Parallel itself (config
// validation, dc type assertion, a replay-consistency mismatch, or
// propagated suspension) still returns directly, exactly as before.
func All[TOut any](dc types.DurableContext, id string, branches []func(types.DurableContext) (TOut, error), opts ...ParallelOption[TOut]) ([]TOut, error) {
	result, err := Parallel(dc, id, branches, opts...)
	if err != nil {
		return nil, err
	}
	if throwErr := result.ThrowIfError(); throwErr != nil {
		// See Any's own identical unwrap-to-*AggregateError comment
		// below for why errors.As (not a direct type assertion) is used
		// here - ThrowIfError returns a *BatchFailedError wrapping an
		// *AggregateError, and All's own documented contract
		// ("*AggregateError collecting all branch errors") predates and
		// is unaffected by that wrapping.
		var aggErr *AggregateError
		if errors.As(throwErr, &aggErr) {
			return nil, aggErr
		}
		return nil, throwErr
	}

	out := make([]TOut, 0, len(result.Items))
	for _, item := range result.Items {
		out = append(out, item.Value)
	}
	return out, nil
}

// AllSettled runs branches via Parallel and returns the outcome of every
// branch regardless of success or failure - it never itself returns an
// error for individual branch failures (unlike Parallel's default
// all-must-succeed policy), matching Promise.allSettled semantics
// exactly.
//
// Unaffected by the completion-policy-contract fix (see BatchResult's
// own top-level doc): AllSettled configures MinSuccessful: 0, which by
// definition can never itself be "not met" (zero successes always
// satisfies a minimum of zero) - Parallel's own return here was already
// only ever a genuine, unrelated error (config validation, suspension,
// etc.), never a policy-driven one, both before and after this fix.
func AllSettled[TOut any](dc types.DurableContext, id string, branches []func(types.DurableContext) (TOut, error), opts ...ParallelOption[TOut]) (BatchResult[TOut], error) {
	minZero := 0
	opts = append([]ParallelOption[TOut]{WithParallelCompletionConfig[TOut](types.CompletionConfig{MinSuccessful: &minZero})}, opts...)
	return Parallel(dc, id, branches, opts...)
}

// Any runs branches via Parallel and returns the first successful result
// in input order (Go's synchronous return can't express true
// first-to-complete ordering the way Promise.any does without an
// additional non-deterministic checkpoint, which would break replay
// determinism - see the doc note below). If every branch fails, it
// returns an *AggregateError. Mirrors Promise.any semantics, with that one
// caveat.
//
// # Determinism note
//
// A truly first-to-complete Any would need to record and replay WHICH
// branch happened to finish first, since goroutine scheduling is
// non-deterministic - but nothing observes that ordering here beyond
// picking a result, and returning the first-in-INPUT-ORDER success is
// fully deterministic and replay-safe without needing to checkpoint
// anything extra, so that's what this does instead of the strictly
// literal "first-to-complete" semantics.
//
// # Updated for the completion-policy-contract fix
//
// Any configures MinSuccessful: 1, so - unlike AllSettled's
// MinSuccessful: 0 - this threshold genuinely CAN go unmet (every branch
// fails). Before this fix, Parallel itself detected that and returned a
// *BatchFailedError directly; now it always returns a valid BatchResult
// (with HasFailure() true, Status() "FAILED"), so Any must inspect
// result.Items itself to find either a success or, if none exists,
// reconstruct the *AggregateError its OWN documented contract still
// promises - which is exactly what the loop below already did
// (unchanged from before this fix; only the PRECEDING err-from-Parallel
// branch's own reasoning changed, since that path is now only ever a
// genuine, unrelated error).
func Any[TOut any](dc types.DurableContext, id string, branches []func(types.DurableContext) (TOut, error), opts ...ParallelOption[TOut]) (TOut, error) {
	minOne := 1
	opts = append([]ParallelOption[TOut]{WithParallelCompletionConfig[TOut](types.CompletionConfig{MinSuccessful: &minOne})}, opts...)
	result, err := Parallel(dc, id, branches, opts...)
	var zero TOut
	if err != nil {
		return zero, err
	}

	var errs []error
	for _, item := range result.Items {
		if item.Err == nil {
			return item.Value, nil
		}
		if item.Err != errBatchSkipped {
			errs = append(errs, item.Err)
		}
	}
	return zero, &AggregateError{Errors: errs}
}

// Race runs branches via Parallel and returns the result of the
// first-in-input-order branch (see Any's determinism note - the same
// caveat applies here), regardless of success or failure. Mirrors
// Promise.race semantics, with that one caveat.
func Race[TOut any](dc types.DurableContext, id string, branches []func(types.DurableContext) (TOut, error), opts ...ParallelOption[TOut]) (TOut, error) {
	result, err := AllSettled(dc, id, branches, opts...)
	var zero TOut
	if err != nil {
		return zero, err
	}
	if len(result.Items) == 0 {
		return zero, fmt.Errorf("operations.Race: no branches given")
	}
	first := result.Items[0]
	return first.Value, first.Err
}
