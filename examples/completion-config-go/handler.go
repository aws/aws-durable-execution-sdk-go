// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring this repo's other
// examples' handler.go/handler_test.go split.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// RecordBatchEvent is this example's input shape: a batch of records to
// import, where some records are deliberately marked as "bad" (should
// fail during import) so the batch's CompletionConfig threshold has
// something real to tolerate (or not).
//
// This mirrors a realistic bulk-import scenario: importing N independent
// records where a handful of malformed/rejected records shouldn't sink
// the entire batch, as long as "most" records succeed.
type RecordBatchEvent struct {
	RecordIDs []string `json:"recordIds"`
	// BadRecordIDs marks which of RecordIDs should fail its simulated
	// import step - a plain []string rather than a map, since JSON event
	// payloads are the natural place for a test/demo to describe "which
	// items fail" declaratively, without needing any special serdes.
	BadRecordIDs []string `json:"badRecordIds"`
	// ToleratedFailureCount is forwarded directly into
	// operations.WithMapCompletionConfig's types.CompletionConfig - see
	// that option's doc. Left as *int (not int) so the event can omit it
	// entirely to exercise Map's own default Promise.all-style semantics
	// if ever needed, though every scenario in this example sets it
	// explicitly.
	ToleratedFailureCount *int `json:"toleratedFailureCount,omitempty"`
	// SequentialExecution, if true, caps Map's concurrency at 1 via
	// WithMapMaxConcurrency. Only needed for the failure-threshold-
	// exceeded scenario (see handler_test.go's
	// TestHandler_ExceedsToleratedFailureThreshold): with the DEFAULT
	// unbounded concurrency, every item's goroutine is spawned before
	// any of them can finish, so runBatch's early-exit check
	// (completion.thresholdExceeded, evaluated inside each item's own
	// goroutine right before it runs) races against the Go scheduler -
	// on rare schedules, a later item can observe the threshold as
	// already-exceeded before it gets a chance to run at all, becoming
	// errBatchSkipped instead of genuinely running and
	// succeeding/failing on its own merits (see batch.go's own doc on
	// errBatchSkipped: this is a real, acknowledged possibility of
	// Map/Parallel's concurrent design, not a bug in this example).
	// That's a non-deterministic operation-log SHAPE (an item present
	// vs. entirely absent, not just a different status), which would
	// make this scenario's golden-file assertion flaky through no
	// mistake of the SDK's own correctness - forcing sequential
	// execution here removes the race entirely: each item is fully
	// finished (and failedCount incremented, if applicable) before the
	// next one's own threshold check ever runs, exactly matching a
	// non-concurrent Map's own well-defined ordering. The
	// tolerated-failures-succeed scenario deliberately does NOT set
	// this, since it has no early-exit at all (the batch never trips
	// its threshold, so every item runs regardless of concurrency) and
	// is a better demonstration of Map's normal, fully-concurrent
	// behavior.
	SequentialExecution bool `json:"sequentialExecution,omitempty"`
}

// ImportedRecord is one Map iteration's successful result.
type ImportedRecord struct {
	RecordID string `json:"recordId"`
}

// RecordBatchResult is this example's output shape.
type RecordBatchResult struct {
	// SucceededCount and FailedCount summarize operations.BatchResult's
	// own per-item Items slice (see BatchResult.SucceededCount) so a
	// caller doesn't need to know this SDK's internal BatchResult shape
	// just to see the headline numbers.
	SucceededCount int `json:"succeededCount"`
	FailedCount    int `json:"failedCount"`
	// ImportedRecordIDs lists only the records that succeeded, in input
	// order - the "tolerated failures" simply don't appear here.
	ImportedRecordIDs []string `json:"importedRecordIds"`
}

// handler demonstrates operations.WithMapCompletionConfig: a Map fan-out
// over RecordIDs, where every record in BadRecordIDs is configured to
// fail its simulated import step. With ToleratedFailureCount set high
// enough, the batch as a whole SUCCEEDS despite those individual
// failures (this is the interesting, otherwise-easy-to-miss behavior
// this example exists to demonstrate - see
// docs/remaining-work.md/docs/ts-sdk-examples-comparison.md's "Map /
// Parallel" section for why this was a real, zero-Go-example gap before
// this example existed: the underlying types.CompletionConfig evaluation
// was already fully implemented and unit-tested at the SDK level, but no
// example ever set a CompletionConfig option at all). If
// ToleratedFailureCount is set too low (fewer than the actual number of
// bad records), the batch instead correctly FAILS - see
// handler_test.go's TestHandler_ExceedsToleratedFailureThreshold for
// that scenario.
//
// Per operations.Map's own doc, a per-item failure does not stop other
// already-running items - every item still runs, and the failure only
// counts toward the configured threshold. WithMapCompletionConfig's
// MinSuccessful is deliberately NOT used here in favor of
// ToleratedFailureCount, since "tolerate up to N failures" is the more
// direct fit for this bulk-import framing (MinSuccessful would require
// recomputing "how many can fail" as "total - minSuccessful" by hand at
// the call site, which is exactly the kind of restatement
// ToleratedFailureCount exists to avoid) - see
// operations.WithMapCompletionConfig's own doc for the full set of
// available thresholds.
func handler(event RecordBatchEvent, dc types.DurableContext) (RecordBatchResult, error) {
	dc.Logger().Info("handler started", map[string]any{
		"recordCount":           len(event.RecordIDs),
		"badRecordCount":        len(event.BadRecordIDs),
		"toleratedFailureCount": event.ToleratedFailureCount,
	})

	badSet := make(map[string]bool, len(event.BadRecordIDs))
	for _, id := range event.BadRecordIDs {
		badSet[id] = true
	}

	var opts []operations.MapOption[string, ImportedRecord]
	if event.ToleratedFailureCount != nil {
		opts = append(opts, operations.WithMapCompletionConfig[string, ImportedRecord](types.CompletionConfig{
			ToleratedFailureCount: event.ToleratedFailureCount,
		}))
	}
	if event.SequentialExecution {
		opts = append(opts, operations.WithMapMaxConcurrency[string, ImportedRecord](1))
	}

	batch, err := operations.Map(dc, "import-records", event.RecordIDs,
		func(child types.DurableContext, recordID string, index int) (ImportedRecord, error) {
			return operations.Step(child, "import-record", func(sc types.StepContext) (ImportedRecord, error) {
				if badSet[recordID] {
					// Simulates a genuinely bad record (e.g. failed
					// schema validation) rather than a transient error -
					// there is nothing to retry here, so this step has
					// no retry strategy configured; it fails once and
					// that single failure is what counts toward the
					// batch's ToleratedFailureCount.
					return ImportedRecord{}, fmt.Errorf("record %s: failed validation", recordID)
				}
				return ImportedRecord{RecordID: recordID}, nil
			})
		},
		opts...,
	)
	if err != nil {
		// A genuine, unrelated error from Map itself (config validation,
		// dc type assertion, a replay-consistency mismatch) - Map no
		// longer returns an error here for a policy-not-met batch (see
		// operations.BatchResult's own doc for the full completion-
		// policy-contract fix): that case is now handled below, via
		// batch.ThrowIfError(), once Map has already returned a valid
		// BatchResult.
		return RecordBatchResult{}, fmt.Errorf("importing record batch: %w", err)
	}
	if throwErr := batch.ThrowIfError(); throwErr != nil && batch.CompletionReason == operations.CompletionReasonFailureToleranceExceeded {
		// The batch's own ToleratedFailureCount threshold was genuinely
		// EXCEEDED (CompletionReason confirms this, distinct from the
		// "some items failed but the threshold still tolerated them"
		// case exercised by TestHandler_ToleratesFailuresWithinThreshold
		// - see this file's own doc comment for why this specific
		// distinction, not a bare "did any item fail" check, is the
		// correct thing to test here: ThrowIfError()/HasFailure() are
		// deliberately POLICY-AGNOSTIC, raw per-item checks - matching
		// the JS reference SDK's own identical, policy-agnostic
		// hasFailure/throwIfError() - so a batch that tolerated its
		// failures within budget still has HasFailure()==true and a
		// non-nil ThrowIfError(), exactly as intended; only
		// CompletionReason distinguishes "tolerated" from "exceeded").
		// This example's own documented intent (see handler's own doc
		// above: "If ToleratedFailureCount is set too low... the batch
		// instead correctly FAILS") is to treat ONLY the exceeded case
		// as a genuine execution failure - explicitly ask BatchResult
		// for exactly the *BatchFailedError Map itself used to return
		// automatically before the completion-policy-contract fix -
		// propagated as-is so callers can still inspect it via
		// errors.As, matching every other operation in this SDK's
		// error-handling convention (see pkg/durable/operations/errors.go).
		return RecordBatchResult{}, fmt.Errorf("importing record batch: %w", throwErr)
	}

	var importedIDs []string
	for _, item := range batch.Items {
		if item.Err == nil {
			importedIDs = append(importedIDs, item.Value.RecordID)
		}
	}

	result := RecordBatchResult{
		SucceededCount:    batch.SucceededCount(),
		FailedCount:       len(batch.Items) - batch.SucceededCount(),
		ImportedRecordIDs: importedIDs,
	}
	dc.Logger().Info("handler completed", map[string]any{
		"succeededCount": result.SucceededCount,
		"failedCount":    result.FailedCount,
	})
	return result, nil
}
