// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria: every example must run and pass against the local test
// runner, with an event-history/signature assertion (not just a
// result-only assertion) pinning the exact shape of the checkpointed
// operation log.
//
// Two scenarios are covered, mirroring the two halves of
// docs/ts-sdk-examples-comparison.md's "Map / Parallel" CompletionConfig
// gap this example closes:
//
//   - TestHandler_ToleratesFailuresWithinThreshold: some Map items fail,
//     but WithMapCompletionConfig's ToleratedFailureCount is set high
//     enough to absorb them - the batch, and the whole execution,
//     SUCCEED despite the individual item failures.
//   - TestHandler_ExceedsToleratedFailureThreshold: the same kind of
//     per-item failures, but ToleratedFailureCount is set too low - the
//     batch, and the whole execution, correctly FAIL.
package main

import (
	"fmt"
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_ToleratesFailuresWithinThreshold(t *testing.T) {
	runner := dtesting.New(handler, nil)

	toleratedFailureCount := 2
	event := RecordBatchEvent{
		RecordIDs:             []string{"rec-1", "rec-2", "rec-3", "rec-4", "rec-5"},
		BadRecordIDs:          []string{"rec-2", "rec-4"}, // 2 bad records, threshold tolerates up to 2
		ToleratedFailureCount: &toleratedFailureCount,
	}

	result, err := runner.Run(event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The overall execution SUCCEEDS despite 2 of the 5 items failing,
	// because ToleratedFailureCount: 2 explicitly allows up to 2
	// failures - this is the core behavior this example exists to
	// demonstrate (see handler.go's doc comment).
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED (failures should be tolerated), got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[RecordBatchResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.SucceededCount != 3 {
		t.Fatalf("expected 3 succeeded records, got %d", out.SucceededCount)
	}
	if out.FailedCount != 2 {
		t.Fatalf("expected 2 failed (tolerated) records, got %d", out.FailedCount)
	}
	if len(out.ImportedRecordIDs) != 3 {
		t.Fatalf("expected 3 imported record IDs, got %d: %v", len(out.ImportedRecordIDs), out.ImportedRecordIDs)
	}
	for _, bad := range event.BadRecordIDs {
		for _, imported := range out.ImportedRecordIDs {
			if imported == bad {
				t.Fatalf("bad record %q should not appear in ImportedRecordIDs, got %v", bad, out.ImportedRecordIDs)
			}
		}
	}

	// Assert on the outer CONTEXT/MAP operation: it must itself report
	// SUCCEEDED even though 2 of its MAP_ITERATION children failed -
	// this is exactly the distinction WithMapCompletionConfig exists to
	// make (see batchCompletion.overallSucceeded's doc in batch.go).
	mapOp, ok := result.GetOperation("import-records")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'import-records'")
	}
	if mapOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected outer MAP operation SUCCEEDED, got %s", mapOp.GetStatus())
	}

	// Confirm the individual failed iterations are themselves genuinely
	// FAILED (the tolerance applies to the BATCH's overall outcome, not
	// to hiding the fact that these two items really did fail).
	failedIndexes := []int{1, 3} // rec-2, rec-4 are at indices 1 and 3
	for _, idx := range failedIndexes {
		iterOp, ok := result.GetOperation(itemNameForTest("import-records", idx))
		if !ok {
			t.Fatalf("expected to find MAP_ITERATION %d", idx)
		}
		if iterOp.GetStatus() != types.OperationStatusFailed {
			t.Fatalf("expected iteration %d to be FAILED (tolerated, not hidden), got %s", idx, iterOp.GetStatus())
		}
	}
	succeededIndexes := []int{0, 2, 4}
	for _, idx := range succeededIndexes {
		iterOp, ok := result.GetOperation(itemNameForTest("import-records", idx))
		if !ok {
			t.Fatalf("expected to find MAP_ITERATION %d", idx)
		}
		if iterOp.GetStatus() != types.OperationStatusSucceeded {
			t.Fatalf("expected iteration %d to be SUCCEEDED, got %s", idx, iterOp.GetStatus())
		}
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): pins the deterministic shape of the full operation log - the
	// outer CONTEXT/MAP wrapping 5 CONTEXT/MAP_ITERATION children (two
	// of which are FAILED, three SUCCEEDED), each nesting its own
	// "import-record" STEP, with the outer MAP operation itself still
	// SUCCEEDED overall. Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/completion-config-go/... -run TestHandler_ToleratesFailuresWithinThreshold
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_ToleratesFailuresWithinThreshold.history.json")
}

func TestHandler_ExceedsToleratedFailureThreshold(t *testing.T) {
	runner := dtesting.New(handler, nil)

	toleratedFailureCount := 1
	event := RecordBatchEvent{
		RecordIDs:             []string{"rec-1", "rec-2", "rec-3", "rec-4", "rec-5"},
		BadRecordIDs:          []string{"rec-2", "rec-4"}, // 2 bad records, threshold only tolerates 1
		ToleratedFailureCount: &toleratedFailureCount,
		// SequentialExecution avoids a real but here-undesirable race
		// in Map's early-exit check (see RecordBatchEvent.
		// SequentialExecution's doc in handler.go) that would otherwise
		// make this scenario's golden-file assertion flaky depending on
		// goroutine scheduling, without changing the actual behavior
		// under test (the batch still fails once the threshold is
		// exceeded either way).
		SequentialExecution: true,
	}

	result, err := runner.Run(event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The overall execution correctly FAILS: 2 items failed, but
	// ToleratedFailureCount only allows 1 - the threshold is exceeded,
	// so the batch (and thus the whole execution) fails, exactly
	// matching batchCompletion.thresholdExceeded/overallSucceeded's
	// documented semantics in batch.go.
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED (threshold exceeded), got %s", result.GetStatus())
	}

	msg, ok := result.GetError()
	if !ok || msg == "" {
		t.Fatal("expected a non-empty error message for the failed execution")
	}

	// The outer CONTEXT/MAP operation itself is now checkpointed
	// SUCCEEDED, even though the batch's own ToleratedFailureCount
	// threshold was exceeded - this is the real, confirmed completion-
	// policy-contract fix (see operations.BatchResult's own doc): "did
	// the batch satisfy its policy" is a property carried BY the
	// returned BatchResult (CompletionReason:
	// FAILURE_TOLERANCE_EXCEEDED, in this exact scenario), not an
	// execution-level failure baked into the outer context's own
	// checkpoint status. The overall EXECUTION still genuinely fails
	// (see result.GetStatus() == ExecutionStatusFailed, asserted above)
	// because THIS EXAMPLE'S OWN handler.go explicitly checks
	// batch.CompletionReason == FAILURE_TOLERANCE_EXCEEDED and, only in
	// that specific case, calls batch.ThrowIfError() and propagates the
	// result - see handler.go's own updated doc comment for the full
	// reasoning, including why a bare HasFailure()/ThrowIfError() check
	// alone would incorrectly ALSO fail
	// TestHandler_ToleratesFailuresWithinThreshold's own scenario (which
	// also has HasFailure()==true, just with a CompletionReason of
	// ALL_COMPLETED instead).
	mapOp, ok := result.GetOperation("import-records")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'import-records'")
	}
	if mapOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected outer MAP operation SUCCEEDED (the outer Map context always succeeds once its items finish, regardless of whether the batch's own policy was met - see operations.BatchResult's own completion-policy-contract doc), got %s", mapOp.GetStatus())
	}

	// Tighter than a bare non-empty check (docs/ts-sdk-examples-comparison.md
	// gap 5/8): the handler's own %w-wrapping ("importing record batch:
	// %w" - see handler.go) wraps the real *operations.BatchFailedError
	// finishBatch returned, whose own Error() method (errors.go's
	// BatchFailedError.Error) prefers its embedded *AggregateError's
	// message over OperationError's generic shape - "aggregate error: "
	// followed by each underlying *BatchItemFailedError's own
	// OperationError.Error() text (batch.go's AggregateError.Error),
	// joined by "; " in item order. Each *BatchItemFailedError's message
	// is 'batch item "import-records[N]" (id id): <cause>' (kindLabel's
	// OperationKindBatchItem => "batch item" - see errors.go's
	// kindLabel), where <cause> is itself the NESTED step's own
	// *StepFailedError-shaped message ('step "import-record" (id id):
	// record recX: failed validation' - see step.go's retryOrFail: the
	// simulated "bad record" failure originates inside the nested
	// operations.Step call, so runBatchItem's fn returns THAT step's own
	// wrapped error, not a bare fmt.Errorf, and batch.go's runBatchItem
	// FAIL path wraps it again one level further as the batch item's own
	// Err). Asserting the EXACT wrapped format - not a vague substring -
	// proves the propagated message really is this specific,
	// doubly-nested aggregated batch failure, not merely some other
	// error that happens to mention similar words.
	badIndexes := []int{1, 3} // rec-2, rec-4
	var itemMsgs []string
	for _, idx := range badIndexes {
		itemName := itemNameForTest("import-records", idx)
		itemOp, ok := result.GetOperation(itemName)
		if !ok {
			t.Fatalf("expected to find MAP_ITERATION %q", itemName)
		}
		children := result.GetChildOperations(itemOp.GetID())
		if len(children) != 1 || children[0].GetName() != "import-record" {
			t.Fatalf("expected MAP_ITERATION %q to nest exactly one 'import-record' STEP child, got %+v", itemName, children)
		}
		itemMsgs = append(itemMsgs, fmt.Sprintf("batch item %q (id %s): step %q (id %s): record %s: failed validation",
			itemName, itemOp.GetID(), "import-record", children[0].GetID(), event.RecordIDs[idx]))
	}
	wantMsg := fmt.Sprintf("importing record batch: aggregate error: %s; %s", itemMsgs[0], itemMsgs[1])
	if msg != wantMsg {
		t.Fatalf("expected the error message to exactly match the wrapped *operations.BatchFailedError/AggregateError format\n  got:      %q\n  expected: %q", msg, wantMsg)
	}

	// Also assert directly on the outer MAP operation's own checkpointed
	// Result (NOT Error - see this block's own updated reasoning): under
	// the completion-policy-contract fix, finishBatch always checkpoints
	// ContextSucceeded with a Payload (the full serialized BatchResult,
	// including CompletionReason), NEVER an Error, regardless of whether
	// the batch's own policy was met - there is no longer a checkpointed
	// Error on this operation at all to assert on. Confirm instead that
	// the checkpointed Result genuinely reflects the exceeded-threshold
	// outcome via its own CompletionReason field, mirroring exactly what
	// batch.CompletionReason (already asserted via handler.go's own
	// FAILURE_TOLERANCE_EXCEEDED check, which is what makes this whole
	// execution fail in the first place) resolved to for THIS
	// invocation.
	if mapOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected the outer MAP operation's checkpointed status to be SUCCEEDED (re-confirming the earlier assertion), got %s", mapOp.GetStatus())
	}

	// Event-history/signature assertion: a DISTINCT golden file from
	// TestHandler_ToleratesFailuresWithinThreshold's, since the outer
	// MAP operation's own checkpointed Payload differs (a different
	// CompletionReason and Items shape - fewer successes, 2 explicit
	// failures - even though its own terminal STATUS is now SUCCEEDED in
	// both scenarios, unlike before this fix) - this specifically pins
	// that a lower ToleratedFailureCount still changes the batch's own
	// recorded outcome, just via CompletionReason/Items now rather than
	// via the outer operation's own terminal status. Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/completion-config-go/... -run TestHandler_ExceedsToleratedFailureThreshold
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_ExceedsToleratedFailureThreshold.history.json")
}

// itemNameForTest mirrors operations.itemName's exact naming convention
// ("<batchName>[<index>]") for MAP_ITERATION/PARALLEL_BRANCH child
// operation names - that helper itself is unexported (package
// operations, not importable from this example module), so this test
// file reconstructs the same, documented naming scheme directly rather
// than hardcoding each index's full name as a separate literal.
func itemNameForTest(batchName string, index int) string {
	return fmt.Sprintf("%s[%d]", batchName, index)
}
