// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria (§10 task 23). Unlike examples/map-parallel-go's combined
// Map+Parallel test, every scenario here drives ONLY the standalone
// operations.All fan-out this example exists to demonstrate.
package main

import (
	"fmt"
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_AllChecksPassConcurrently(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(OrderEvent{OrderID: "order-1", SKU: "widget-42", Amount: 49.99})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[OrderResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.OrderID != "order-1" {
		t.Fatalf("expected OrderID to round-trip, got %q", out.OrderID)
	}
	if !out.FraudCleared || !out.InventoryInStock || !out.AddressValid || !out.PaymentAuthed {
		t.Fatalf("expected all four checks to pass, got %+v", out)
	}

	// The outer CONTEXT/PARALLEL operation ("order-checks") should have
	// exactly 4 PARALLEL_BRANCH children, one per independent check -
	// this is the core structural shape this example exists to
	// demonstrate: four genuinely independent branches, no Map anywhere.
	parallelCtx, ok := result.GetOperation("order-checks")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'order-checks'")
	}
	if parallelCtx.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type, got %s", parallelCtx.GetType())
	}
	if parallelCtx.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'order-checks' context SUCCEEDED, got %s", parallelCtx.GetStatus())
	}
	children := result.GetChildOperations(parallelCtx.GetID())
	if len(children) != 4 {
		t.Fatalf("expected 4 PARALLEL_BRANCH children, got %d", len(children))
	}
	for _, child := range children {
		if child.GetStatus() != types.OperationStatusSucceeded {
			t.Fatalf("expected child branch %q SUCCEEDED, got %s", child.GetName(), child.GetStatus())
		}
	}

	// Each branch's own nested STEP is independently checkpointed and
	// discoverable by name, confirming the branches are genuinely
	// isolated child contexts rather than a single flattened operation.
	for _, stepName := range []string{"fraud-check", "inventory-check", "address-validation", "payment-authorization"} {
		step, ok := result.GetOperation(stepName)
		if !ok {
			t.Fatalf("expected to find a STEP operation named %q", stepName)
		}
		if step.GetStatus() != types.OperationStatusSucceeded {
			t.Fatalf("expected step %q SUCCEEDED, got %s", stepName, step.GetStatus())
		}
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): pins the exact shape of the operation log - a single
	// CONTEXT/PARALLEL operation nesting exactly four PARALLEL_BRANCH
	// children, each with one nested STEP - so a future change that
	// altered how operations.All/Parallel checkpoints branches would be
	// caught even though the checks above only inspect specific
	// operations by name, not the full log shape. Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/parallel-processing-go/... -run TestHandler_AllChecksPassConcurrently
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_AllChecksPassConcurrently.history.json")
}

func TestHandler_FraudCheckFailsWholeBatch(t *testing.T) {
	runner := dtesting.New(handler, nil)

	// Amount >= 10000 makes the fraud-check branch's own Step return an
	// error (see handler.go: "event.Amount < 10000" is the fraud-check
	// step's pass condition) - operations.All's Promise.all-style
	// semantics (batch.go's own doc: "returns the successful results
	// only if every branch succeeds; otherwise ... *AggregateError")
	// mean a single failing branch fails the WHOLE operations.All call,
	// and therefore the whole execution, even though the other three
	// branches succeed on their own.
	result, err := runner.Run(OrderEvent{OrderID: "order-2", SKU: "widget-7", Amount: 15000})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED when the fraud-check branch fails, got %s", result.GetStatus())
	}

	msg, ok := result.GetError()
	if !ok || msg == "" {
		t.Fatal("expected a non-empty error message")
	}

	// Tighter than a bare non-empty check (matching
	// docs/ts-sdk-examples-comparison.md gap 5/8's established
	// convention in this repo): TestResult.GetError() only ever exposes
	// a flattened string (confirmed by reading result.go/runner.go in
	// full, matching every other example's established finding - there
	// is no live Go error value on this side of the boundary to run
	// errors.As against). The handler's own %w-wrapping ("order %s:
	// running concurrent checks: %w" - see handler.go) wraps
	// operations.All's returned error, which - since a Parallel branch
	// failure is reported the SAME way a Map item failure is
	// (batch.go's shared runBatch/batchScheduler machinery, per
	// completion-config-go's own precedent for the exact 'batch item
	// "..." (id id): step "..." (id id): <cause>' format) - is an
	// *operations.AggregateError wrapping one 'batch item "order-checks[0]"
	// (id <id>): step "fraud-check" (id <id>): fraud check failed: amount
	// exceeds threshold' entry. Asserting the EXACT composed format (not
	// a vague substring) proves every layer of this chain - handler
	// wrapper -> AggregateError -> batch-item wrapper -> the original
	// errFraudCheckFailed text - actually ran, matching this repo's
	// established structured-error assertion rigor.
	fraudStepForID, ok := result.GetOperation("fraud-check")
	if !ok {
		t.Fatal("expected to find the failing 'fraud-check' STEP operation")
	}
	parallelCtxForID, ok := result.GetOperation("order-checks")
	if !ok {
		t.Fatal("expected to find the 'order-checks' CONTEXT operation even though it failed")
	}
	// The failing PARALLEL_BRANCH child's own ID is the "batch item"
	// wrapper's ID in the composed message - a DIFFERENT ID from the
	// nested fraud-check STEP's own ID (the branch is its own child
	// context wrapping the step, per Parallel's implementation), found
	// via GetChildOperations rather than assumed.
	var branchForID dtesting.Operation
	for _, child := range result.GetChildOperations(parallelCtxForID.GetID()) {
		if child.GetStatus() == types.OperationStatusFailed {
			branchForID = child
			break
		}
	}
	if branchForID.GetID() == "" {
		t.Fatal("expected to find the failing PARALLEL_BRANCH child of 'order-checks'")
	}
	wantMsg := fmt.Sprintf(
		"order order-2: running concurrent checks: aggregate error: batch item %q (id %s): step %q (id %s): %s",
		"order-checks[0]", branchForID.GetID(), "fraud-check", fraudStepForID.GetID(), errFraudCheckFailed.Error(),
	)
	if msg != wantMsg {
		t.Fatalf("expected the error message to exactly match the composed handler/AggregateError/batch-item format\n  got:      %q\n  expected: %q", msg, wantMsg)
	}

	// Also assert directly on the failing STEP's own checkpointed
	// structured error (Operation.GetError() *types.ErrorObject) - a
	// stronger, non-string-matching check than the message-format
	// assertion above.
	if fraudStepForID.GetError() == nil {
		t.Fatal("expected the 'fraud-check' STEP operation to carry a structured checkpointed Error")
	}
	if fraudStepForID.GetError().ErrorMessage != errFraudCheckFailed.Error() {
		t.Fatalf("expected the fraud-check step's checkpointed ErrorMessage to be %q, got %q", errFraudCheckFailed.Error(), fraudStepForID.GetError().ErrorMessage)
	}

	// The outer CONTEXT/PARALLEL operation itself is now checkpointed
	// SUCCEEDED, even though one of its branches failed - this is the
	// real, confirmed completion-policy-contract fix (see operations.
	// BatchResult's own doc): "did the batch satisfy its policy" is a
	// property carried BY the returned BatchResult, not an execution-
	// level failure baked into the outer context's own checkpoint status
	// - only the individual FAILING BRANCH's own child context is
	// checkpointed FAILED (confirmed via branchForID, found above,
	// itself already required to have OperationStatusFailed to be
	// selected as "the failing PARALLEL_BRANCH child"). The overall
	// EXECUTION still genuinely fails (see result.GetStatus() ==
	// ExecutionStatusFailed, asserted above) because operations.All
	// itself still calls BatchResult.ThrowIfError() internally and
	// returns that error when any branch fails (see All's own doc for
	// why its OWN public contract - "otherwise it returns an
	// *AggregateError" - is deliberately unchanged by this fix), and
	// this handler still propagates that error uncaught.
	parallelCtx, ok := result.GetOperation("order-checks")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'order-checks' even though one of its branches failed")
	}
	if parallelCtx.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'order-checks' context SUCCEEDED (the outer Parallel context always succeeds once its branches finish, regardless of individual branch outcomes - see operations.BatchResult's own completion-policy-contract doc), got %s", parallelCtx.GetStatus())
	}
	if branchForID.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected the failing PARALLEL_BRANCH child itself to be checkpointed FAILED, got %s", branchForID.GetStatus())
	}

	// Confirm the OTHER three branches still ran to completion and
	// succeeded on their own - operations.Parallel's own doc guarantees a
	// failing branch does not cancel its siblings (matching Map's
	// identical "every item still runs to completion regardless"
	// semantics, per completion-config-go's README) - only the AGGREGATE
	// outcome is affected.
	for _, stepName := range []string{"inventory-check", "address-validation", "payment-authorization"} {
		step, ok := result.GetOperation(stepName)
		if !ok {
			t.Fatalf("expected to find a STEP operation named %q even though a sibling branch failed", stepName)
		}
		if step.GetStatus() != types.OperationStatusSucceeded {
			t.Fatalf("expected sibling step %q SUCCEEDED despite fraud-check failing, got %s", stepName, step.GetStatus())
		}
	}
	fraudStep, ok := result.GetOperation("fraud-check")
	if !ok {
		t.Fatal("expected to find the failing 'fraud-check' STEP operation")
	}
	if fraudStep.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected 'fraud-check' step FAILED, got %s", fraudStep.GetStatus())
	}

	// Event-history/signature assertion: a DISTINCT golden file from
	// TestHandler_AllChecksPassConcurrently's, since a failing branch
	// produces a meaningfully different operation-log shape (one
	// PARALLEL_BRANCH/STEP pair ends FAILED, and the enclosing
	// CONTEXT/PARALLEL ends FAILED too, rather than every branch
	// SUCCEEDED). Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/parallel-processing-go/... -run TestHandler_FraudCheckFailsWholeBatch
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_FraudCheckFailsWholeBatch.history.json")
}
