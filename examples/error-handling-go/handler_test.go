// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria: every example must run and pass against the local test
// runner, asserting on the actual checkpointed operations (via the
// event-history golden-file pattern), not just the final result.
//
// See nondeterministic_test.go for a SEPARATE test demonstrating
// *operations.NonDeterministicReplayError - the other half of this
// example's error-hierarchy coverage (docs/remaining-work.md §4 task
// 11), which needs a genuinely different scenario (a redeployed handler
// calling a different operation at the same step ID) than a plain
// step-failure test can express.
package main

import (
	"strings"
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_ChargeSucceeds(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(ChargeCardEvent{OrderID: "order-1", Amount: 42.50, AlwaysFails: false})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[ChargeCardResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.OrderID != "order-1" {
		t.Fatalf("expected OrderID to round-trip, got %q", out.OrderID)
	}
	if out.ChargeReceiptID != "receipt-order-1" {
		t.Fatalf("expected receipt 'receipt-order-1', got %q", out.ChargeReceiptID)
	}

	chargeStep, ok := result.GetOperation("charge-card")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'charge-card'")
	}
	if chargeStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected charge-card step SUCCEEDED, got %s", chargeStep.GetStatus())
	}

	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/error-handling-go/... -run TestHandler_ChargeSucceeds
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_ChargeSucceeds.history.json")
}

// TestHandler_ChargeExhaustsRetries drives the simulated card-decline
// path (AlwaysFails: true) to exhaust the step's configured 3-attempt
// FixedDelay retry strategy, demonstrating this example's core point:
// the handler's own errors.As-based inspection of the resulting
// *operations.StepFailedError (see handler.go), surfaced here through
// the wrapped error message this test asserts contains the SPECIFIC
// attempt count and operation id the inspection recovered - not just a
// generic "step failed" string, which would be indistinguishable from
// what an UNINSPECTED opaque error would have produced too.
func TestHandler_ChargeExhaustsRetries(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(ChargeCardEvent{OrderID: "order-2", Amount: 999.00, AlwaysFails: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED once the card-decline retries are exhausted, got %s", result.GetStatus())
	}

	msg, ok := result.GetError()
	if !ok || msg == "" {
		t.Fatal("expected a non-empty error message")
	}

	chargeStep, ok := result.GetOperation("charge-card")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'charge-card'")
	}

	// The handler's own errors.As-based inspection (see handler.go)
	// enriches the propagated error with the SPECIFIC attempt count
	// (3, since utils.Presets.FixedDelay(0s, 3) retries exactly 3
	// times before giving up) and operation id it recovered from the
	// *operations.StepFailedError - asserting on this substring proves
	// the inspection actually ran and found real data, not just that
	// SOME error occurred. The operation id itself is a SHA-256 hash
	// (see context.Context.NextStepID's doc - operation IDs are opaque
	// hashes, not plain hierarchical strings, as of this SDK's SHA-256
	// operation-ID-hashing change), so this asserts against the step's
	// OWN real ID (chargeStep.GetID()) recovered from the result,
	// rather than a hardcoded literal like "1" that no longer means
	// anything once IDs are hashed.
	wantIDSubstring := "operation id " + chargeStep.GetID()
	if !strings.Contains(msg, "charge-card step failed after 3 attempt(s)") || !strings.Contains(msg, wantIDSubstring) {
		t.Fatalf("expected the error message to reflect the inspected StepFailedError's Attempt/ID fields (looking for %q), got %q", wantIDSubstring, msg)
	}

	if chargeStep.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected charge-card step FAILED, got %s", chargeStep.GetStatus())
	}
	if chargeStep.GetStepDetails() == nil || chargeStep.GetStepDetails().Attempt != 2 {
		// See retry-go's handler_test.go for the same Attempt semantics
		// explained in full: a RETRY checkpoint is what increments
		// StepDetails.Attempt, and FixedDelay(0s, 3) retries twice
		// (after attempt 1 and attempt 2) before giving up permanently
		// on attempt 3's own terminal FAIL checkpoint, which does not
		// itself touch Attempt further - so the CHECKPOINTED Attempt is
		// 2, even though the step's fn actually ran 3 times (which is
		// what the error message above, sourced from
		// StepFailedError.Attempt - a parameter passed explicitly by
		// retryOrFail's caller, not read back off StepDetails - correctly
		// reports as 3).
		t.Fatalf("expected charge-card step to have recorded 2 retries before its terminal failure, got %+v", chargeStep.GetStepDetails())
	}

	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/error-handling-go/... -run TestHandler_ChargeExhaustsRetries
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_ChargeExhaustsRetries.history.json")
}
