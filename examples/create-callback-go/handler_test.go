// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria. Like examples/wait-for-callback-go, CreateCallback genuinely
// suspends across TWO invocations - this test drives both halves using
// LocalTestRunner.Continue and Operation.SendCallbackSuccess/Failure to
// simulate the external carrier system resolving the callback in
// between. Unlike WaitForCallback, the callback checkpoint's own Name is
// exactly the caller-supplied id ("carrier-pickup") - CreateCallback has
// no automatic "-callback" suffix (see operations.CreateCallback's own
// doc: that wrapping convention is specific to WaitForCallback).
package main

import (
	"fmt"
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_SuccessfulPickup(t *testing.T) {
	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:create-callback-test:1"

	// First invocation: the handler creates the callback, registers it
	// with the carrier via a Step, checks inventory via another Step -
	// all BEFORE ever awaiting the callback result - then suspends once
	// it actually blocks on AwaitCallback.
	pending, err := runner.Continue(arn, ShipmentEvent{OrderID: "ord-001"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING while awaiting carrier pickup, got %s (%s)", pending.GetStatus(), msg)
	}

	callbackOp, ok := pending.GetOperation("carrier-pickup")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'carrier-pickup' (CreateCallback's own name, no '-callback' suffix)")
	}
	if callbackOp.GetStatus() != types.OperationStatusStarted {
		t.Fatalf("expected callback STARTED, got %s", callbackOp.GetStatus())
	}

	// Both interleaved Steps must have already run and succeeded BEFORE
	// suspension - proving CreateCallback let the handler do other
	// durable work instead of blocking immediately like WaitForCallback
	// would have.
	registerStep, ok := pending.GetOperation("register-with-carrier")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'register-with-carrier'")
	}
	if registerStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'register-with-carrier' step SUCCEEDED before suspension, got %s", registerStep.GetStatus())
	}
	inventoryStep, ok := pending.GetOperation("check-inventory")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'check-inventory'")
	}
	if inventoryStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'check-inventory' step SUCCEEDED before suspension, got %s", inventoryStep.GetStatus())
	}

	// Simulate the carrier's webhook firing.
	if err := callbackOp.SendCallbackSuccess("picked-up"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	// Second invocation: the callback has resolved, so AwaitCallback
	// returns and the handler completes.
	final, err := runner.Continue(arn, ShipmentEvent{OrderID: "ord-001"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED after carrier pickup, got %s (%s)", final.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[ShipmentResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.OrderID != "ord-001" {
		t.Fatalf("expected OrderID to round-trip, got %q", out.OrderID)
	}
	if !out.InventoryChecked {
		t.Fatal("expected InventoryChecked to be true")
	}
	if out.CarrierStatus != "picked-up" {
		t.Fatalf("expected CarrierStatus 'picked-up', got %q", out.CarrierStatus)
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c convention, mirrored from examples/wait-for-callback-go):
	// asserts on the deterministic SHAPE of the operation log across
	// both invocations against a committed golden file. Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/create-callback-go/... -run TestHandler_SuccessfulPickup
	dtesting.AssertEventSignatures(t, final, "testdata/TestHandler_SuccessfulPickup.history.json")
}

func TestHandler_FailedPickup(t *testing.T) {
	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:create-callback-fail-test:1"

	pending, err := runner.Continue(arn, ShipmentEvent{OrderID: "ord-002"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		t.Fatalf("expected PENDING, got %s", pending.GetStatus())
	}

	callbackOp, ok := pending.GetOperation("carrier-pickup")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'carrier-pickup'")
	}

	// Simulate the carrier reporting a failed pickup.
	if err := callbackOp.SendCallbackFailure(types.ErrorObject{ErrorMessage: "package not found at pickup location"}); err != nil {
		t.Fatalf("SendCallbackFailure: %v", err)
	}

	final, err := runner.Continue(arn, ShipmentEvent{OrderID: "ord-002"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED after carrier failure, got %s", final.GetStatus())
	}
	msg, ok := final.GetError()
	if !ok || msg == "" {
		t.Fatal("expected a non-empty error message")
	}

	// Tighter than a bare non-empty check (mirroring
	// examples/wait-for-callback-go's own precedent): the handler's own
	// %w-wrapping ("order %s: carrier pickup callback: %w" - see
	// handler.go) wraps AwaitCallback's returned result.Err directly -
	// unlike WaitForCallback (which is implemented as
	// RunInChildContext(dc, id, fn), adding an extra ChildContextFailedError
	// layer), CreateCallback's result.Err here is the RAW
	// *operations.CallbackFailedError with no extra wrapping layer, since
	// this handler calls CreateCallback/AwaitCallback directly rather
	// than through RunInChildContext.
	//
	// Re-fetch the callback operation from the SECOND (final) result,
	// not the first (pending) one - the first invocation's own snapshot
	// predates SendCallbackFailure and never observes the resolved
	// error (mirroring examples/wait-for-callback-go's own
	// callbackOpForID, which is likewise re-fetched from final).
	finalCallbackOp, ok := final.GetOperation("carrier-pickup")
	if !ok {
		t.Fatal("expected to find the 'carrier-pickup' CALLBACK operation even though it failed")
	}
	wantCallbackMsg := fmt.Sprintf("callback %q (id %s): package not found at pickup location", "carrier-pickup", finalCallbackOp.GetID())
	wantMsg := fmt.Sprintf("order ord-002: carrier pickup callback: %s", wantCallbackMsg)
	if msg != wantMsg {
		t.Fatalf("expected the error message to exactly match the composed CallbackFailedError format\n  got:      %q\n  expected: %q", msg, wantMsg)
	}

	if finalCallbackOp.GetError() == nil {
		t.Fatal("expected the 'carrier-pickup' CALLBACK operation to carry a structured checkpointed Error")
	}
	if finalCallbackOp.GetError().ErrorMessage != "package not found at pickup location" {
		t.Fatalf("expected the callback's checkpointed ErrorMessage to be %q, got %q", "package not found at pickup location", finalCallbackOp.GetError().ErrorMessage)
	}

	// Event-history/signature assertion - a DISTINCT golden file from
	// TestHandler_SuccessfulPickup's, since a failed pickup produces a
	// meaningfully different operation-log shape (the CALLBACK ends
	// FAILED here, not SUCCEEDED). Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/create-callback-go/... -run TestHandler_FailedPickup
	dtesting.AssertEventSignatures(t, final, "testdata/TestHandler_FailedPickup.history.json")
}
