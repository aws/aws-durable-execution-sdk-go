// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria (§10 task 23). Like wait-for-callback-go's test, this drives
// TWO invocations via LocalTestRunner.Continue and
// Operation.SendCallbackSuccess/Failure to simulate the external
// approval system resolving the batch-approval callback in between - the
// default SkipTime: true (dtesting.New(handler, nil)) resolves the
// WaitForCondition carrier-status polling's fixed-delay waits internally
// within the FIRST invocation, so only the WaitForCallback suspension
// point requires a second invocation, matching this repo's established
// convention that only genuine external-signal suspensions (not
// SkipTime-eligible timers) need multi-invocation test driving.
package main

import (
	"fmt"
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_ApprovedBatchWithCarrierPolling(t *testing.T) {
	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:batch-approval-test:1"

	event := BatchEvent{
		OrderIDs:           []string{"order-1", "order-2", "order-3"},
		CarrierPollOrderID: "order-2",
	}

	// First invocation: Map prices all three orders (order-2's own Map
	// iteration additionally polls carrier status via
	// WaitForCondition - fully resolved within this single invocation
	// since SkipTime fast-forwards the FixedDelay waits between polls),
	// then the handler reaches WaitForCallback, checkpoints the
	// callback registration and its submitter step, and suspends - no
	// manager has approved yet.
	pending, err := runner.Continue(arn, event)
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING while awaiting batch approval, got %s (%s)", pending.GetStatus(), msg)
	}

	// Confirm every Map iteration (including the carrier-polling one)
	// already fully resolved during this FIRST invocation, before the
	// callback suspension - proving Map's own work completes
	// independently of, and prior to, the WaitForCallback gate.
	priceCtx, ok := pending.GetOperation("price-orders")
	if !ok {
		t.Fatal("expected to find a CONTEXT/MAP operation named 'price-orders'")
	}
	if priceCtx.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'price-orders' MAP context SUCCEEDED before the callback resolves, got %s", priceCtx.GetStatus())
	}
	mapChildren := pending.GetChildOperations(priceCtx.GetID())
	if len(mapChildren) != 3 {
		t.Fatalf("expected 3 MAP_ITERATION children, got %d", len(mapChildren))
	}

	// The carrier-status poll for order-2 specifically should have run 3
	// times (matching handler.go's simulated carrier: "pending" for the
	// first 2 polls, "in_transit" on the 3rd) and already be resolved -
	// nested inside order-2's own MAP_ITERATION child context, a
	// genuinely different nesting depth from wait-for-condition-go's
	// top-level-only WaitForCondition call.
	carrierPoll, ok := pending.GetOperation("poll-carrier-status")
	if !ok {
		t.Fatal("expected to find a CONTEXT/WAIT_FOR_CONDITION operation named 'poll-carrier-status' nested inside order-2's Map iteration")
	}
	if carrierPoll.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'poll-carrier-status' SUCCEEDED (resolved via SkipTime within the first invocation), got %s", carrierPoll.GetStatus())
	}

	callbackOp, ok := pending.GetOperation("batch-approval-callback")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'batch-approval-callback'")
	}
	if callbackOp.GetStatus() != types.OperationStatusStarted {
		t.Fatalf("expected callback STARTED, got %s", callbackOp.GetStatus())
	}

	submitStep, ok := pending.GetOperation("batch-approval-submit")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'batch-approval-submit' (the submitter)")
	}
	if submitStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected submitter step SUCCEEDED (it just logs and returns), got %s", submitStep.GetStatus())
	}

	// Simulate the manager approving the whole priced batch.
	if err := callbackOp.SendCallbackSuccess("approved"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	// Second invocation: the callback has resolved, so WaitForCallback
	// completes and the handler returns its final result. Every Map
	// iteration and the carrier poll replay-skip via their own
	// checkpoints - none of that work re-runs.
	final, err := runner.Continue(arn, event)
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED after approval, got %s (%s)", final.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[BatchResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if len(out.PricedOrders) != 3 {
		t.Fatalf("expected 3 priced orders, got %d", len(out.PricedOrders))
	}
	if out.ApprovalStatus != "approved" {
		t.Fatalf("expected approval status 'approved', got %q", out.ApprovalStatus)
	}

	var order2 *PricedOrder
	for i := range out.PricedOrders {
		if out.PricedOrders[i].OrderID == "order-2" {
			order2 = &out.PricedOrders[i]
		}
	}
	if order2 == nil {
		t.Fatal("expected order-2 to be present in the priced batch")
	}
	if order2.CarrierStatus != "in_transit" {
		t.Fatalf("expected order-2's CarrierStatus to be 'in_transit' after polling resolves, got %q", order2.CarrierStatus)
	}
	for _, o := range out.PricedOrders {
		if o.OrderID != "order-2" && o.CarrierStatus != "" {
			t.Fatalf("expected only order-2 to have a non-empty CarrierStatus, but order %s has %q", o.OrderID, o.CarrierStatus)
		}
	}

	approvalCtx, ok := final.GetOperation("batch-approval")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'batch-approval'")
	}
	if approvalCtx.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type, got %s", approvalCtx.GetType())
	}
	if approvalCtx.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'batch-approval' context SUCCEEDED, got %s", approvalCtx.GetStatus())
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): pins the exact shape of the FINAL operation log - a
	// CONTEXT/MAP nesting 3 MAP_ITERATION children (one of which further
	// nests a CONTEXT/WAIT_FOR_CONDITION), followed by a sibling
	// CONTEXT/WAIT_FOR_CALLBACK nesting a CALLBACK and a submitter STEP -
	// a genuinely distinct shape from any single existing example's
	// golden file in this repo. Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/map-with-condition-and-callback-go/... -run TestHandler_ApprovedBatchWithCarrierPolling
	dtesting.AssertEventSignatures(t, final, "testdata/TestHandler_ApprovedBatchWithCarrierPolling.history.json")
}

func TestHandler_RejectedBatch(t *testing.T) {
	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:batch-rejection-test:1"

	// No CarrierPollOrderID set - this scenario isolates the
	// WaitForCallback rejection path from the WaitForCondition polling
	// path, so a failure here is unambiguously about the callback, not
	// an interaction with the carrier-polling order.
	event := BatchEvent{OrderIDs: []string{"order-4", "order-5"}}

	pending, err := runner.Continue(arn, event)
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		t.Fatalf("expected PENDING, got %s", pending.GetStatus())
	}

	// Confirm no WaitForCondition operation exists at all in this
	// scenario (no order requested carrier polling) - isolating this
	// test's failure path from the polling mechanism entirely.
	if _, ok := pending.GetOperation("poll-carrier-status"); ok {
		t.Fatal("expected no 'poll-carrier-status' operation when CarrierPollOrderID is unset")
	}

	callbackOp, ok := pending.GetOperation("batch-approval-callback")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'batch-approval-callback'")
	}

	// Simulate the manager rejecting the whole batch.
	if err := callbackOp.SendCallbackFailure(types.ErrorObject{ErrorMessage: "batch total exceeds manager's approval limit"}); err != nil {
		t.Fatalf("SendCallbackFailure: %v", err)
	}

	final, err := runner.Continue(arn, event)
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusFailed {
		t.Fatalf("expected FAILED after rejection, got %s", final.GetStatus())
	}

	msg, ok := final.GetError()
	if !ok || msg == "" {
		t.Fatal("expected a non-empty error message")
	}

	approvalCtx, ok := final.GetOperation("batch-approval")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'batch-approval'")
	}
	if approvalCtx.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected 'batch-approval' context FAILED, got %s", approvalCtx.GetStatus())
	}

	// Tighter than a bare non-empty check (matching
	// docs/ts-sdk-examples-comparison.md gap 5/8's established
	// convention in this repo, and mirroring wait-for-callback-go's
	// identical TestHandler_RejectedExpense assertion exactly): the
	// handler's own %w-wrapping ("batch approval: %w" - see handler.go)
	// wraps operations.WaitForCallback's returned error, which (since
	// WaitForCallback is implemented as RunInChildContext(dc, id, fn) -
	// see callback.go) is a *operations.ChildContextFailedError wrapping
	// the *operations.CallbackFailedError the manager's SendCallbackFailure
	// ErrorObject produced.
	callbackOpForID, ok := final.GetOperation("batch-approval-callback")
	if !ok {
		t.Fatal("expected to find the 'batch-approval-callback' CALLBACK operation even though it failed")
	}
	wantCallbackMsg := fmt.Sprintf("callback %q (id %s): batch total exceeds manager's approval limit", "batch-approval-callback", callbackOpForID.GetID())
	wantContextMsg := fmt.Sprintf("child context %q (id %s): %s", "batch-approval", approvalCtx.GetID(), wantCallbackMsg)
	wantMsg := fmt.Sprintf("batch approval: %s", wantContextMsg)
	if msg != wantMsg {
		t.Fatalf("expected the error message to exactly match the composed ChildContextFailedError/CallbackFailedError format\n  got:      %q\n  expected: %q", msg, wantMsg)
	}

	// Also assert directly on the checkpointed operations' own structured
	// errors (Operation.GetError() *types.ErrorObject) as a second,
	// stronger, non-string-matching check.
	if callbackOpForID.GetError() == nil {
		t.Fatal("expected the 'batch-approval-callback' CALLBACK operation to carry a structured checkpointed Error")
	}
	if callbackOpForID.GetError().ErrorMessage != "batch total exceeds manager's approval limit" {
		t.Fatalf("expected the callback's checkpointed ErrorMessage to be the rejection text, got %q", callbackOpForID.GetError().ErrorMessage)
	}
	if approvalCtx.GetError() == nil {
		t.Fatal("expected the 'batch-approval' CONTEXT operation's checkpointed Error to be populated")
	}
	if approvalCtx.GetError().ErrorMessage != wantCallbackMsg {
		t.Fatalf("expected the enclosing CONTEXT operation's checkpointed ErrorMessage to be %q, got %q", wantCallbackMsg, approvalCtx.GetError().ErrorMessage)
	}

	// Even though the batch was ultimately rejected, both orders' own
	// Map iterations should have already succeeded (pricing happens
	// BEFORE the approval gate, per handler.go's own sequencing) -
	// confirming the callback's rejection doesn't retroactively unwind
	// already-completed Map work.
	priceCtx, ok := final.GetOperation("price-orders")
	if !ok {
		t.Fatal("expected to find the 'price-orders' MAP context even though the batch was ultimately rejected")
	}
	if priceCtx.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'price-orders' MAP context to remain SUCCEEDED despite the later callback rejection, got %s", priceCtx.GetStatus())
	}

	// Event-history/signature assertion: a DISTINCT golden file from
	// TestHandler_ApprovedBatchWithCarrierPolling's, both because the
	// callback here is REJECTED (not approved) and because this scenario
	// has no WaitForCondition operation at all (CarrierPollOrderID
	// unset) - a meaningfully different operation-log shape on both
	// counts. Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/map-with-condition-and-callback-go/... -run TestHandler_RejectedBatch
	dtesting.AssertEventSignatures(t, final, "testdata/TestHandler_RejectedBatch.history.json")
}
