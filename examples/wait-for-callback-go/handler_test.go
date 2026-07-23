// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria. Unlike Step/RunInChildContext, WaitForCallback genuinely
// suspends across TWO invocations - this test drives both halves using
// LocalTestRunner.Continue and Operation.SendCallbackSuccess/Failure to
// simulate the external approval system resolving the callback in
// between, matching the official Testing API Reference's "Assert on a
// callback" pattern.
package main

import (
	"fmt"
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestHandler_ApprovedExpense(t *testing.T) {
	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:expense-approval-test:1"

	// First invocation: the handler reaches WaitForCallback, checkpoints
	// the callback registration and the submitter step, and suspends -
	// no manager has approved or rejected yet.
	pending, err := runner.Continue(arn, ExpenseEvent{RequestID: "exp-001", Amount: 250.00, Requester: "alice"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING while awaiting manager approval, got %s (%s)", pending.GetStatus(), msg)
	}

	callbackOp, ok := pending.GetOperation("manager-approval-callback")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'manager-approval-callback'")
	}
	if callbackOp.GetStatus() != types.OperationStatusStarted {
		t.Fatalf("expected callback STARTED, got %s", callbackOp.GetStatus())
	}

	submitStep, ok := pending.GetOperation("manager-approval-submit")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'manager-approval-submit' (the submitter)")
	}
	if submitStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected submitter step SUCCEEDED (it just logs and returns), got %s", submitStep.GetStatus())
	}

	// Simulate the manager approving the request.
	if err := callbackOp.SendCallbackSuccess("approved"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	// Second invocation: the callback has resolved, so WaitForCallback
	// completes and the handler returns its final result.
	final, err := runner.Continue(arn, ExpenseEvent{RequestID: "exp-001", Amount: 250.00, Requester: "alice"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED after approval, got %s (%s)", final.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[ExpenseResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.RequestID != "exp-001" {
		t.Fatalf("expected RequestID to round-trip, got %q", out.RequestID)
	}
	if out.Decision != "approved" {
		t.Fatalf("expected decision 'approved', got %q", out.Decision)
	}

	approvalCtx, ok := final.GetOperation("manager-approval")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'manager-approval'")
	}
	if approvalCtx.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type, got %s", approvalCtx.GetType())
	}
	if approvalCtx.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'manager-approval' context SUCCEEDED, got %s", approvalCtx.GetStatus())
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): asserts on the deterministic SHAPE of the operation log - a
	// CONTEXT/WAIT_FOR_CALLBACK operation nesting a CALLBACK and a
	// submitter STEP - against a committed golden file. This is the
	// first example in this repo whose operation log spans TWO separate
	// invocations (the callback suspends the first, and only resolves
	// on the second, driven by SendCallbackSuccess above); the golden
	// file captures the FINAL log's shape after both invocations, and
	// would catch a regression that changed how WaitForCallback
	// composes CreateCallback + the submitter step (e.g. a dropped
	// checkpoint, or the submitter step disappearing) even though this
	// test's own checks above only inspect the callback and context
	// operations by name, not the full log. Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/wait-for-callback-go/... -run TestHandler_ApprovedExpense
	dtesting.AssertEventSignatures(t, final, "testdata/TestHandler_ApprovedExpense.history.json")
}

func TestHandler_RejectedExpense(t *testing.T) {
	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:expense-rejection-test:1"

	pending, err := runner.Continue(arn, ExpenseEvent{RequestID: "exp-002", Amount: 9999.00, Requester: "bob"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		t.Fatalf("expected PENDING, got %s", pending.GetStatus())
	}

	callbackOp, ok := pending.GetOperation("manager-approval-callback")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'manager-approval-callback'")
	}

	// Simulate the manager rejecting the request.
	if err := callbackOp.SendCallbackFailure(types.ErrorObject{ErrorMessage: "amount exceeds approval limit"}); err != nil {
		t.Fatalf("SendCallbackFailure: %v", err)
	}

	final, err := runner.Continue(arn, ExpenseEvent{RequestID: "exp-002", Amount: 9999.00, Requester: "bob"})
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

	approvalCtx, ok := final.GetOperation("manager-approval")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'manager-approval'")
	}
	if approvalCtx.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected 'manager-approval' context FAILED, got %s", approvalCtx.GetStatus())
	}

	// Tighter than a bare non-empty check (docs/ts-sdk-examples-comparison.md
	// gap 5/8): the handler's own %w-wrapping ("expense request %s: %w" -
	// see handler.go) wraps operations.WaitForCallback's returned error,
	// which - since WaitForCallback is implemented as
	// RunInChildContext(dc, id, fn) (see callback.go's WaitForCallback) -
	// is really a *operations.ChildContextFailedError produced by
	// RunInChildContext's own FAIL path (invoke.go) wrapping fn's
	// returned result.Err, itself the *operations.CallbackFailedError
	// AwaitCallback/callbackError (callback.go) built from the manager's
	// SendCallbackFailure ErrorObject above. Three real structured-error
	// layers, each with its own OperationError.Error() format
	// ('<kind> "<name>" (id <id>): <cause>' - errors.go), chained by %w
	// at every layer: handler's wrapper -> ChildContextFailedError ->
	// CallbackFailedError -> the literal rejection text. Asserting the
	// EXACT composed format (not a vague substring) proves every layer
	// of this chain actually ran and produced the real structured types,
	// not just that SOME error occurred.
	callbackOpForID, ok := final.GetOperation("manager-approval-callback")
	if !ok {
		t.Fatal("expected to find the 'manager-approval-callback' CALLBACK operation even though it failed")
	}
	wantCallbackMsg := fmt.Sprintf("callback %q (id %s): amount exceeds approval limit", "manager-approval-callback", callbackOpForID.GetID())
	wantContextMsg := fmt.Sprintf("child context %q (id %s): %s", "manager-approval", approvalCtx.GetID(), wantCallbackMsg)
	wantMsg := fmt.Sprintf("expense request exp-002: %s", wantContextMsg)
	if msg != wantMsg {
		t.Fatalf("expected the error message to exactly match the composed ChildContextFailedError/CallbackFailedError format\n  got:      %q\n  expected: %q", msg, wantMsg)
	}

	// Also assert directly on the checkpointed operations' own structured
	// errors (Operation.GetError() *types.ErrorObject - a stronger,
	// non-string-matching check than the message-format assertion
	// above): both the CALLBACK and the enclosing CONTEXT record the
	// manager's rejection text.
	if callbackOpForID.GetError() == nil {
		t.Fatal("expected the 'manager-approval-callback' CALLBACK operation to carry a structured checkpointed Error")
	}
	if callbackOpForID.GetError().ErrorMessage != "amount exceeds approval limit" {
		t.Fatalf("expected the callback's checkpointed ErrorMessage to be %q, got %q", "amount exceeds approval limit", callbackOpForID.GetError().ErrorMessage)
	}
	if approvalCtx.GetError() == nil {
		t.Fatal("expected the 'manager-approval' CONTEXT operation's checkpointed Error to be populated")
	}
	if approvalCtx.GetError().ErrorMessage != wantCallbackMsg {
		t.Fatalf("expected the enclosing CONTEXT operation's checkpointed ErrorMessage to be %q, got %q", wantCallbackMsg, approvalCtx.GetError().ErrorMessage)
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): a DISTINCT golden file from TestHandler_ApprovedExpense's,
	// since a rejected approval produces a meaningfully different
	// operation-log shape (the CONTEXT/WAIT_FOR_CALLBACK operation and
	// its nested CALLBACK end FAILED here, not SUCCEEDED) - each golden
	// file is meant to capture ONE specific scenario's shape, not a
	// single file shared across both the happy and failure paths.
	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/wait-for-callback-go/... -run TestHandler_RejectedExpense
	dtesting.AssertEventSignatures(t, final, "testdata/TestHandler_RejectedExpense.history.json")
}
