//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to examples/create-callback-go. Gated behind
// the cloudintegration build tag; makes a REAL, billed, ASYNCHRONOUS
// Lambda Invoke against a REAL deployed function in account
// 730758745077 (region us-east-1), followed by a real
// SendDurableExecutionCallbackSuccess call from this test itself. Not
// wired into any CI workflow.
//
// This example genuinely suspends on operations.AwaitCallback partway
// through - nothing EXTERNAL to this SDK will ever complete that
// callback on its own - so this test uses
// CloudTestRunner.RunUntilCallback/Continue (see
// examples/map-with-condition-and-callback-go/cloud_integration_test.go's
// own extensive doc comment for the full mechanism writeup this reuses
// verbatim) rather than the single-call Run every other, non-suspending
// cloud test in this repo uses.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// createCallbackGoExampleFunctionARN is the real, deployed function's
// qualified version-1 ARN, confirmed live via a real smoke-test Invoke
// immediately before writing this test.
const createCallbackGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:create-callback-go-example:1"

func TestCloudTestRunner_Handler_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active?)", err)
	}
	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[ShipmentEvent, ShipmentResult](
		createCallbackGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	event := ShipmentEvent{OrderID: "cloud-1"}

	// RunUntilCallback invokes ASYNCHRONOUSLY and polls until a
	// CALLBACK-type operation named "carrier-pickup" (CreateCallback's
	// own name, no "-callback" suffix - see handler.go's own doc for why
	// that suffix convention is specific to WaitForCallback) appears
	// with a real, backend-issued CallbackID.
	pending, arn, err := runner.RunUntilCallback(ctx, "", event, "carrier-pickup")
	if err != nil {
		t.Fatalf("RunUntilCallback: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING (suspended awaiting the carrier-pickup callback), got %s (%s)", pending.GetStatus(), msg)
	}
	if arn == "" {
		t.Fatal("expected a non-empty resolved execution ARN")
	}
	t.Logf("resolved execution ARN: %s", arn)

	callbackOp, ok := pending.GetOperation("carrier-pickup")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'carrier-pickup' while the execution is suspended")
	}
	if callbackOp.GetType() != types.OperationTypeCallback {
		t.Fatalf("expected CALLBACK type, got %s", callbackOp.GetType())
	}
	callbackDetails := callbackOp.GetCallbackDetails()
	if callbackDetails == nil || callbackDetails.CallbackID == "" {
		t.Fatal("expected a non-empty, real backend-issued CallbackID on the 'carrier-pickup' operation")
	}
	t.Logf("real backend-issued CallbackID: %s", callbackDetails.CallbackID)

	// Cross-check that BOTH interleaved Steps already completed BEFORE
	// the callback opened - proving CreateCallback genuinely let the
	// handler keep working instead of blocking immediately, against the
	// REAL backend (not just the fake in-memory client).
	registerStep, ok := pending.GetOperation("register-with-carrier")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'register-with-carrier'")
	}
	if registerStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'register-with-carrier' SUCCEEDED before suspension, got %s", registerStep.GetStatus())
	}
	inventoryStep, ok := pending.GetOperation("check-inventory")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'check-inventory'")
	}
	if inventoryStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'check-inventory' SUCCEEDED before suspension, got %s", inventoryStep.GetStatus())
	}

	// Send the REAL callback via the real
	// SendDurableExecutionCallbackSuccess API, routed through
	// Operation.SendCallbackSuccess.
	if err := callbackOp.SendCallbackSuccess("picked-up"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	final, err := runner.Continue(ctx, arn)
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED after sending the real callback, got %s (%s)", final.GetStatus(), msg)
	}

	finalCallbackOp, ok := final.GetOperation("carrier-pickup")
	if !ok || finalCallbackOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatal("expected 'carrier-pickup' to report SUCCEEDED in the final terminal result")
	}
}
