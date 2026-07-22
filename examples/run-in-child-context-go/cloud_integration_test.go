//go:build cloudintegration

// cloud_integration_test.go extends docs/remaining-work.md's §7 task
// 17b's CloudTestRunner wiring to examples/run-in-child-context-go,
// following the exact same pattern established by
// examples/completion-config-go/cloud_integration_test.go (see that
// file's own extensive header doc comment for the full rationale on the
// `//go:build cloudintegration` gating, why this is NOT wired into CI,
// and the three real CloudTestRunner/lambda_invoker.go/sdk_state_client.go
// bugs a prior session found and fixed to make this whole pattern work
// against a real deployed function at all - none of that is re-litigated
// here to avoid drifting out of sync with the original).
//
// # Why the ORIGINAL handler's happy path, not PaymentStepFails or the
// custom-serdes scenario
//
// handler.go's own doc comment documents THREE scenarios reachable from
// this example's single deployed function (selected via
// OrderEvent.UseCustomChildSerdes and OrderEvent.Amount):
//  1. The original happy path (Amount > 0, UseCustomChildSerdes: false) -
//     a "payment" RunInChildContext (validate-amount + charge-card
//     Steps) followed by a sibling top-level "create-shipping-label"
//     Step.
//  2. PaymentStepFails (Amount < 0) - charge-card genuinely fails,
//     propagating a *operations.ChildContextFailedError up through the
//     enclosing "payment" RunInChildContext.
//  3. customSerdesHandlerScenario (UseCustomChildSerdes: true) - a
//     DIFFERENT RunInChildContext ("reconciliation") using
//     operations.WithChildSerdes.
//
// This test uses scenario 1, per this task's own stated preference for
// "a fast, non-suspending happy path" - none of RunInChildContext's own
// operations (Step nested inside RunInChildContext) suspend at all, so
// every one of these three scenarios is equally fast in that sense; the
// deciding factor is instead PROOF VALUE for this specific, first
// RunInChildContext-exercising cloud test: scenario 1 is the one that
// proves the CONTEXT/RUN_IN_CHILD_CONTEXT nesting itself (a CONTEXT
// operation with nested child STEP operations, reconstructed correctly
// by sdk_state_client.go's reconstructOperationsFromHistory via
// ParentID-based pairing - see that function's own doc) round-trips
// correctly through the real backend and this runner's real polling,
// which is the more foundational, broadly-useful thing to confirm first
// for this example versus either failure-path or custom-Serdes-specific
// behavior. Scenario 2 (PaymentStepFails) and scenario 3 (custom child
// Serdes) remain candidates for a FUTURE session to add as additional
// cloud_integration_test.go tests in this same file, following this
// same pattern - not attempted in this session, per this task's own
// scoping preference to finish fewer examples correctly over rushing
// every possible scenario for every example.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// runInChildContextGoExampleFunctionARN is the REAL, already-deployed,
// currently-active target this test invokes - the latest published
// qualified version ARN, confirmed via a read-only
// `lambda:ListVersionsByFunction` check immediately before writing this
// test: version 4's CodeSha256
// (5571f2cbd5236092a2db75abdd7476ef64779443ee38d5affa5fd359752b37f7)
// matches $LATEST's, confirming version 4 is the function's current
// code, not a stale earlier version. Per CloudTestRunner.FunctionName's
// own doc comment, this MUST be a qualified ARN.
const runInChildContextGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:run-in-child-context-go-example:4"

// TestCloudTestRunner_OriginalHandler_HappyPath_RealDeployedFunction runs
// handler's original happy-path scenario (a valid order, Amount > 0,
// UseCustomChildSerdes: false, going through the "payment" child context
// + a sibling top-level shipping step) against the real deployed
// run-in-child-context-go-example function via testing.CloudTestRunner.
func TestCloudTestRunner_OriginalHandler_HappyPath_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active? see this file's own doc comment)", err)
	}

	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[OrderEvent, OrderResult](
		runInChildContextGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	// A fast, non-suspending happy path - no Wait/Callback cycle to poll
	// through, matching completion-config-go's/simple-step-go's own
	// choice for the identical reason.
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	event := OrderEvent{
		OrderID:              "order-cloud-1",
		Amount:               42.50,
		UseCustomChildSerdes: false,
	}

	res, err := runner.Run(ctx, "", event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	// Assert on the "payment" CONTEXT/RUN_IN_CHILD_CONTEXT operation:
	// must be SUCCEEDED, and must have exactly two SUCCEEDED nested STEP
	// children ("validate-amount", "charge-card") - proving
	// sdk_state_client.go's ParentID-based reconstruction of nested
	// operations from the real GetDurableExecutionHistory event log
	// works correctly for this example's genuine nesting, the first time
	// this has been exercised against a REAL deployed function rather
	// than only against fakes (cloud_runner_test.go) or against
	// completion-config-go's flatter CONTEXT/MAP_ITERATION shape.
	paymentOp, ok := res.GetOperation("payment")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'payment' in the real, cloud-polled operation log")
	}
	if paymentOp.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type, got %s", paymentOp.GetType())
	}
	if paymentOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected the 'payment' CONTEXT operation to report SUCCEEDED, got %s", paymentOp.GetStatus())
	}

	children := res.GetChildOperations(paymentOp.GetID())
	if len(children) != 2 {
		t.Fatalf("expected 2 nested STEP operations under 'payment', got %d", len(children))
	}
	childNames := map[string]bool{}
	for _, child := range children {
		if child.GetType() != types.OperationTypeStep {
			t.Fatalf("expected a nested STEP operation under 'payment', got type %s (name %s)", child.GetType(), child.GetName())
		}
		if child.GetStatus() != types.OperationStatusSucceeded {
			t.Fatalf("expected nested operation %q to report SUCCEEDED, got %s", child.GetName(), child.GetStatus())
		}
		childNames[child.GetName()] = true
	}
	if !childNames["validate-amount"] || !childNames["charge-card"] {
		t.Fatalf("expected nested STEP operations 'validate-amount' and 'charge-card' under 'payment', got %v", childNames)
	}

	// Assert on the sibling top-level "create-shipping-label" STEP,
	// outside the "payment" child context - proving a root-level STEP
	// alongside a CONTEXT also round-trips correctly.
	labelOp, ok := res.GetOperation("create-shipping-label")
	if !ok {
		t.Fatal("expected to find a top-level STEP operation named 'create-shipping-label'")
	}
	if labelOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'create-shipping-label' to report SUCCEEDED, got %s", labelOp.GetStatus())
	}

	// GetResult[OrderResult] (the top-level handler result) is
	// deliberately NOT asserted on here, for the same documented reason
	// completion-config-go's/simple-step-go's own cloud_integration_test.go
	// files don't: this session did not independently re-confirm whether
	// the top-level-handler-result gap documented in
	// sdk_state_client.go's GetExecutionResult doc applies to this
	// specific function/execution shape too. What IS confirmed reliable -
	// GetStatus() and the nested STEP/CONTEXT-level operation log,
	// including StepResult[T] on individual nested steps - is what this
	// test actually verifies, per the SAME scoping every prior cloud
	// integration test in this repo has used.
	chargeCardOp, ok := res.GetOperationRecursive("charge-card")
	if !ok {
		t.Fatal("expected to find a nested STEP operation named 'charge-card' via GetOperationRecursive")
	}
	receipt, err := dtesting.StepResult[string](chargeCardOp)
	if err != nil {
		t.Fatalf("StepResult[string] on 'charge-card': %v", err)
	}
	if receipt == "" {
		t.Fatal("expected a non-empty charge-card receipt string")
	}
}
