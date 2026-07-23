//go:build cloudintegration

// cloud_integration_test.go extends docs/remaining-work.md's §7 task
// 17b's CloudTestRunner wiring to examples/parallel-processing-go,
// following the exact same pattern established by
// examples/completion-config-go/cloud_integration_test.go (see that
// file's own extensive header doc comment for the full rationale on the
// `//go:build cloudintegration` gating and why this is NOT wired into
// CI - not re-litigated here).
//
// # Why the all-succeed scenario
//
// handler.go's own doc identifies exactly one interesting failure mode
// (errFraudCheckFailed, triggered by Amount >= 10000 - see
// TestHandler_FraudCheckFailsWholeBatch for the local-runner coverage of
// that path) alongside the normal all-four-branches-succeed path. This
// task's own instructions ask for "the all-succeed scenario" here
// explicitly, which is also this example's fast, non-suspending happy
// path (all four operations.All branches are plain Steps with no
// suspension of any kind) - so both the letter of the instruction and
// this task's general fast-happy-path preference point the same way.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// parallelProcessingGoExampleFunctionARN is the REAL, already-deployed,
// currently-active target this test invokes - the latest published
// qualified version ARN, confirmed via a read-only
// `lambda:ListVersionsByFunction` check immediately before writing this
// test: version 1's CodeSha256
// (a24c198c516707952880d5b1b966928f2154dd00660b8213ca74e82424429865)
// matches $LATEST's, confirming version 1 is the function's current
// (and, so far, only-ever-published) code. Per
// CloudTestRunner.FunctionName's own doc comment, this MUST be a
// qualified ARN.
const parallelProcessingGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:parallel-processing-go-example:1"

// TestCloudTestRunner_AllChecksSucceed_RealDeployedFunction runs the
// all-succeed scenario (an order amount safely under the fraud
// threshold) against the real deployed parallel-processing-go-example
// function via testing.CloudTestRunner: four independent branches
// (fraud-check, inventory-check, address-validation,
// payment-authorization) fanning out via a single operations.All call,
// all expected to succeed.
func TestCloudTestRunner_AllChecksSucceed_RealDeployedFunction(t *testing.T) {
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
		parallelProcessingGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	event := OrderEvent{
		OrderID: "order-cloud-1",
		SKU:     "sku-widget-1",
		Amount:  250.00, // well under the 10000 fraud threshold - all branches succeed
	}

	res, err := runner.Run(ctx, "", event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	// Assert on the "order-checks" CONTEXT/PARALLEL operation: SUCCEEDED,
	// with exactly 4 nested PARALLEL_BRANCH children, all succeeded -
	// proving a wider (4-branch, vs. map-parallel-go's 2-branch) Parallel
	// fan-out reconstructs correctly too.
	checksOp, ok := res.GetOperation("order-checks")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'order-checks'")
	}
	if checksOp.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type for 'order-checks', got %s", checksOp.GetType())
	}
	if checksOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'order-checks' to report SUCCEEDED, got %s", checksOp.GetStatus())
	}

	branches := res.GetChildOperations(checksOp.GetID())
	if len(branches) != 4 {
		t.Fatalf("expected 4 PARALLEL_BRANCH children under 'order-checks', got %d", len(branches))
	}
	for _, branch := range branches {
		if branch.GetStatus() != types.OperationStatusSucceeded {
			t.Fatalf("expected PARALLEL_BRANCH %q to report SUCCEEDED, got %s", branch.GetID(), branch.GetStatus())
		}
	}

	// Cross-check all four expected nested STEP names are present via
	// GetOperationRecursive, confirming each branch's own nested Step
	// (not just the branch CONTEXT wrapper) round-trips correctly.
	for _, stepName := range []string{"fraud-check", "inventory-check", "address-validation", "payment-authorization"} {
		op, ok := res.GetOperationRecursive(stepName)
		if !ok {
			t.Fatalf("expected to find a nested STEP operation named %q via GetOperationRecursive", stepName)
		}
		if op.GetStatus() != types.OperationStatusSucceeded {
			t.Fatalf("expected nested STEP %q to report SUCCEEDED, got %s", stepName, op.GetStatus())
		}
		result, err := dtesting.StepResult[bool](op)
		if err != nil {
			t.Fatalf("StepResult[bool] on %q: %v", stepName, err)
		}
		if !result {
			t.Fatalf("expected nested STEP %q's checkpointed result to be true, got false", stepName)
		}
	}

	// GetResult[OrderResult] (top-level handler result) is deliberately
	// NOT asserted on, per the same documented,
	// not-independently-re-confirmed-for-this-function gap every prior
	// cloud_integration_test.go in this repo defers on.
}
