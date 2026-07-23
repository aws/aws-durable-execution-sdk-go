//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to examples/error-handling-go. Gated behind the
// cloudintegration build tag; makes a REAL, billed, synchronous Lambda
// Invoke against a REAL deployed function in account 730758745077
// (region us-east-1). Not wired into any CI workflow.
//
// Runs the SUCCESS path (AlwaysFails: false), not the AlwaysFails: true
// scenario - matching every other cloud test in this repo's own
// preference for "a fast, non-suspending happy path" as the first cloud
// verification for a newly-wired example; the failure path's own
// multi-attempt StepFailedError.Attempt assertions are already covered
// by handler_test.go against LocalTestRunner.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// errorHandlingGoExampleFunctionARN is the real, deployed function's
// qualified version-1 ARN, confirmed live via a real smoke-test Invoke
// immediately before writing this test.
const errorHandlingGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:error-handling-go-example:1"

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

	runner := dtesting.NewCloudTestRunner[ChargeCardEvent, ChargeCardResult](
		errorHandlingGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	event := ChargeCardEvent{OrderID: "cloud-1", Amount: 42.00, AlwaysFails: false}

	res, err := runner.Run(ctx, "", event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	chargeOp, ok := res.GetOperation("charge-card")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'charge-card' in the real, cloud-polled operation log")
	}
	if chargeOp.GetType() != types.OperationTypeStep {
		t.Fatalf("expected STEP type, got %s", chargeOp.GetType())
	}
	if chargeOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'charge-card' SUCCEEDED, got %s", chargeOp.GetStatus())
	}
}
