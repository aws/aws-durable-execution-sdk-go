//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to examples/wait-for-condition-go. Gated behind
// the cloudintegration build tag; makes a REAL, billed, synchronous
// Lambda Invoke against a REAL deployed function in account
// 730758745077 (region us-east-1). Not wired into any CI workflow.
//
// A synchronous Invoke blocks for the whole 3-poll/5-second-interval
// WaitForCondition cycle (>=10s real wall time) - a longer Timeout than
// the fast single-Step examples' own cloud tests, but still a single Run
// call (no RunUntilCallback/suspension-driving needed, since nothing
// EXTERNAL to the deployed function needs to act - each poll's own
// "condition not yet met" retry-delay suspension is entirely internal to
// the backend's own retry mechanism, resolved automatically once its own
// timer elapses).
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// waitForConditionGoExampleFunctionARN is the real, deployed function's
// qualified version-1 ARN, confirmed live via a real smoke-test Invoke
// immediately before writing this test.
const waitForConditionGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:wait-for-condition-go-example:1"

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

	runner := dtesting.NewCloudTestRunner[JobEvent, JobResult](
		waitForConditionGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 5 * time.Second
	runner.Timeout = 3 * time.Minute

	res, err := runner.Run(ctx, "", JobEvent{JobID: "cloud-job-1"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	pollOp, ok := res.GetOperation("poll-job-status")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'poll-job-status' in the real, cloud-polled operation log")
	}
	if pollOp.GetType() != types.OperationTypeStep {
		t.Fatalf("expected STEP type (WaitForCondition checkpoints as STEP/WAIT_FOR_CONDITION), got %s", pollOp.GetType())
	}
	if pollOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'poll-job-status' SUCCEEDED, got %s", pollOp.GetStatus())
	}
}
