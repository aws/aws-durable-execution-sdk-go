//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to examples/wait-for-callback-heartbeat-go.
// Gated behind the cloudintegration build tag; makes a REAL, billed,
// ASYNCHRONOUS Lambda Invoke against a REAL deployed function in
// account 730758745077 (region us-east-1), followed by a real
// SendDurableExecutionCallbackSuccess call from this test itself. Not
// wired into any CI workflow. See
// examples/create-callback-go/cloud_integration_test.go's own doc for
// the full RunUntilCallback/Continue mechanism this reuses.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

const waitForCallbackHeartbeatGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:wait-for-callback-heartbeat-go-example:1"

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

	runner := dtesting.NewCloudTestRunner[HeartbeatEvent, HeartbeatResult](
		waitForCallbackHeartbeatGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	pending, arn, err := runner.RunUntilCallback(ctx, "", HeartbeatEvent{RequestID: "cloud-1"}, "long-running-task-callback-callback")
	if err != nil {
		t.Fatalf("RunUntilCallback: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING, got %s (%s)", pending.GetStatus(), msg)
	}
	if arn == "" {
		t.Fatal("expected a non-empty resolved execution ARN")
	}

	callbackOp, ok := pending.GetOperationRecursive("long-running-task-callback-callback")
	if !ok {
		t.Fatal("expected to find a nested CALLBACK operation named 'long-running-task-callback-callback'")
	}
	callbackDetails := callbackOp.GetCallbackDetails()
	if callbackDetails == nil || callbackDetails.CallbackID == "" {
		t.Fatal("expected a non-empty, real backend-issued CallbackID")
	}

	if err := callbackOp.SendCallbackSuccess("done"); err != nil {
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
}
