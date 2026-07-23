//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to examples/wait-for-callback-serdes-go. Gated
// behind the cloudintegration build tag; makes a REAL, billed,
// ASYNCHRONOUS Lambda Invoke against a REAL deployed function in
// account 730758745077 (region us-east-1). Not wired into any CI
// workflow.
//
// Like handler_test.go's own local test, this sends SendCallbackSuccess
// a upperKeyWire value DIRECTLY (whose own JSON tags already spell the
// custom serdes's exact wire shape) rather than a completionData value -
// see handler_test.go's own top-level doc comment for why this
// genuinely exercises upperKeySerdes.Deserialize despite
// testing.Operation.SendCallbackSuccess having no raw-string injection
// support.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

const waitForCallbackSerdesGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:wait-for-callback-serdes-go-example:1"

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

	runner := dtesting.NewCloudTestRunner[TaskEvent, TaskResult](
		waitForCallbackSerdesGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	pending, arn, err := runner.RunUntilCallback(ctx, "", TaskEvent{RequestID: "cloud-1"}, "custom-serdes-callback-callback")
	if err != nil {
		t.Fatalf("RunUntilCallback: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING, got %s (%s)", pending.GetStatus(), msg)
	}

	callbackOp, ok := pending.GetOperationRecursive("custom-serdes-callback-callback")
	if !ok {
		t.Fatal("expected to find 'custom-serdes-callback-callback'")
	}

	completedAt := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	if err := callbackOp.SendCallbackSuccess(upperKeyWire{MESSAGE: "task complete", COMPLETEDAT: completedAt}); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	final, err := runner.Continue(ctx, arn)
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", final.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[TaskResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Message != "task complete" {
		t.Fatalf("expected Message 'task complete' (proves upperKeySerdes.Deserialize correctly ran against the REAL backend), got %q", out.Message)
	}
	if !out.CompletedAtDate {
		t.Fatal("expected CompletedAtIsDate=true")
	}

	finalCallback, ok := final.GetOperationRecursive("custom-serdes-callback-callback")
	if !ok {
		t.Fatal("expected to find the resolved CALLBACK operation in the final result")
	}
	callbackDetails := finalCallback.GetCallbackDetails()
	if callbackDetails == nil || callbackDetails.Result == nil {
		t.Fatal("expected the checkpointed CALLBACK operation to carry a Result payload")
	}
	payload := *callbackDetails.Result
	if !containsSubstring(payload, `"MESSAGE"`) {
		t.Fatalf("expected the checkpointed payload to use the custom UPPERCASE key format against the REAL backend, got %q", payload)
	}
}
