//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to examples/wait-for-callback-nested-go. Gated
// behind the cloudintegration build tag; makes REAL, billed,
// ASYNCHRONOUS Lambda Invoke(s) against a REAL deployed function in
// account 730758745077 (region us-east-1). Not wired into any CI
// workflow.
//
// This example has a PARENT callback, then (inside a RunInChildContext)
// a CHILD callback - RunUntilCallback is called twice: once with an
// empty arn (fresh invocation) for the parent, once with the resolved
// arn (see examples/wait-for-callback-multiple-invocations-go/cloud_integration_test.go's
// own doc for why a non-empty arn lets RunUntilCallback poll an
// ALREADY-RUNNING execution for the NEXT named callback, rather than
// starting a second, unrelated execution) for the child.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

const waitForCallbackNestedGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:wait-for-callback-nested-go-example:1"

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

	runner := dtesting.NewCloudTestRunner[NestedCallbackEvent, NestedCallbackResult](
		waitForCallbackNestedGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	event := NestedCallbackEvent{RequestID: "cloud-1"}

	// Parent callback (checkpointed as "parent-callback-callback" - see
	// callback.go's WaitForCallback, which suffixes the caller-supplied
	// id with "-callback" for the actual CALLBACK-type operation).
	parentPending, arn, err := runner.RunUntilCallback(ctx, "", event, "parent-callback-callback")
	if err != nil {
		t.Fatalf("RunUntilCallback (parent): %v", err)
	}
	if parentPending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := parentPending.GetError()
		t.Fatalf("expected PENDING after the parent callback, got %s (%s)", parentPending.GetStatus(), msg)
	}
	parentCallback, ok := parentPending.GetOperationRecursive("parent-callback-callback")
	if !ok {
		t.Fatal("expected to find 'parent-callback-callback'")
	}
	if err := parentCallback.SendCallbackSuccess("parent-done"); err != nil {
		t.Fatalf("SendCallbackSuccess (parent): %v", err)
	}

	// Child callback, on the SAME execution (non-empty arn).
	childPending, _, err := runner.RunUntilCallback(ctx, arn, event, "child-callback-callback")
	if err != nil {
		t.Fatalf("RunUntilCallback (child): %v", err)
	}
	if childPending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := childPending.GetError()
		t.Fatalf("expected PENDING after the child callback, got %s (%s)", childPending.GetStatus(), msg)
	}
	childCallback, ok := childPending.GetOperationRecursive("child-callback-callback")
	if !ok {
		t.Fatal("expected to find 'child-callback-callback'")
	}
	if err := childCallback.SendCallbackSuccess("child-done"); err != nil {
		t.Fatalf("SendCallbackSuccess (child): %v", err)
	}

	final, err := runner.Continue(ctx, arn)
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", final.GetStatus(), msg)
	}
}
