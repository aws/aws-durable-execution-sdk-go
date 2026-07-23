// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner across THREE invocations: SkipTime fast-forwards both
// Wait calls synchronously, but each of the two WaitForCallback calls
// requires its own explicit SendCallbackSuccess round-trip - proving
// this SDK's own checkpoint/replay tracking correctly resumes exactly
// where each prior invocation left off, twice in a row within one
// handler.
package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestHandler_TracksAcrossMultipleSuspendResumeCycles(t *testing.T) {
	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:multi-invocation-test:1"

	// First invocation: both Wait calls resolve synchronously (SkipTime),
	// the handler reaches the FIRST callback and suspends there.
	first, err := runner.Continue(arn, MultiInvocationEvent{RequestID: "req-001"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if first.GetStatus() != types.ExecutionStatusPending {
		msg, _ := first.GetError()
		t.Fatalf("expected PENDING after the first callback, got %s (%s)", first.GetStatus(), msg)
	}
	waitOp1, ok := first.GetOperation("wait-invocation-1")
	if !ok {
		t.Fatal("expected to find WAIT operation 'wait-invocation-1'")
	}
	if waitOp1.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'wait-invocation-1' SUCCEEDED (SkipTime), got %s", waitOp1.GetStatus())
	}
	firstCallback, ok := first.GetOperation("first-callback-callback")
	if !ok {
		t.Fatal("expected to find CALLBACK operation 'first-callback-callback'")
	}
	if err := firstCallback.SendCallbackSuccess("first-done"); err != nil {
		t.Fatalf("SendCallbackSuccess (first): %v", err)
	}

	// Second invocation: first callback resolved, handler proceeds
	// through the Step and second Wait, reaches the SECOND callback, and
	// suspends again.
	second, err := runner.Continue(arn, MultiInvocationEvent{RequestID: "req-001"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if second.GetStatus() != types.ExecutionStatusPending {
		msg, _ := second.GetError()
		t.Fatalf("expected PENDING after the second callback, got %s (%s)", second.GetStatus(), msg)
	}
	stepOp, ok := second.GetOperation("process-callback-data")
	if !ok {
		t.Fatal("expected to find STEP operation 'process-callback-data'")
	}
	if stepOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'process-callback-data' SUCCEEDED, got %s", stepOp.GetStatus())
	}
	waitOp2, ok := second.GetOperation("wait-invocation-2")
	if !ok {
		t.Fatal("expected to find WAIT operation 'wait-invocation-2'")
	}
	if waitOp2.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'wait-invocation-2' SUCCEEDED (SkipTime), got %s", waitOp2.GetStatus())
	}
	secondCallback, ok := second.GetOperation("second-callback-callback")
	if !ok {
		t.Fatal("expected to find CALLBACK operation 'second-callback-callback'")
	}
	if err := secondCallback.SendCallbackSuccess("second-done"); err != nil {
		t.Fatalf("SendCallbackSuccess (second): %v", err)
	}

	// Third invocation: both callbacks resolved, handler completes.
	final, err := runner.Continue(arn, MultiInvocationEvent{RequestID: "req-001"})
	if err != nil {
		t.Fatalf("third Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", final.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[MultiInvocationResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.FirstCallback != "first-done" {
		t.Fatalf("expected FirstCallback 'first-done', got %q", out.FirstCallback)
	}
	if out.SecondCallback != "second-done" {
		t.Fatalf("expected SecondCallback 'second-done', got %q", out.SecondCallback)
	}
	if !out.StepProcessed {
		t.Fatal("expected StepProcessed=true")
	}
	if out.InvocationCount != "multiple" {
		t.Fatalf("expected InvocationCount 'multiple', got %q", out.InvocationCount)
	}

	dtesting.AssertEventSignatures(t, final, "testdata/TestHandler_TracksAcrossMultipleSuspendResumeCycles.history.json")
}
