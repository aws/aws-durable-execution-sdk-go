// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner across THREE invocations - the parent callback
// resolves first, then the child-context callback resolves separately,
// each requiring its own Continue/SendCallbackSuccess round-trip.
package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestHandler_ResolvesParentThenChildCallback(t *testing.T) {
	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:wait-for-callback-nested-test:1"

	// First invocation: reaches the PARENT callback and suspends.
	first, err := runner.Continue(arn, NestedCallbackEvent{RequestID: "req-001"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if first.GetStatus() != types.ExecutionStatusPending {
		msg, _ := first.GetError()
		t.Fatalf("expected PENDING after the parent callback, got %s (%s)", first.GetStatus(), msg)
	}
	parentCallback, ok := first.GetOperation("parent-callback-callback")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'parent-callback-callback'")
	}
	if err := parentCallback.SendCallbackSuccess("parent-done"); err != nil {
		t.Fatalf("SendCallbackSuccess (parent): %v", err)
	}

	// Second invocation: parent callback resolved, handler proceeds into
	// the child context, reaches the CHILD callback, and suspends again.
	second, err := runner.Continue(arn, NestedCallbackEvent{RequestID: "req-001"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if second.GetStatus() != types.ExecutionStatusPending {
		msg, _ := second.GetError()
		t.Fatalf("expected PENDING after the child callback, got %s (%s)", second.GetStatus(), msg)
	}
	childCallback, ok := second.GetOperationRecursive("child-callback-callback")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'child-callback-callback' inside the child context")
	}
	if err := childCallback.SendCallbackSuccess("child-done"); err != nil {
		t.Fatalf("SendCallbackSuccess (child): %v", err)
	}

	// Third invocation: both callbacks resolved, handler completes.
	final, err := runner.Continue(arn, NestedCallbackEvent{RequestID: "req-001"})
	if err != nil {
		t.Fatalf("third Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", final.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[NestedCallbackResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.ParentResult != "parent-done" {
		t.Fatalf("expected ParentResult 'parent-done', got %q", out.ParentResult)
	}
	if out.ChildResult != "child-done" {
		t.Fatalf("expected ChildResult 'child-done', got %q", out.ChildResult)
	}
	if !out.ChildFinished {
		t.Fatal("expected ChildFinished=true")
	}

	dtesting.AssertEventSignatures(t, final, "testdata/TestHandler_ResolvesParentThenChildCallback.history.json")
}
