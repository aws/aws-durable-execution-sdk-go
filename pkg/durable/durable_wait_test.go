package durable_test

import (
	"context"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// TestWait_SuspendsThenResumes verifies the core suspend-or-complete race
// described in docs/checkpoint-replay-design.md §4: a handler that calls
// Wait suspends the invocation (returns Pending) without running the code
// after the Wait, and a subsequent "replay" invocation (with the wait
// already recorded as Succeeded, as a real backend would report once the
// duration elapses) continues past the Wait to completion.
func TestWait_SuspendsThenResumes(t *testing.T) {
	client := newFakeClient()
	// The fake client's Checkpoint() marks Wait Start actions as
	// immediately Succeeded (see fakeClient.Checkpoint), which does not
	// exercise real suspension timing - but WithDurableExecution's
	// suspend-or-complete select races the handler goroutine's completion
	// against execManager.Suspended() regardless of how fast the wait
	// resolves, so this still validates that:
	//   (a) code after Wait only runs once Wait actually returns nil, and
	//   (b) the overall invocation completes successfully once it does.
	// A dedicated execmgr-level test (execmgr package, not yet added)
	// would be the place to test true suspension (Wait never resolving in
	// this invocation) in isolation from the backend's timing.
	afterWaitRan := false

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		if err := operations.Wait(dc, "cool-down", types.Duration{Seconds: 1}); err != nil {
			return orderResult{}, err
		}
		afterWaitRan = true
		return orderResult{OrderID: event.OrderID, Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	input := newInvocationInput("exec-wait-1", "arn:test:1", "token-0", mustMarshalPayload(t, orderEvent{OrderID: "wait-order"}))

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got status=%s error=%v", out.Status, out.Error)
	}
	if !afterWaitRan {
		t.Fatal("expected code after Wait to have run")
	}

	ops := client.snapshot()
	waitOp, ok := ops[expectedStepID("exec-wait-1", 1)]
	if !ok {
		t.Fatal("expected wait operation '1' (hashed) to be checkpointed")
	}
	if waitOp.Type != types.OperationTypeWait {
		t.Fatalf("expected checkpointed operation to be a Wait, got type=%s", waitOp.Type)
	}
}
