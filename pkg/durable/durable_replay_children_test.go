package durable_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// TestRunInChildContext_OversizedResult_UsesReplayChildren verifies the
// real, real-backend-confirmed ReplayChildren protocol (conformance
// suite requirement 3-11, "Child context large payload (ReplayChildren
// mode)"; see operations/invoke.go's RunInChildContext doc for the full
// writeup): a child context whose real result serializes larger than the
// single-operation checkpoint threshold no longer fails outright with a
// client-side *ResultTooLargeError (the pre-existing behavior for every
// OTHER operation kind, and RunInChildContext's own prior behavior before
// this fix) - instead it checkpoints ContextSucceeded with an EMPTY
// Payload and ContextOptions.ReplayChildren=true, returns the real
// in-memory result to THIS invocation directly, and on any LATER
// invocation that replays past this now-SUCCEEDED context, re-executes
// the child context's own function body again (not a replay-skip) to
// reconstruct the result purely in memory.
//
// This test drives both halves in one go, exactly mirroring
// TestStep_RetryDelaySuspendsThenResumes's own two-invocation pattern:
// the FIRST invocation both produces the large result AND, in the same
// invocation, must already return it correctly (RunInChildContext's own
// caller within that same call never needed anything from a later
// invocation - only the CHECKPOINT is deferred/stripped, not the actual
// return value). The SECOND invocation, seeded with the first's own
// checkpointed operations (simulating a fresh Lambda invocation that
// replays past this same execution's history, e.g. because a later Wait
// or Step elsewhere in the handler suspended), must independently
// reconstruct the identical large result by re-running the child
// context's function body - confirmed by counting how many times that
// body actually executes (expected: exactly 2, once per invocation, not
// 1 - a replay-skip would incorrectly leave it at 1 with a wrong/empty
// result).
func TestRunInChildContext_OversizedResult_UsesReplayChildren(t *testing.T) {
	client := newFakeClient()
	childBodyExecutions := 0

	// One byte over checkpoint.DefaultLimits().MaxPayloadBytes (750KB)
	// once JSON-encoded - the same real, confirmed threshold
	// TestStep_OversizedResult_FailsClearly already uses for Step's own
	// (unaffected, still-rejecting) oversized-result case.
	const largeSize = 800 * 1024

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		result, err := operations.RunInChildContext(dc, "big-child", func(child types.DurableContext) (string, error) {
			childBodyExecutions++
			// The inner Step's own result stays small and deliberately
			// checkpoints normally - conformance requirement 3-11's own
			// ExpectedExecutionHistory confirms the inner Step's
			// StepStarted/StepSucceeded events appear EXACTLY ONCE, not
			// twice, because that Step's own checkpoint is independently
			// already SUCCEEDED and correctly replay-skips when this
			// child body re-executes on the second invocation - only the
			// OUTER child context's own body (this function) re-runs.
			small, stepErr := operations.Step(child, "small-step", func(sc types.StepContext) (string, error) {
				return "ok", nil
			})
			if stepErr != nil {
				return "", stepErr
			}
			// Build the large result AFTER the step, matching 3-11's own
			// "step returns a small value, but the child context
			// function builds a large result from it" scenario shape.
			return small + strings.Repeat("x", largeSize), nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	eventPayload := mustMarshalPayload(t, orderEvent{OrderID: "big"})

	// First invocation: the child context's real body runs, produces the
	// oversized result, and this invocation must receive that REAL result
	// directly - not an error, and not a suspended/pending status.
	firstInput := newInvocationInput("exec-replay-children-1", "arn:test:1", "token-0", eventPayload)
	out, err := entry(context.Background(), firstInput)
	if err != nil {
		t.Fatalf("unexpected transport-level error on first invocation: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		errMsg := "<nil>"
		if out.Error != nil {
			errMsg = out.Error.ErrorMessage
		}
		t.Fatalf("expected Succeeded on first invocation (the real result should be returned directly, not rejected), got status=%s error=%s", out.Status, errMsg)
	}
	if childBodyExecutions != 1 {
		t.Fatalf("expected the child context body to run exactly once on the first invocation, got %d", childBodyExecutions)
	}
	if out.ResultPayload == nil || !strings.Contains(*out.ResultPayload, strings.Repeat("x", 100)) {
		t.Fatal("expected the first invocation's own result to contain the real, large payload - it was computed in memory this same invocation and should never have been rejected or truncated")
	}

	// Confirm the checkpointed operation on record shows the
	// ReplayChildren protocol's own wire shape: Succeeded status, but an
	// EMPTY Result (not the large payload - checkResultSize's original
	// unconditional rejection is gone, but the checkpoint itself must
	// still never durably persist the oversized bytes).
	snapshot := client.snapshot()
	var childOp *types.Operation
	for id, op := range snapshot {
		if op.Name == "big-child" {
			o := op
			childOp = &o
			_ = id
		}
	}
	if childOp == nil {
		t.Fatal("expected the child context operation 'big-child' to be checkpointed")
	}
	if childOp.Status != types.OperationStatusSucceeded {
		t.Fatalf("expected the checkpointed child context to be Succeeded (not Failed - the whole point of ReplayChildren is this no longer fails), got %s", childOp.Status)
	}
	if childOp.ContextDetails == nil || !childOp.ContextDetails.ReplayChildren {
		t.Fatalf("expected ContextDetails.ReplayChildren=true on the checkpointed operation, got %+v", childOp.ContextDetails)
	}
	if childOp.ContextDetails.Result != nil && *childOp.ContextDetails.Result != "" {
		t.Fatalf("expected an EMPTY checkpointed Result (the large payload must never be durably persisted), got %d bytes", len(*childOp.ContextDetails.Result))
	}

	// Second invocation ("replay" of the same execution, e.g. because
	// some LATER operation in the handler suspended and this is a fresh
	// Lambda invocation resuming past this already-completed child
	// context): seed InitialExecutionState with every operation
	// checkpointed so far, exactly as a real re-invocation would receive
	// them, and confirm the child context's body re-executes (NOT a
	// replay-skip) to reconstruct the identical large result.
	var extraOps []types.Operation
	for _, op := range snapshot {
		extraOps = append(extraOps, op)
	}
	secondInput := newInvocationInput("exec-replay-children-1", "arn:test:1", "token-1", eventPayload, extraOps...)

	out2, err := entry(context.Background(), secondInput)
	if err != nil {
		t.Fatalf("unexpected transport-level error on second invocation: %v", err)
	}
	if out2.Status != types.ExecutionStatusSucceeded {
		errMsg := "<nil>"
		if out2.Error != nil {
			errMsg = out2.Error.ErrorMessage
		}
		t.Fatalf("expected Succeeded on second invocation (ReplayChildren re-execution should reconstruct the result), got status=%s error=%s", out2.Status, errMsg)
	}
	if childBodyExecutions != 2 {
		t.Fatalf("expected the child context body to run exactly TWICE in total (once per invocation - the second invocation must re-execute it, not replay-skip), got %d", childBodyExecutions)
	}
	if out2.ResultPayload == nil || *out2.ResultPayload != *out.ResultPayload {
		t.Fatalf("expected the second invocation's reconstructed result to exactly match the first invocation's real result")
	}
}
