package durable_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// TestFakeClient_RejectsRetryWithBothErrorAndPayload is a direct
// regression test for a REAL bug confirmed via a live cloud deployment
// failure (examples/map-with-condition-and-callback-go invoked against a
// real Lambda function, account 730758745077, us-east-1 - see
// wait_for_condition.go's package-level bug writeup for the full
// investigation): the real Lambda Durable Functions backend rejects a
// RETRY-action OperationUpdate that carries both Error and Payload with
// "InvalidParameterValueException: Cannot provide both an Error and
// Payload for RETRY action."
//
// Before this fix, fakeClient (this file's own hand-rolled
// checkpoint.Client test double) applied any combination of fields
// unconditionally - it had NO notion of this constraint at all, which is
// exactly why wait_for_condition.go's own pre-fix RETRY checkpoint (which
// used to set both Payload AND Error) passed every local test in this
// repo yet failed immediately against the real backend. This test proves
// the gap is now closed: fakeClient itself enforces the constraint, so
// any future regression that reintroduces Error+Payload on a RETRY
// checkpoint - in wait_for_condition.go or anywhere else - will fail
// locally, without needing a real deployment to discover it.
func TestFakeClient_RejectsRetryWithBothErrorAndPayload(t *testing.T) {
	client := newFakeClient()
	payload := "some-state"
	req := types.CheckpointDurableExecutionRequest{
		DurableExecutionArn: "arn:test:1",
		CheckpointToken:     "token-0",
		Updates: []types.OperationUpdate{
			{
				ID:      "1",
				Type:    types.OperationTypeStep,
				SubType: "WAIT_FOR_CONDITION",
				Action:  types.OperationActionRetry,
				Payload: &payload,
				Error:   &types.OperationError{ErrorMessage: "condition not yet met"},
			},
		},
	}

	_, err := client.Checkpoint(context.Background(), req)
	if err == nil {
		t.Fatal("expected fakeClient.Checkpoint to reject a RETRY update carrying both Error and Payload, got nil error")
	}
	if !strings.Contains(err.Error(), "Cannot provide both an Error and Payload for RETRY action") {
		t.Fatalf("expected error to mention the real backend's exact rejection message, got: %v", err)
	}
}

// TestFakeClient_AllowsRetryWithPayloadOnly confirms the fix's OTHER
// side: a RETRY update carrying ONLY Payload (no Error) - exactly what
// wait_for_condition.go's pollAndCheckpoint now sends, matching all three
// reference SDKs' own confirmed behavior (see that file's RETRY-checkpoint
// comment) - is still accepted and correctly persists the state for the
// next invocation's replay-read to recover.
func TestFakeClient_AllowsRetryWithPayloadOnly(t *testing.T) {
	client := newFakeClient()
	payload := "some-state"
	req := types.CheckpointDurableExecutionRequest{
		DurableExecutionArn: "arn:test:1",
		CheckpointToken:     "token-0",
		Updates: []types.OperationUpdate{
			{
				ID:      "1",
				Type:    types.OperationTypeStep,
				SubType: "WAIT_FOR_CONDITION",
				Action:  types.OperationActionRetry,
				Payload: &payload,
			},
		},
	}

	resp, err := client.Checkpoint(context.Background(), req)
	if err != nil {
		t.Fatalf("expected a Payload-only RETRY to be accepted, got error: %v", err)
	}
	if len(resp.UpdatedOperations) != 1 {
		t.Fatalf("expected exactly 1 updated operation, got %d", len(resp.UpdatedOperations))
	}
	op := resp.UpdatedOperations[0]
	if op.StepDetails == nil || op.StepDetails.Result == nil || *op.StepDetails.Result != payload {
		t.Fatalf("expected Payload to be persisted into StepDetails.Result, got %+v", op.StepDetails)
	}
}

// TestFakeClient_AllowsRetryWithErrorOnly confirms plain Step's own RETRY
// shape (Error only, no Payload - step.go's retryOrFail) is unaffected by
// this new constraint.
func TestFakeClient_AllowsRetryWithErrorOnly(t *testing.T) {
	client := newFakeClient()
	req := types.CheckpointDurableExecutionRequest{
		DurableExecutionArn: "arn:test:1",
		CheckpointToken:     "token-0",
		Updates: []types.OperationUpdate{
			{
				ID:     "1",
				Type:   types.OperationTypeStep,
				Action: types.OperationActionRetry,
				Error:  &types.OperationError{ErrorMessage: "transient failure"},
			},
		},
	}

	resp, err := client.Checkpoint(context.Background(), req)
	if err != nil {
		t.Fatalf("expected an Error-only RETRY to be accepted, got error: %v", err)
	}
	if len(resp.UpdatedOperations) != 1 {
		t.Fatalf("expected exactly 1 updated operation, got %d", len(resp.UpdatedOperations))
	}
	op := resp.UpdatedOperations[0]
	if op.StepDetails == nil || op.StepDetails.Error == nil || op.StepDetails.Error.ErrorMessage != "transient failure" {
		t.Fatalf("expected Error to be persisted into StepDetails.Error, got %+v", op.StepDetails)
	}
}
