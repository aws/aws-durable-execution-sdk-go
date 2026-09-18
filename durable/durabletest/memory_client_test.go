// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// stepUpdate builds a step checkpoint update for the in-memory client tests.
func stepUpdate(id string, action durable.OperationAction, payload *string) durable.OperationUpdate {
	return durable.OperationUpdate{
		Id:      aws.String(id),
		Name:    aws.String("step-" + id),
		Type:    durable.OperationTypeStep,
		Action:  action,
		Payload: payload,
	}
}

// checkpointOne applies a single update and returns the stored operation.
func checkpointOne(t *testing.T, m *memoryClient, u durable.OperationUpdate) durable.Operation {
	t.Helper()
	out, err := m.Checkpoint(context.Background(), durable.CheckpointInput{
		CheckpointToken: m.currentToken(),
		Updates:         []durable.OperationUpdate{u},
	})
	if err != nil {
		t.Fatalf("Checkpoint(%s): %v", u.Action, err)
	}
	if len(out.NewExecutionState) != 1 {
		t.Fatalf("Checkpoint(%s) returned %d operations, want 1", u.Action, len(out.NewExecutionState))
	}
	return out.NewExecutionState[0]
}

// TestMemoryClientRetryStoresPayload asserts that the stored step details
// after a RETRY carry the payload the checkpoint update carried, so a
// polling step reads its intermediate state back on the next attempt.
func TestMemoryClientRetryStoresPayload(t *testing.T) {
	m := newMemoryClient()

	checkpointOne(t, m, stepUpdate("op-1", durable.OperationActionStart, nil))

	retry := stepUpdate("op-1", durable.OperationActionRetry, aws.String(`{"count":1}`))
	retry.StepOptions = &durable.StepOptions{NextAttemptDelaySeconds: aws.Int32(1)}
	got := checkpointOne(t, m, retry)

	if got.Status != durable.OperationStatusPending {
		t.Errorf("status after RETRY = %s, want %s", got.Status, durable.OperationStatusPending)
	}
	if got.StepDetails == nil {
		t.Fatal("StepDetails after RETRY is nil")
	}
	if got.StepDetails.Attempt != 1 {
		t.Errorf("Attempt after RETRY = %d, want 1", got.StepDetails.Attempt)
	}
	if got.StepDetails.Result == nil || *got.StepDetails.Result != *retry.Payload {
		t.Errorf("Result after RETRY = %v, want %q", got.StepDetails.Result, *retry.Payload)
	}

	// The stored operation, as the next invocation reads it through the
	// execution state, must carry the same payload.
	stored := m.findByName("step-op-1")
	if stored == nil || stored.StepDetails == nil || stored.StepDetails.Result == nil {
		t.Fatalf("stored operation after RETRY = %+v, want StepDetails.Result set", stored)
	}
	if *stored.StepDetails.Result != *retry.Payload {
		t.Errorf("stored Result after RETRY = %q, want %q", *stored.StepDetails.Result, *retry.Payload)
	}

	// A second RETRY replaces the payload and increments the attempt.
	retry2 := stepUpdate("op-1", durable.OperationActionRetry, aws.String(`{"count":2}`))
	got = checkpointOne(t, m, retry2)
	if got.StepDetails.Attempt != 2 {
		t.Errorf("Attempt after second RETRY = %d, want 2", got.StepDetails.Attempt)
	}
	if got.StepDetails.Result == nil || *got.StepDetails.Result != *retry2.Payload {
		t.Errorf("Result after second RETRY = %v, want %q", got.StepDetails.Result, *retry2.Payload)
	}
}

// TestMemoryClientStartStoresPayload asserts that a START update with a
// payload stores that payload as the step result.
func TestMemoryClientStartStoresPayload(t *testing.T) {
	m := newMemoryClient()

	start := stepUpdate("op-1", durable.OperationActionStart, aws.String(`"seed"`))
	got := checkpointOne(t, m, start)

	if got.Status != durable.OperationStatusStarted {
		t.Errorf("status after START = %s, want %s", got.Status, durable.OperationStatusStarted)
	}
	if got.StepDetails == nil {
		t.Fatal("StepDetails after START is nil")
	}
	if got.StepDetails.Attempt != 0 {
		t.Errorf("Attempt after START = %d, want 0", got.StepDetails.Attempt)
	}
	if got.StepDetails.Result == nil || *got.StepDetails.Result != *start.Payload {
		t.Errorf("Result after START = %v, want %q", got.StepDetails.Result, *start.Payload)
	}
}

// TestMemoryClientStartWithoutPayloadHasNoResult asserts that a START
// update without a payload leaves the step result unset.
func TestMemoryClientStartWithoutPayloadHasNoResult(t *testing.T) {
	m := newMemoryClient()

	got := checkpointOne(t, m, stepUpdate("op-1", durable.OperationActionStart, nil))
	if got.StepDetails == nil {
		t.Fatal("StepDetails after START is nil")
	}
	if got.StepDetails.Result != nil {
		t.Errorf("Result after START without payload = %q, want nil", *got.StepDetails.Result)
	}
}
