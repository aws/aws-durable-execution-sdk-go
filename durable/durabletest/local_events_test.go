// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func checkpoint(t *testing.T, m *memoryClient, updates ...durable.OperationUpdate) {
	t.Helper()
	if _, err := m.Checkpoint(context.Background(), durable.CheckpointInput{Updates: updates}); err != nil {
		t.Fatal(err)
	}
}

// TestLocalEventsStepRetry asserts that a step's retry checkpoint records a
// StepFailed event carrying the attempt number and the retry delay, and
// that the attempt that then succeeds records the incremented attempt.
func TestLocalEventsStepRetry(t *testing.T) {
	m := newMemoryClient()
	id := aws.String("step-1")
	checkpoint(t, m, durable.OperationUpdate{Id: id, Type: durable.OperationTypeStep, Action: durable.OperationActionStart, Name: aws.String("flaky")})
	checkpoint(t, m, durable.OperationUpdate{
		Id: id, Type: durable.OperationTypeStep, Action: durable.OperationActionRetry, Name: aws.String("flaky"),
		Error:       &durable.ErrorObject{ErrorType: aws.String("Transient"), ErrorMessage: aws.String("try again")},
		StepOptions: &durable.StepOptions{NextAttemptDelaySeconds: aws.Int32(7)},
	})
	checkpoint(t, m, durable.OperationUpdate{Id: id, Type: durable.OperationTypeStep, Action: durable.OperationActionStart})
	checkpoint(t, m, durable.OperationUpdate{Id: id, Type: durable.OperationTypeStep, Action: durable.OperationActionSucceed, Payload: aws.String(`"ok"`)})

	events := m.allEvents()
	want := []types.EventType{types.EventTypeStepStarted, types.EventTypeStepFailed, types.EventTypeStepStarted, types.EventTypeStepSucceeded}
	if len(events) != len(want) {
		t.Fatalf("len(events) = %d, want %d", len(events), len(want))
	}
	for i, ev := range events {
		if ev.EventType != want[i] {
			t.Errorf("events[%d].EventType = %s, want %s", i, ev.EventType, want[i])
		}
	}
	failed := events[1].StepFailedDetails
	if failed == nil || failed.Error == nil || aws.ToString(failed.Error.Payload.ErrorType) != "Transient" {
		t.Fatalf("StepFailed details = %+v, want Transient error", failed)
	}
	if failed.RetryDetails == nil || failed.RetryDetails.CurrentAttempt != 1 || aws.ToInt32(failed.RetryDetails.NextAttemptDelaySeconds) != 7 {
		t.Errorf("StepFailed RetryDetails = %+v, want attempt 1 and delay 7", failed.RetryDetails)
	}
	if aws.ToString(events[1].Name) != "flaky" {
		t.Errorf("StepFailed event Name = %q, want flaky", aws.ToString(events[1].Name))
	}
	succeeded := events[3].StepSucceededDetails
	if succeeded == nil || succeeded.RetryDetails == nil || succeeded.RetryDetails.CurrentAttempt != 1 {
		t.Errorf("StepSucceeded details = %+v, want attempt 1", succeeded)
	}
}

// TestLocalEventsPollingStepRetryWithoutError asserts that a retry that
// carries state but no error is recorded as a succeeded attempt with that
// state as its result.
func TestLocalEventsPollingStepRetryWithoutError(t *testing.T) {
	m := newMemoryClient()
	id := aws.String("poll-1")
	checkpoint(t, m, durable.OperationUpdate{Id: id, Type: durable.OperationTypeStep, Action: durable.OperationActionStart})
	checkpoint(t, m, durable.OperationUpdate{
		Id: id, Type: durable.OperationTypeStep, Action: durable.OperationActionRetry,
		Payload: aws.String(`"IN_PROGRESS"`),
	})

	events := m.allEvents()
	if len(events) != 2 || events[1].EventType != types.EventTypeStepSucceeded {
		t.Fatalf("events = %+v, want StepStarted then StepSucceeded", events)
	}
	if d := events[1].StepSucceededDetails; d == nil || aws.ToString(d.Result.Payload) != `"IN_PROGRESS"` {
		t.Errorf("StepSucceeded details = %+v, want the polling state as result", d)
	}
}

// TestLocalEventsCallbackTimeout asserts that timing out a callback records
// a CallbackTimedOut event with an error payload.
func TestLocalEventsCallbackTimeout(t *testing.T) {
	m := newMemoryClient()
	id := aws.String("cb-1")
	checkpoint(t, m, durable.OperationUpdate{
		Id: id, Type: durable.OperationTypeCallback, Action: durable.OperationActionStart,
		CallbackOptions: &durable.CallbackOptions{TimeoutSeconds: 30, HeartbeatTimeoutSeconds: 10},
	})
	if err := m.timeoutCallback("cb-1"); err != nil {
		t.Fatal(err)
	}

	events := m.allEvents()
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(events))
	}
	started := events[0].CallbackStartedDetails
	if started == nil || aws.ToString(started.CallbackId) != "cb-1" || aws.ToInt32(started.Timeout) != 30 || aws.ToInt32(started.HeartbeatTimeout) != 10 {
		t.Errorf("CallbackStarted details = %+v, want id cb-1, timeout 30, heartbeat 10", started)
	}
	if events[1].EventType != types.EventTypeCallbackTimedOut {
		t.Fatalf("events[1].EventType = %s, want CallbackTimedOut", events[1].EventType)
	}
	if d := events[1].CallbackTimedOutDetails; d == nil || d.Error == nil || aws.ToString(d.Error.Payload.ErrorType) != errTypeCallbackTimedOut {
		t.Errorf("CallbackTimedOut details = %+v, want error type %q", d, errTypeCallbackTimedOut)
	}
}

// TestLocalEventsExecutionCheckpointDefersTerminalEvent asserts that a
// checkpoint on the execution operation records no event by itself, that
// a PENDING response still records none, and that the SUCCEEDED response
// then records one ExecutionSucceeded event carrying the checkpointed
// result.
func TestLocalEventsExecutionCheckpointDefersTerminalEvent(t *testing.T) {
	m := newMemoryClient()
	m.recordExecutionStarted(`"in"`)
	checkpoint(t, m, durable.OperationUpdate{
		Id: aws.String(localExecutionOperationID), Type: durable.OperationTypeExecution,
		Action: durable.OperationActionSucceed, Payload: aws.String(`"big"`),
	})
	if got := m.allEvents(); len(got) != 1 {
		t.Fatalf("events after the execution checkpoint = %d, want 1 (ExecutionStarted only)", len(got))
	}

	m.recordExecutionEnded(statusPending, nil, nil)
	if got := m.allEvents(); len(got) != 1 {
		t.Fatalf("events after a PENDING response = %d, want 1 (no terminal event)", len(got))
	}

	empty := ""
	m.recordExecutionEnded(statusSucceeded, &empty, nil)
	m.recordExecutionEnded(statusSucceeded, &empty, nil)

	events := m.allEvents()
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2 (ExecutionStarted, ExecutionSucceeded)", len(events))
	}
	if events[1].EventType != types.EventTypeExecutionSucceeded {
		t.Errorf("events[1].EventType = %s, want ExecutionSucceeded", events[1].EventType)
	}
	if d := events[1].ExecutionSucceededDetails; d == nil || d.Result == nil || aws.ToString(d.Result.Payload) != `"big"` {
		t.Errorf("ExecutionSucceeded details = %+v, want the checkpointed result", d)
	}
}

// TestLocalEventsExecutionCheckpointFailure asserts that a FAIL checkpoint
// on the execution operation fills the ExecutionFailed event when the
// FAILED response carries no error of its own.
func TestLocalEventsExecutionCheckpointFailure(t *testing.T) {
	m := newMemoryClient()
	checkpoint(t, m, durable.OperationUpdate{
		Id: aws.String(localExecutionOperationID), Type: durable.OperationTypeExecution,
		Action: durable.OperationActionFail,
		Error:  &durable.ErrorObject{ErrorType: aws.String("Boom"), ErrorMessage: aws.String("too big")},
	})
	m.recordExecutionEnded(statusFailed, nil, nil)

	events := m.allEvents()
	if len(events) != 1 || events[0].EventType != types.EventTypeExecutionFailed {
		t.Fatalf("events = %+v, want one ExecutionFailed", events)
	}
	if d := events[0].ExecutionFailedDetails; d == nil || d.Error == nil || aws.ToString(d.Error.Payload.ErrorType) != "Boom" {
		t.Errorf("ExecutionFailed details = %+v, want the checkpointed error", d)
	}
}

// TestInvocationsFromEventsSkipsOtherEventsAndNilDetails asserts the
// invocation derivation ignores non-invocation events and tolerates an
// InvocationCompleted event without details.
func TestInvocationsFromEventsSkipsOtherEventsAndNilDetails(t *testing.T) {
	events := []types.Event{
		{EventType: types.EventTypeStepStarted, Id: aws.String("s")},
		{EventType: types.EventTypeInvocationCompleted},
		{EventType: types.EventTypeInvocationCompleted, InvocationCompletedDetails: &types.InvocationCompletedDetails{RequestId: aws.String("r2")}},
	}
	got := invocationsFromEvents(events)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0] != (TestInvocation{}) {
		t.Errorf("got[0] = %+v, want zero record", got[0])
	}
	if got[1].RequestID != "r2" || got[1].Error != nil {
		t.Errorf("got[1] = %+v, want RequestID r2 and no error", got[1])
	}
	if invocationsFromEvents(nil) != nil {
		t.Error("invocationsFromEvents(nil) != nil")
	}
}
