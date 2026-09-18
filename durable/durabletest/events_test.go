// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// twoWaitHandler suspends twice: once on each wait.
func twoWaitHandler(ctx durable.Context, event string) (string, error) {
	if _, err := durable.Step(ctx, "first", func(durable.StepContext) (string, error) {
		return "1", nil
	}); err != nil {
		return "", err
	}
	if err := durable.Wait(ctx, "pause-1", time.Minute); err != nil {
		return "", err
	}
	if _, err := durable.Step(ctx, "second", func(durable.StepContext) (string, error) {
		return "2", nil
	}); err != nil {
		return "", err
	}
	if err := durable.Wait(ctx, "pause-2", time.Minute); err != nil {
		return "", err
	}
	if _, err := durable.Step(ctx, "third", func(durable.StepContext) (string, error) {
		return "3", nil
	}); err != nil {
		return "", err
	}
	return event + "-done", nil
}

// TestEventsAndInvocationsAcrossTwoSuspensions asserts the event sequence
// and the invocation records of a workflow that suspends twice, so the
// execution needs three invocations to finish.
func TestEventsAndInvocationsAcrossTwoSuspensions(t *testing.T) {
	runner := durabletest.NewLocalRunner(twoWaitHandler)
	result := runner.RunUntilComplete(t, "go")

	if result.Status != durabletest.Succeeded {
		t.Fatalf("Status = %s, want SUCCEEDED", result.Status)
	}

	wantTypes := []string{
		"ExecutionStarted",
		"StepStarted", "StepSucceeded",
		"WaitStarted",
		"InvocationCompleted",
		"WaitSucceeded",
		"StepStarted", "StepSucceeded",
		"WaitStarted",
		"InvocationCompleted",
		"WaitSucceeded",
		"StepStarted", "StepSucceeded",
		"InvocationCompleted",
		"ExecutionSucceeded",
	}
	if got := result.EventTypes(); !reflect.DeepEqual(got, wantTypes) {
		t.Errorf("EventTypes() =\n  %v\nwant\n  %v", got, wantTypes)
	}

	// Event IDs count from 1 in order.
	for i, ev := range result.Events {
		if aws.ToInt32(ev.EventId) != int32(i+1) {
			t.Errorf("Events[%d].EventId = %d, want %d", i, aws.ToInt32(ev.EventId), i+1)
		}
		if ev.EventTimestamp == nil {
			t.Errorf("Events[%d].EventTimestamp = nil, want set", i)
		}
	}

	// Operation events carry the operation's identity, and that identity
	// matches the operation snapshot.
	first := result.Operation("first")
	if first == nil {
		t.Fatal("Operation(first) = nil")
	}
	ev := result.Events[1]
	if aws.ToString(ev.Id) != first.ID || aws.ToString(ev.Name) != "first" || aws.ToString(ev.SubType) != first.SubType {
		t.Errorf("StepStarted event identity = (%q, %q, %q), want (%q, %q, %q)",
			aws.ToString(ev.Id), aws.ToString(ev.Name), aws.ToString(ev.SubType), first.ID, "first", first.SubType)
	}
	if d := result.Events[2].StepSucceededDetails; d == nil || aws.ToString(d.Result.Payload) != `"1"` {
		t.Errorf("StepSucceeded details = %+v, want result payload %q", d, `"1"`)
	}
	if d := result.Events[3].WaitStartedDetails; d == nil || aws.ToInt32(d.Duration) != 60 || d.ScheduledEndTimestamp == nil {
		t.Errorf("WaitStarted details = %+v, want Duration 60 and a scheduled end", d)
	}
	if d := result.Events[len(result.Events)-1].ExecutionSucceededDetails; d == nil || aws.ToString(d.Result.Payload) != `"go-done"` {
		t.Errorf("ExecutionSucceeded details = %+v, want result payload %q", d, `"go-done"`)
	}

	// One invocation record per InvocationCompleted event.
	if len(result.Invocations) != 3 {
		t.Fatalf("len(Invocations) = %d, want 3", len(result.Invocations))
	}
	for i, inv := range result.Invocations {
		want := "local-request-" + string(rune('1'+i))
		if inv.RequestID != want {
			t.Errorf("Invocations[%d].RequestID = %q, want %q", i, inv.RequestID, want)
		}
		if inv.StartTime.IsZero() || inv.EndTime.Before(inv.StartTime) {
			t.Errorf("Invocations[%d] times = (%v, %v), want start set and end not before start", i, inv.StartTime, inv.EndTime)
		}
		if inv.Error != nil {
			t.Errorf("Invocations[%d].Error = %+v, want nil", i, inv.Error)
		}
	}
	// Invocations do not overlap and are in order.
	for i := 1; i < len(result.Invocations); i++ {
		if result.Invocations[i].StartTime.Before(result.Invocations[i-1].EndTime) {
			t.Errorf("Invocations[%d] starts before Invocations[%d] ends", i, i-1)
		}
	}
}

// TestEventsAccumulateAcrossRunCalls asserts that single Run calls see the
// same growing event log and invocation list as RunUntilComplete.
func TestEventsAccumulateAcrossRunCalls(t *testing.T) {
	runner := durabletest.NewLocalRunner(twoWaitHandler)

	r1 := runner.Run(t, "go")
	if r1.Status != durabletest.Pending {
		t.Fatalf("first Run Status = %s, want PENDING", r1.Status)
	}
	if got := r1.EventTypes(); !reflect.DeepEqual(got, []string{"ExecutionStarted", "StepStarted", "StepSucceeded", "WaitStarted", "InvocationCompleted"}) {
		t.Errorf("first Run EventTypes() = %v", got)
	}
	if len(r1.Invocations) != 1 {
		t.Fatalf("first Run len(Invocations) = %d, want 1", len(r1.Invocations))
	}

	runner.CompletePendingTimers()
	r2 := runner.Run(t, "go")
	if len(r2.Invocations) != 2 {
		t.Fatalf("second Run len(Invocations) = %d, want 2", len(r2.Invocations))
	}
	if r2.Invocations[0] != r1.Invocations[0] {
		t.Errorf("second Run Invocations[0] = %+v, want the first Run's record %+v", r2.Invocations[0], r1.Invocations[0])
	}
	if len(r2.Events) <= len(r1.Events) {
		t.Errorf("second Run len(Events) = %d, want more than %d", len(r2.Events), len(r1.Events))
	}
	// The first Run's slice is a snapshot: later invocations do not grow it.
	if len(r1.Events) != 5 {
		t.Errorf("first Run len(Events) = %d after second Run, want 5", len(r1.Events))
	}
}

// TestInvocationErrorOnFailedExecution asserts that the invocation whose
// response fails the execution carries that error, and the event log ends
// with ExecutionFailed.
func TestInvocationErrorOnFailedExecution(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		if err := durable.Wait(ctx, "pause", time.Minute); err != nil {
			return "", err
		}
		return "", errors.New("boom")
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go")

	if result.Status != durabletest.Failed {
		t.Fatalf("Status = %s, want FAILED", result.Status)
	}
	if result.Error == nil {
		t.Fatal("Error = nil, want set")
	}
	if len(result.Invocations) != 2 {
		t.Fatalf("len(Invocations) = %d, want 2", len(result.Invocations))
	}
	if result.Invocations[0].Error != nil {
		t.Errorf("Invocations[0].Error = %+v, want nil (it suspended)", result.Invocations[0].Error)
	}
	last := result.Invocations[1]
	if last.Error == nil || *last.Error != *result.Error {
		t.Errorf("Invocations[1].Error = %+v, want %+v", last.Error, result.Error)
	}
	types := result.EventTypes()
	if types[len(types)-1] != "ExecutionFailed" {
		t.Errorf("last event = %q, want ExecutionFailed", types[len(types)-1])
	}
	failed := result.Events[len(result.Events)-1].ExecutionFailedDetails
	if failed == nil || failed.Error == nil || failed.Error.Payload == nil || aws.ToString(failed.Error.Payload.ErrorMessage) != result.Error.Message {
		t.Errorf("ExecutionFailed details = %+v, want error message %q", failed, result.Error.Message)
	}
}

// TestCallbackAndInvokeEvents asserts the events the runner records for
// transitions it applies outside a checkpoint: callback resolution and
// chained-invoke settlement.
func TestCallbackAndInvokeEvents(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		cb, err := durable.CreateCallback[string](ctx, "approval")
		if err != nil {
			return "", err
		}
		approved, err := cb.Result()
		if err != nil {
			return "", err
		}
		return durable.Invoke[string](ctx, "downstream", "downstream-function", approved)
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go")
	if result.Status != durabletest.Pending {
		t.Fatalf("Status = %s, want PENDING on callback", result.Status)
	}
	cbs := runner.OpenCallbacks()
	if len(cbs) != 1 {
		t.Fatalf("len(OpenCallbacks) = %d, want 1", len(cbs))
	}
	if err := runner.SendCallbackSuccess(cbs[0].CallbackID, "yes"); err != nil {
		t.Fatal(err)
	}
	result = runner.RunUntilComplete(t, "go")
	if result.Status != durabletest.Pending {
		t.Fatalf("Status = %s, want PENDING on invoke", result.Status)
	}
	if err := runner.CompleteChainedInvoke("downstream", "ok"); err != nil {
		t.Fatal(err)
	}
	result = runner.RunUntilComplete(t, "go")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("Status = %s, want SUCCEEDED", result.Status)
	}

	wantTypes := []string{
		"ExecutionStarted",
		"CallbackStarted",
		"InvocationCompleted",
		"CallbackSucceeded",
		"ChainedInvokeStarted",
		"InvocationCompleted",
		"ChainedInvokeSucceeded",
		"InvocationCompleted",
		"ExecutionSucceeded",
	}
	if got := result.EventTypes(); !reflect.DeepEqual(got, wantTypes) {
		t.Errorf("EventTypes() =\n  %v\nwant\n  %v", got, wantTypes)
	}
	if d := result.Events[1].CallbackStartedDetails; d == nil || aws.ToString(d.CallbackId) != cbs[0].CallbackID {
		t.Errorf("CallbackStarted details = %+v, want CallbackId %q", d, cbs[0].CallbackID)
	}
	if d := result.Events[3].CallbackSucceededDetails; d == nil || aws.ToString(d.Result.Payload) != `"yes"` {
		t.Errorf("CallbackSucceeded details = %+v, want payload %q", d, `"yes"`)
	}
	if d := result.Events[4].ChainedInvokeStartedDetails; d == nil || aws.ToString(d.FunctionName) != "downstream-function" || aws.ToString(d.Input.Payload) != `"yes"` {
		t.Errorf("ChainedInvokeStarted details = %+v, want function %q and input %q", d, "downstream-function", `"yes"`)
	}
	if d := result.Events[6].ChainedInvokeSucceededDetails; d == nil || aws.ToString(d.Result.Payload) != `"ok"` {
		t.Errorf("ChainedInvokeSucceeded details = %+v, want payload %q", d, `"ok"`)
	}
	if len(result.Invocations) != 3 {
		t.Errorf("len(Invocations) = %d, want 3", len(result.Invocations))
	}
}

// TestCloudRunnerEventsAndInvocations asserts that CloudRunner exposes the
// service's event sequence unchanged and derives invocation records from
// its InvocationCompleted events.
func TestCloudRunnerEventsAndInvocations(t *testing.T) {
	t0 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	events := []types.Event{
		{
			EventId:   aws.Int32(1),
			EventType: types.EventTypeExecutionStarted,
			Id:        aws.String("exec"),
		},
		{
			EventId:            aws.Int32(2),
			Id:                 aws.String("op-1"),
			Name:               aws.String("validate"),
			SubType:            aws.String("Step"),
			EventType:          types.EventTypeStepStarted,
			StepStartedDetails: &types.StepStartedDetails{},
		},
		{
			EventId:   aws.Int32(3),
			EventType: types.EventTypeInvocationCompleted,
			InvocationCompletedDetails: &types.InvocationCompletedDetails{
				RequestId:      aws.String("req-a"),
				StartTimestamp: aws.Time(t0),
				EndTimestamp:   aws.Time(t0.Add(time.Second)),
			},
		},
		{
			EventId:   aws.Int32(4),
			Id:        aws.String("op-1"),
			EventType: types.EventTypeStepSucceeded,
			StepSucceededDetails: &types.StepSucceededDetails{
				Result: &types.EventResult{Payload: aws.String(`"validated"`)},
			},
		},
		{
			EventId:   aws.Int32(5),
			EventType: types.EventTypeInvocationCompleted,
			InvocationCompletedDetails: &types.InvocationCompletedDetails{
				RequestId:      aws.String("req-b"),
				StartTimestamp: aws.Time(t0.Add(2 * time.Second)),
				EndTimestamp:   aws.Time(t0.Add(3 * time.Second)),
				Error: &types.EventError{Payload: &types.ErrorObject{
					ErrorType:    aws.String("RuntimeError"),
					ErrorMessage: aws.String("out of memory"),
				}},
			},
		},
		{
			EventId:   aws.Int32(6),
			EventType: types.EventTypeExecutionSucceeded,
			Id:        aws.String("exec"),
		},
	}

	api := &fakeCloudAPI{
		getExecutionFunc: func(_ context.Context, _ *lambda.GetDurableExecutionInput) (*lambda.GetDurableExecutionOutput, error) {
			return &lambda.GetDurableExecutionOutput{
				Status: types.ExecutionStatusSucceeded,
				Result: aws.String(`"ok"`),
			}, nil
		},
		getHistoryFunc: func(_ context.Context, params *lambda.GetDurableExecutionHistoryInput) (*lambda.GetDurableExecutionHistoryOutput, error) {
			// Two pages, split mid-sequence.
			if params.Marker == nil {
				return &lambda.GetDurableExecutionHistoryOutput{Events: events[:3], NextMarker: aws.String("p2")}, nil
			}
			return &lambda.GetDurableExecutionHistoryOutput{Events: events[3:]}, nil
		},
	}

	runner := durabletest.NewCloudRunner(api, "my-func:$LATEST",
		durabletest.WithPollInterval(time.Millisecond),
		durabletest.WithTimeout(time.Second),
	)
	result := runner.Run(t, "go")

	if result.Status != durabletest.Succeeded {
		t.Fatalf("Status = %s, want SUCCEEDED", result.Status)
	}
	if !reflect.DeepEqual(result.Events, events) {
		t.Errorf("Events differ from the service's sequence:\n got %+v\nwant %+v", result.Events, events)
	}
	wantInv := []durabletest.TestInvocation{
		{RequestID: "req-a", StartTime: t0, EndTime: t0.Add(time.Second)},
		{
			RequestID: "req-b",
			StartTime: t0.Add(2 * time.Second),
			EndTime:   t0.Add(3 * time.Second),
			Error:     &durabletest.TestError{Type: "RuntimeError", Message: "out of memory"},
		},
	}
	if !reflect.DeepEqual(result.Invocations, wantInv) {
		t.Errorf("Invocations =\n  %+v\nwant\n  %+v", result.Invocations, wantInv)
	}
	// Existing fields are unaffected by the InvocationCompleted events.
	steps := result.OperationsByType("STEP")
	if len(steps) != 1 || steps[0].Status != "SUCCEEDED" {
		t.Errorf("OperationsByType(STEP) = %+v, want one SUCCEEDED step", steps)
	}
}

// oversizedResultBytes is one byte more than the largest result an
// invocation response carries inline, so the handler checkpoints the
// result on the execution operation instead of returning it.
const oversizedResultBytes = 6*1024*1024 - 50 + 1

// oversizedResultHandler runs one step and then returns a result too large
// to carry inline.
func oversizedResultHandler(ctx durable.Context, _ string) (string, error) {
	if _, err := durable.Step(ctx, "prep", func(durable.StepContext) (string, error) {
		return "ok", nil
	}); err != nil {
		return "", err
	}
	return strings.Repeat("a", oversizedResultBytes-2), nil
}

// TestOversizedResultEventOrder asserts that when the handler checkpoints
// its result on the execution operation, the history still ends with the
// terminal event after InvocationCompleted, and that the terminal event
// carries the checkpointed result.
func TestOversizedResultEventOrder(t *testing.T) {
	runner := durabletest.NewLocalRunner(oversizedResultHandler)
	result := runner.RunUntilComplete(t, "go")

	if result.Status != durabletest.Succeeded {
		t.Fatalf("Status = %s, want SUCCEEDED", result.Status)
	}
	wantTypes := []string{
		"ExecutionStarted",
		"StepStarted", "StepSucceeded",
		"InvocationCompleted",
		"ExecutionSucceeded",
	}
	if got := result.EventTypes(); !reflect.DeepEqual(got, wantTypes) {
		t.Fatalf("EventTypes() =\n  %v\nwant\n  %v", got, wantTypes)
	}
	last := result.Events[len(result.Events)-1]
	d := last.ExecutionSucceededDetails
	if d == nil || d.Result == nil || d.Result.Payload == nil {
		t.Fatalf("ExecutionSucceeded details = %+v, want a result payload", d)
	}
	if got := len(*d.Result.Payload); got != oversizedResultBytes {
		t.Errorf("ExecutionSucceeded result payload size = %d, want %d", got, oversizedResultBytes)
	}
	if len(result.Invocations) != 1 {
		t.Errorf("len(Invocations) = %d, want 1", len(result.Invocations))
	}
}

// TestOversizedResultPendingRecordsNoTerminalEvent asserts that when the
// checkpoint carrying an oversized result gets a response without a
// token, the invocation ends PENDING and no terminal event is recorded;
// the next invocation then records the terminal event last.
func TestOversizedResultPendingRecordsNoTerminalEvent(t *testing.T) {
	handler := func(_ durable.Context, _ string) (string, error) {
		return strings.Repeat("a", oversizedResultBytes-2), nil
	}
	runner := durabletest.NewLocalRunner(handler)
	// With no steps, the first checkpoint is the oversized-result one.
	runner.OmitTokenOnCheckpoint(1)

	result := runner.Run(t, "go")
	if result.Status != durabletest.Pending {
		t.Fatalf("Status = %s, want PENDING", result.Status)
	}
	wantTypes := []string{"ExecutionStarted", "InvocationCompleted"}
	if got := result.EventTypes(); !reflect.DeepEqual(got, wantTypes) {
		t.Fatalf("EventTypes() after PENDING =\n  %v\nwant\n  %v", got, wantTypes)
	}

	result = runner.Run(t, "go")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("Status = %s, want SUCCEEDED", result.Status)
	}
	wantTypes = []string{"ExecutionStarted", "InvocationCompleted", "InvocationCompleted", "ExecutionSucceeded"}
	if got := result.EventTypes(); !reflect.DeepEqual(got, wantTypes) {
		t.Fatalf("EventTypes() after SUCCEEDED =\n  %v\nwant\n  %v", got, wantTypes)
	}
	if d := result.Events[len(result.Events)-1].ExecutionSucceededDetails; d == nil || d.Result == nil || len(aws.ToString(d.Result.Payload)) != oversizedResultBytes {
		t.Errorf("ExecutionSucceeded details = %+v, want the checkpointed result", d)
	}
	if len(result.Invocations) != 2 {
		t.Errorf("len(Invocations) = %d, want 2", len(result.Invocations))
	}
}
