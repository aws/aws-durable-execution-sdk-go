// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var (
	fakeErrorData  = `{"code":"E42"}`
	fakeStackTrace = []string{"inner frame.go:1", "outer frame.go:2"}
)

// errorDecoratingClient is a fake ExecutionClient. It forwards every call
// to an in-memory client and replaces ErrorData and StackTrace on every
// FAIL update with fixed values.
type errorDecoratingClient struct {
	inner *memoryClient
}

func (c errorDecoratingClient) GetExecutionState(ctx context.Context, in durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return c.inner.GetExecutionState(ctx, in)
}

func (c errorDecoratingClient) Checkpoint(ctx context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	updates := make([]durable.OperationUpdate, len(in.Updates))
	for i, u := range in.Updates {
		if u.Action == durable.OperationActionFail && u.Error != nil {
			e := *u.Error
			data := fakeErrorData
			e.ErrorData = &data
			e.StackTrace = append([]string(nil), fakeStackTrace...)
			u.Error = &e
		}
		updates[i] = u
	}
	in.Updates = updates
	return c.inner.Checkpoint(ctx, in)
}

// newDecoratedRunner builds a LocalRunner whose handler talks to an
// errorDecoratingClient while the runner reads the in-memory records.
func newDecoratedRunner[I, O any](handler durable.Handler[I, O]) *LocalRunner[I, O] {
	client := newMemoryClient()
	raw := durable.Wrap(handler, durable.WithExecutionClient(errorDecoratingClient{inner: client}))
	return &LocalRunner[I, O]{
		exec:   newLocalExecution(raw, client, newFunctionRegistry(), localExecutionArn, 0),
		client: client,
		cfg:    runnerConfig{maxInvocations: DefaultMaxInvocations},
	}
}

func assertErrorDetails(t *testing.T, what, data string, stack []string) {
	t.Helper()
	if data != fakeErrorData {
		t.Errorf("%s ErrorData = %q, want %q", what, data, fakeErrorData)
	}
	if !reflect.DeepEqual(stack, fakeStackTrace) {
		t.Errorf("%s StackTrace = %v, want %v", what, stack, fakeStackTrace)
	}
}

func TestErrorDetailsFromFakeClient(t *testing.T) {
	handler := func(ctx durable.Context, _ any) (string, error) {
		_, stepErr := durable.Step(ctx, "failing-step", func(durable.StepContext) (string, error) {
			return "", errors.New("step failed")
		}, durable.WithRetry(durable.NoRetry()))
		if stepErr == nil {
			return "", errors.New("step succeeded")
		}
		_, childErr := durable.RunInChildContext(ctx, "failing-child", func(durable.Context) (string, error) {
			return "", errors.New("child failed")
		})
		if childErr == nil {
			return "", errors.New("child succeeded")
		}
		cb, err := durable.CreateCallback[string](ctx, "failing-callback")
		if err != nil {
			return "", err
		}
		if _, err := cb.Result(ctx); err == nil {
			return "", errors.New("callback succeeded")
		}
		return "", errors.New("execution failed")
	}

	runner := newDecoratedRunner(handler)
	result, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Pending {
		t.Fatalf("status = %s, want PENDING on the callback", result.Status)
	}
	open := runner.OpenCallbacks()
	if len(open) != 1 {
		t.Fatalf("open callbacks = %+v, want one", open)
	}
	if err := runner.client.completeCallback(open[0].CallbackID, operationResult{
		status:     statusFailed,
		errType:    "Rejected",
		errMsg:     "callback failed",
		errData:    fakeErrorData,
		stackTrace: fakeStackTrace,
	}); err != nil {
		t.Fatal(err)
	}
	result, err = runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Failed {
		t.Fatalf("status = %s, want FAILED", result.Status)
	}

	step := result.Operation("failing-step")
	if step == nil || step.StepDetails == nil {
		t.Fatalf("failing-step = %+v, want step details", step)
	}
	assertErrorDetails(t, "step", step.StepDetails.ErrorData, step.StepDetails.StackTrace)

	child := result.Operation("failing-child")
	if child == nil || child.ContextDetails == nil {
		t.Fatalf("failing-child = %+v, want context details", child)
	}
	assertErrorDetails(t, "context", child.ContextDetails.ErrorData, child.ContextDetails.StackTrace)

	cb := result.Operation("failing-callback")
	if cb == nil || cb.CallbackDetails == nil {
		t.Fatalf("failing-callback = %+v, want callback details", cb)
	}
	assertErrorDetails(t, "callback", cb.CallbackDetails.ErrorData, cb.CallbackDetails.StackTrace)

	if result.Error == nil {
		t.Fatal("result.Error = nil, want the execution failure")
	}
	if len(result.Error.StackTrace) == 0 {
		t.Error("result.Error.StackTrace is empty, want the recorded trace")
	}
	last := result.Invocations[len(result.Invocations)-1]
	if last.Error == nil || !reflect.DeepEqual(*last.Error, *result.Error) {
		t.Errorf("last invocation Error = %+v, want %+v", last.Error, result.Error)
	}
}

// TestExecutionErrorDetails checks that a FAILED invocation response's
// ErrorData and StackTrace reach TestResult.Error.
func TestExecutionErrorDetails(t *testing.T) {
	ops := []durable.Operation{}
	resp := []byte(`{"Status":"FAILED","Error":{"ErrorType":"E","ErrorMessage":"m","ErrorData":"{\"code\":\"E42\"}","StackTrace":["inner frame.go:1","outer frame.go:2"]}}`)
	tr, err := testResultFromResponse(resp, ops)
	if err != nil {
		t.Fatal(err)
	}
	if tr.Error == nil {
		t.Fatal("Error = nil")
	}
	assertErrorDetails(t, "TestResult.Error", tr.Error.ErrorData, tr.Error.StackTrace)
}

// TestWaitSecondsFromHistory checks that WaitSeconds comes from the first
// WaitStarted event whose Id is the operation's ID.
func TestWaitSecondsFromHistory(t *testing.T) {
	r := &TestResult{Operations: []TestOperation{
		{ID: "w1", Type: "WAIT", WaitDetails: &TestWaitDetails{}},
		{ID: "w2", Type: "WAIT", WaitDetails: &TestWaitDetails{}},
	}}
	r.attachEvents([]types.Event{
		{EventType: types.EventTypeWaitStarted, Id: aws.String("w1"), WaitStartedDetails: &types.WaitStartedDetails{Duration: aws.Int32(7)}},
		{EventType: types.EventTypeWaitStarted, Id: aws.String("w1"), WaitStartedDetails: &types.WaitStartedDetails{Duration: aws.Int32(9)}},
	})
	if got := r.Operations[0].WaitDetails.WaitSeconds; got != 7 {
		t.Errorf("w1 WaitSeconds = %d, want 7", got)
	}
	if got := r.Operations[1].WaitDetails.WaitSeconds; got != 0 {
		t.Errorf("w2 WaitSeconds = %d, want 0", got)
	}
}
