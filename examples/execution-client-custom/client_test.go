// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// The tests in this file check serviceClient's conversions field by field,
// as the SDK's tests check its default client. serviceClient copies that
// mapping, so a field it omits or maps wrongly fails here.

// recordingAPI is a lambdaAPI double. It records the requests serviceClient
// builds and serves canned responses, so both directions of the
// conversions are observable.
type recordingAPI struct {
	gotStateIn *lambdasvc.GetDurableExecutionStateInput
	stateOut   *lambdasvc.GetDurableExecutionStateOutput
	stateErr   error

	gotCheckpointIn *lambdasvc.CheckpointDurableExecutionInput
	checkpointOut   *lambdasvc.CheckpointDurableExecutionOutput
	checkpointErr   error
}

func (r *recordingAPI) GetDurableExecutionState(_ context.Context, in *lambdasvc.GetDurableExecutionStateInput, _ ...func(*lambdasvc.Options)) (*lambdasvc.GetDurableExecutionStateOutput, error) {
	r.gotStateIn = in
	return r.stateOut, r.stateErr
}

func (r *recordingAPI) CheckpointDurableExecution(_ context.Context, in *lambdasvc.CheckpointDurableExecutionInput, _ ...func(*lambdasvc.Options)) (*lambdasvc.CheckpointDurableExecutionOutput, error) {
	r.gotCheckpointIn = in
	return r.checkpointOut, r.checkpointErr
}

func newServiceClient(api lambdaAPI) *serviceClient {
	return &serviceClient{api: api, logger: slog.New(slog.NewJSONHandler(io.Discard, nil))}
}

// serviceOperationsAllFields returns an operation of each type in the AWS
// SDK's type, together covering every field operationsFromService maps,
// including the error inside each kind of details, present or absent.
func serviceOperationsAllFields(start, end time.Time) []types.Operation {
	return []types.Operation{
		{
			Id:             aws.String("op-exec"),
			Status:         types.OperationStatusStarted,
			Type:           types.OperationTypeExecution,
			SubType:        aws.String("sub"),
			Name:           aws.String("exec-name"),
			ParentId:       aws.String("parent-0"),
			StartTimestamp: aws.Time(start),
			EndTimestamp:   aws.Time(end),
			ExecutionDetails: &types.ExecutionDetails{
				InputPayload: aws.String(`{"orderId":"ORD-12345"}`),
			},
		},
		{
			Id:     aws.String("op-step"),
			Status: types.OperationStatusPending,
			Type:   types.OperationTypeStep,
			StepDetails: &types.StepDetails{
				Attempt: 3,
				Result:  aws.String(`"r"`),
				Error: &types.ErrorObject{
					ErrorType:    aws.String("T"),
					ErrorMessage: aws.String("m"),
					ErrorData:    aws.String("d"),
					StackTrace:   []string{"f1", "f2"},
				},
				NextAttemptTimestamp: aws.Time(end),
			},
		},
		{
			Id:          aws.String("op-step-ok"),
			Status:      types.OperationStatusSucceeded,
			Type:        types.OperationTypeStep,
			StepDetails: &types.StepDetails{Attempt: 1, Result: aws.String(`"ok"`)},
		},
		{
			Id:     aws.String("op-wait"),
			Status: types.OperationStatusStarted,
			Type:   types.OperationTypeWait,
			WaitDetails: &types.WaitDetails{
				ScheduledEndTimestamp: aws.Time(end),
			},
		},
		{
			Id:     aws.String("op-cb"),
			Status: types.OperationStatusStarted,
			Type:   types.OperationTypeCallback,
			CallbackDetails: &types.CallbackDetails{
				CallbackId: aws.String("cb-1"),
				Result:     aws.String(`"cb"`),
				Error:      &types.ErrorObject{ErrorType: aws.String("CT")},
			},
		},
		{
			Id:     aws.String("op-inv"),
			Status: types.OperationStatusSucceeded,
			Type:   types.OperationTypeChainedInvoke,
			ChainedInvokeDetails: &types.ChainedInvokeDetails{
				Result: aws.String(`"inv"`),
				Error:  &types.ErrorObject{ErrorMessage: aws.String("im")},
			},
		},
		{
			Id:     aws.String("op-ctx"),
			Status: types.OperationStatusFailed,
			Type:   types.OperationTypeContext,
			ContextDetails: &types.ContextDetails{
				Result:         aws.String(`"c"`),
				ReplayChildren: aws.Bool(true),
				Error:          &types.ErrorObject{ErrorData: aws.String("cd"), StackTrace: []string{"g1"}},
			},
		},
	}
}

// sdkOperationsAllFields is the SDK's image of serviceOperationsAllFields.
func sdkOperationsAllFields(start, end time.Time) []durable.Operation {
	return []durable.Operation{
		{
			Id:             aws.String("op-exec"),
			Status:         durable.OperationStatusStarted,
			Type:           durable.OperationTypeExecution,
			SubType:        aws.String("sub"),
			Name:           aws.String("exec-name"),
			ParentId:       aws.String("parent-0"),
			StartTimestamp: aws.Time(start),
			EndTimestamp:   aws.Time(end),
			ExecutionDetails: &durable.ExecutionDetails{
				InputPayload: aws.String(`{"orderId":"ORD-12345"}`),
			},
		},
		{
			Id:     aws.String("op-step"),
			Status: durable.OperationStatusPending,
			Type:   durable.OperationTypeStep,
			StepDetails: &durable.StepDetails{
				Attempt: 3,
				Result:  aws.String(`"r"`),
				Error: &durable.ErrorObject{
					ErrorType:    aws.String("T"),
					ErrorMessage: aws.String("m"),
					ErrorData:    aws.String("d"),
					StackTrace:   []string{"f1", "f2"},
				},
				NextAttemptTimestamp: aws.Time(end),
			},
		},
		{
			Id:          aws.String("op-step-ok"),
			Status:      durable.OperationStatusSucceeded,
			Type:        durable.OperationTypeStep,
			StepDetails: &durable.StepDetails{Attempt: 1, Result: aws.String(`"ok"`)},
		},
		{
			Id:     aws.String("op-wait"),
			Status: durable.OperationStatusStarted,
			Type:   durable.OperationTypeWait,
			WaitDetails: &durable.WaitDetails{
				ScheduledEndTimestamp: aws.Time(end),
			},
		},
		{
			Id:     aws.String("op-cb"),
			Status: durable.OperationStatusStarted,
			Type:   durable.OperationTypeCallback,
			CallbackDetails: &durable.CallbackDetails{
				CallbackId: aws.String("cb-1"),
				Result:     aws.String(`"cb"`),
				Error:      &durable.ErrorObject{ErrorType: aws.String("CT")},
			},
		},
		{
			Id:     aws.String("op-inv"),
			Status: durable.OperationStatusSucceeded,
			Type:   durable.OperationTypeChainedInvoke,
			ChainedInvokeDetails: &durable.ChainedInvokeDetails{
				Result: aws.String(`"inv"`),
				Error:  &durable.ErrorObject{ErrorMessage: aws.String("im")},
			},
		},
		{
			Id:     aws.String("op-ctx"),
			Status: durable.OperationStatusFailed,
			Type:   durable.OperationTypeContext,
			ContextDetails: &durable.ContextDetails{
				Result:         aws.String(`"c"`),
				ReplayChildren: aws.Bool(true),
				Error:          &durable.ErrorObject{ErrorData: aws.String("cd"), StackTrace: []string{"g1"}},
			},
		},
	}
}

func TestServiceClientGetExecutionState(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := start.Add(time.Minute)
	api := &recordingAPI{
		stateOut: &lambdasvc.GetDurableExecutionStateOutput{
			Operations: serviceOperationsAllFields(start, end),
			NextMarker: aws.String("marker-2"),
		},
	}

	out, err := newServiceClient(api).GetExecutionState(context.Background(), durable.GetExecutionStateInput{
		ExecutionArn:    "arn:exec",
		CheckpointToken: "token-1",
		Marker:          "marker-1",
	})
	if err != nil {
		t.Fatalf("GetExecutionState() error: %v", err)
	}

	// Every request field reaches the service, the page marker included.
	in := api.gotStateIn
	if got := aws.ToString(in.DurableExecutionArn); got != "arn:exec" {
		t.Errorf("DurableExecutionArn = %q, want %q", got, "arn:exec")
	}
	if got := aws.ToString(in.CheckpointToken); got != "token-1" {
		t.Errorf("CheckpointToken = %q, want %q", got, "token-1")
	}
	if got := aws.ToString(in.Marker); got != "marker-1" {
		t.Errorf("Marker = %q, want %q", got, "marker-1")
	}

	// Every operation field and the next page's marker come back.
	want := durable.GetExecutionStateOutput{
		Operations: sdkOperationsAllFields(start, end),
		NextMarker: "marker-2",
	}
	if !reflect.DeepEqual(out, want) {
		t.Errorf("GetExecutionState() = %+v, want %+v", out, want)
	}
}

func TestServiceClientGetExecutionStateLastPage(t *testing.T) {
	api := &recordingAPI{stateOut: &lambdasvc.GetDurableExecutionStateOutput{}}

	out, err := newServiceClient(api).GetExecutionState(context.Background(), durable.GetExecutionStateInput{
		ExecutionArn:    "arn:exec",
		CheckpointToken: "token-1",
	})
	if err != nil {
		t.Fatalf("GetExecutionState() error: %v", err)
	}
	// An empty Marker is omitted from the request, not sent as "".
	if api.gotStateIn.Marker != nil {
		t.Errorf("Marker = %q, want nil", aws.ToString(api.gotStateIn.Marker))
	}
	// A page without operations or a next marker maps to nil and "".
	if out.Operations != nil {
		t.Errorf("Operations = %v, want nil", out.Operations)
	}
	if out.NextMarker != "" {
		t.Errorf("NextMarker = %q, want empty", out.NextMarker)
	}
}

func TestServiceClientGetExecutionStateError(t *testing.T) {
	wantErr := errors.New("service unavailable")
	_, err := newServiceClient(&recordingAPI{stateErr: wantErr}).GetExecutionState(context.Background(), durable.GetExecutionStateInput{})
	if !errors.Is(err, wantErr) {
		t.Errorf("GetExecutionState() error = %v, want %v", err, wantErr)
	}
}

func TestServiceClientCheckpoint(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := start.Add(time.Minute)
	api := &recordingAPI{
		checkpointOut: &lambdasvc.CheckpointDurableExecutionOutput{
			CheckpointToken: aws.String("token-2"),
			NewExecutionState: &types.CheckpointUpdatedExecutionState{
				Operations: serviceOperationsAllFields(start, end),
			},
		},
	}

	// One update per option struct, together covering every field
	// updatesToService maps.
	updates := []durable.OperationUpdate{
		{
			Id:       aws.String("u-step"),
			Type:     durable.OperationTypeStep,
			Action:   durable.OperationActionRetry,
			SubType:  aws.String("sub"),
			Name:     aws.String("step-name"),
			ParentId: aws.String("parent-1"),
			Payload:  aws.String(`"p"`),
			Error: &durable.ErrorObject{
				ErrorType:    aws.String("T"),
				ErrorMessage: aws.String("m"),
				ErrorData:    aws.String("d"),
				StackTrace:   []string{"f1"},
			},
			StepOptions: &durable.StepOptions{NextAttemptDelaySeconds: aws.Int32(7)},
		},
		{
			Id:          aws.String("u-wait"),
			Type:        durable.OperationTypeWait,
			Action:      durable.OperationActionStart,
			WaitOptions: &durable.WaitOptions{WaitSeconds: aws.Int32(30)},
		},
		{
			Id:     aws.String("u-cb"),
			Type:   durable.OperationTypeCallback,
			Action: durable.OperationActionStart,
			CallbackOptions: &durable.CallbackOptions{
				TimeoutSeconds:          60,
				HeartbeatTimeoutSeconds: 15,
			},
		},
		{
			Id:     aws.String("u-inv"),
			Type:   durable.OperationTypeChainedInvoke,
			Action: durable.OperationActionStart,
			ChainedInvokeOptions: &durable.ChainedInvokeOptions{
				FunctionName: aws.String("fn"),
				TenantId:     aws.String("tenant"),
			},
		},
		{
			Id:             aws.String("u-ctx"),
			Type:           durable.OperationTypeContext,
			Action:         durable.OperationActionSucceed,
			ContextOptions: &durable.ContextOptions{ReplayChildren: aws.Bool(true)},
		},
	}

	out, err := newServiceClient(api).Checkpoint(context.Background(), durable.CheckpointInput{
		ExecutionArn:    "arn:exec",
		CheckpointToken: "token-1",
		Updates:         updates,
	})
	if err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}

	in := api.gotCheckpointIn
	if got := aws.ToString(in.DurableExecutionArn); got != "arn:exec" {
		t.Errorf("DurableExecutionArn = %q, want %q", got, "arn:exec")
	}
	if got := aws.ToString(in.CheckpointToken); got != "token-1" {
		t.Errorf("CheckpointToken = %q, want %q", got, "token-1")
	}
	wantUpdates := []types.OperationUpdate{
		{
			Id:       aws.String("u-step"),
			Type:     types.OperationTypeStep,
			Action:   types.OperationActionRetry,
			SubType:  aws.String("sub"),
			Name:     aws.String("step-name"),
			ParentId: aws.String("parent-1"),
			Payload:  aws.String(`"p"`),
			Error: &types.ErrorObject{
				ErrorType:    aws.String("T"),
				ErrorMessage: aws.String("m"),
				ErrorData:    aws.String("d"),
				StackTrace:   []string{"f1"},
			},
			StepOptions: &types.StepOptions{NextAttemptDelaySeconds: aws.Int32(7)},
		},
		{
			Id:          aws.String("u-wait"),
			Type:        types.OperationTypeWait,
			Action:      types.OperationActionStart,
			WaitOptions: &types.WaitOptions{WaitSeconds: aws.Int32(30)},
		},
		{
			Id:     aws.String("u-cb"),
			Type:   types.OperationTypeCallback,
			Action: types.OperationActionStart,
			CallbackOptions: &types.CallbackOptions{
				TimeoutSeconds:          60,
				HeartbeatTimeoutSeconds: 15,
			},
		},
		{
			Id:     aws.String("u-inv"),
			Type:   types.OperationTypeChainedInvoke,
			Action: types.OperationActionStart,
			ChainedInvokeOptions: &types.ChainedInvokeOptions{
				FunctionName: aws.String("fn"),
				TenantId:     aws.String("tenant"),
			},
		},
		{
			Id:             aws.String("u-ctx"),
			Type:           types.OperationTypeContext,
			Action:         types.OperationActionSucceed,
			ContextOptions: &types.ContextOptions{ReplayChildren: aws.Bool(true)},
		},
	}
	if !reflect.DeepEqual(in.Updates, wantUpdates) {
		t.Errorf("Updates = %+v, want %+v", in.Updates, wantUpdates)
	}

	// The new token and every field of the returned execution state.
	if out.CheckpointToken != "token-2" {
		t.Errorf("CheckpointToken = %q, want %q", out.CheckpointToken, "token-2")
	}
	if want := sdkOperationsAllFields(start, end); !reflect.DeepEqual(out.NewExecutionState, want) {
		t.Errorf("NewExecutionState = %+v, want %+v", out.NewExecutionState, want)
	}
}

func TestServiceClientCheckpointNilSlices(t *testing.T) {
	api := &recordingAPI{
		checkpointOut: &lambdasvc.CheckpointDurableExecutionOutput{
			CheckpointToken:   aws.String("token-2"),
			NewExecutionState: &types.CheckpointUpdatedExecutionState{},
		},
	}

	out, err := newServiceClient(api).Checkpoint(context.Background(), durable.CheckpointInput{
		ExecutionArn:    "arn:exec",
		CheckpointToken: "token-1",
	})
	if err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}
	// nil Updates stay nil in the request, and a returned state without
	// operations maps to nil, as in the SDK's default client.
	if api.gotCheckpointIn.Updates != nil {
		t.Errorf("Updates = %+v, want nil", api.gotCheckpointIn.Updates)
	}
	if out.NewExecutionState != nil {
		t.Errorf("NewExecutionState = %+v, want nil", out.NewExecutionState)
	}

	// A response without a new execution state also maps to nil.
	api.checkpointOut = &lambdasvc.CheckpointDurableExecutionOutput{CheckpointToken: aws.String("token-3")}
	out, err = newServiceClient(api).Checkpoint(context.Background(), durable.CheckpointInput{})
	if err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}
	if out.NewExecutionState != nil || out.CheckpointToken != "token-3" {
		t.Errorf("Checkpoint() = %+v, want token-3 and no execution state", out)
	}
}

func TestServiceClientCheckpointError(t *testing.T) {
	wantErr := errors.New("stale token")
	_, err := newServiceClient(&recordingAPI{checkpointErr: wantErr}).Checkpoint(context.Background(), durable.CheckpointInput{})
	if !errors.Is(err, wantErr) {
		t.Errorf("Checkpoint() error = %v, want %v", err, wantErr)
	}
}
