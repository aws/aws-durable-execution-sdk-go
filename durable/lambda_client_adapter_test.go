// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdaservice "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// fakeLambdaAPI is a lambdaDurableAPI double. It records the wire inputs
// the adapter builds and serves canned wire outputs, so both directions
// of the adapter's conversions are observable.
type fakeLambdaAPI struct {
	gotStateIn *lambdaservice.GetDurableExecutionStateInput
	stateOut   *lambdaservice.GetDurableExecutionStateOutput
	stateErr   error

	gotCheckpointIn *lambdaservice.CheckpointDurableExecutionInput
	checkpointOut   *lambdaservice.CheckpointDurableExecutionOutput
	checkpointErr   error
}

func (f *fakeLambdaAPI) GetDurableExecutionState(_ context.Context, in *lambdaservice.GetDurableExecutionStateInput, _ ...func(*lambdaservice.Options)) (*lambdaservice.GetDurableExecutionStateOutput, error) {
	f.gotStateIn = in
	return f.stateOut, f.stateErr
}

func (f *fakeLambdaAPI) CheckpointDurableExecution(_ context.Context, in *lambdaservice.CheckpointDurableExecutionInput, _ ...func(*lambdaservice.Options)) (*lambdaservice.CheckpointDurableExecutionOutput, error) {
	f.gotCheckpointIn = in
	return f.checkpointOut, f.checkpointErr
}

// wireOperationsAllFields returns a wire operation of each type, together
// covering every field operationFromWire maps.
func wireOperationsAllFields(start, end time.Time) []types.Operation {
	return []types.Operation{
		{
			Id:             aws.String("op-exec"),
			Status:         types.OperationStatus("STARTED"),
			Type:           types.OperationType("EXECUTION"),
			SubType:        aws.String("sub"),
			Name:           aws.String("exec-name"),
			ParentId:       aws.String("parent-0"),
			StartTimestamp: aws.Time(start),
			EndTimestamp:   aws.Time(end),
			ExecutionDetails: &types.ExecutionDetails{
				InputPayload: aws.String(`"in"`),
			},
		},
		{
			Id:     aws.String("op-step"),
			Status: types.OperationStatus("PENDING"),
			Type:   types.OperationType("STEP"),
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
			Id:     aws.String("op-wait"),
			Status: types.OperationStatus("PENDING"),
			Type:   types.OperationType("WAIT"),
			WaitDetails: &types.WaitDetails{
				ScheduledEndTimestamp: aws.Time(end),
			},
		},
		{
			Id:     aws.String("op-cb"),
			Status: types.OperationStatus("STARTED"),
			Type:   types.OperationType("CALLBACK"),
			CallbackDetails: &types.CallbackDetails{
				CallbackId: aws.String("cb-1"),
				Result:     aws.String(`"cb"`),
				Error:      &types.ErrorObject{ErrorType: aws.String("CT")},
			},
		},
		{
			Id:     aws.String("op-inv"),
			Status: types.OperationStatus("SUCCEEDED"),
			Type:   types.OperationType("CHAINED_INVOKE"),
			ChainedInvokeDetails: &types.ChainedInvokeDetails{
				Result: aws.String(`"inv"`),
				Error:  &types.ErrorObject{ErrorMessage: aws.String("im")},
			},
		},
		{
			Id:     aws.String("op-ctx"),
			Status: types.OperationStatus("SUCCEEDED"),
			Type:   types.OperationType("CONTEXT"),
			ContextDetails: &types.ContextDetails{
				Result:         aws.String(`"c"`),
				ReplayChildren: aws.Bool(true),
				Error:          &types.ErrorObject{ErrorData: aws.String("cd")},
			},
		},
	}
}

// ownOperationsAllFields is the own-type image of wireOperationsAllFields.
func ownOperationsAllFields(start, end time.Time) []Operation {
	return []Operation{
		{
			Id:             aws.String("op-exec"),
			Status:         OperationStatusStarted,
			Type:           OperationTypeExecution,
			SubType:        aws.String("sub"),
			Name:           aws.String("exec-name"),
			ParentId:       aws.String("parent-0"),
			StartTimestamp: aws.Time(start),
			EndTimestamp:   aws.Time(end),
			ExecutionDetails: &ExecutionDetails{
				InputPayload: aws.String(`"in"`),
			},
		},
		{
			Id:     aws.String("op-step"),
			Status: OperationStatusPending,
			Type:   OperationTypeStep,
			StepDetails: &StepDetails{
				Attempt: 3,
				Result:  aws.String(`"r"`),
				Error: &ErrorObject{
					ErrorType:    aws.String("T"),
					ErrorMessage: aws.String("m"),
					ErrorData:    aws.String("d"),
					StackTrace:   []string{"f1", "f2"},
				},
				NextAttemptTimestamp: aws.Time(end),
			},
		},
		{
			Id:     aws.String("op-wait"),
			Status: OperationStatusPending,
			Type:   OperationTypeWait,
			WaitDetails: &WaitDetails{
				ScheduledEndTimestamp: aws.Time(end),
			},
		},
		{
			Id:     aws.String("op-cb"),
			Status: OperationStatusStarted,
			Type:   OperationTypeCallback,
			CallbackDetails: &CallbackDetails{
				CallbackId: aws.String("cb-1"),
				Result:     aws.String(`"cb"`),
				Error:      &ErrorObject{ErrorType: aws.String("CT")},
			},
		},
		{
			Id:     aws.String("op-inv"),
			Status: OperationStatusSucceeded,
			Type:   OperationTypeChainedInvoke,
			ChainedInvokeDetails: &ChainedInvokeDetails{
				Result: aws.String(`"inv"`),
				Error:  &ErrorObject{ErrorMessage: aws.String("im")},
			},
		},
		{
			Id:     aws.String("op-ctx"),
			Status: OperationStatusSucceeded,
			Type:   OperationTypeContext,
			ContextDetails: &ContextDetails{
				Result:         aws.String(`"c"`),
				ReplayChildren: aws.Bool(true),
				Error:          &ErrorObject{ErrorData: aws.String("cd")},
			},
		},
	}
}

func TestLambdaClientAdapterGetExecutionState(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := start.Add(time.Minute)
	fake := &fakeLambdaAPI{
		stateOut: &lambdaservice.GetDurableExecutionStateOutput{
			Operations: wireOperationsAllFields(start, end),
			NextMarker: aws.String("marker-2"),
		},
	}
	c := &lambdaExecutionClient{api: fake}

	out, err := c.GetExecutionState(context.Background(), GetExecutionStateInput{
		ExecutionArn:    "arn:exec",
		CheckpointToken: "token-1",
		Marker:          "marker-1",
	})
	if err != nil {
		t.Fatalf("GetExecutionState() error: %v", err)
	}

	// Request conversion: every input field reaches the wire.
	in := fake.gotStateIn
	if got := aws.ToString(in.DurableExecutionArn); got != "arn:exec" {
		t.Errorf("wire DurableExecutionArn = %q, want %q", got, "arn:exec")
	}
	if got := aws.ToString(in.CheckpointToken); got != "token-1" {
		t.Errorf("wire CheckpointToken = %q, want %q", got, "token-1")
	}
	if got := aws.ToString(in.Marker); got != "marker-1" {
		t.Errorf("wire Marker = %q, want %q", got, "marker-1")
	}

	// Response conversion: every operation field and the pagination marker.
	want := GetExecutionStateOutput{
		Operations: ownOperationsAllFields(start, end),
		NextMarker: "marker-2",
	}
	if !reflect.DeepEqual(out, want) {
		t.Errorf("GetExecutionState() = %+v, want %+v", out, want)
	}
}

func TestLambdaClientAdapterGetExecutionStateEmptyMarker(t *testing.T) {
	fake := &fakeLambdaAPI{
		stateOut: &lambdaservice.GetDurableExecutionStateOutput{},
	}
	c := &lambdaExecutionClient{api: fake}

	out, err := c.GetExecutionState(context.Background(), GetExecutionStateInput{
		ExecutionArn:    "arn:exec",
		CheckpointToken: "token-1",
	})
	if err != nil {
		t.Fatalf("GetExecutionState() error: %v", err)
	}
	// An empty Marker is omitted from the wire request, not sent as "".
	if fake.gotStateIn.Marker != nil {
		t.Errorf("wire Marker = %q, want nil", aws.ToString(fake.gotStateIn.Marker))
	}
	if out.Operations != nil {
		t.Errorf("Operations = %v, want nil", out.Operations)
	}
	if out.NextMarker != "" {
		t.Errorf("NextMarker = %q, want empty", out.NextMarker)
	}
}

func TestLambdaClientAdapterGetExecutionStateError(t *testing.T) {
	wantErr := errors.New("service unavailable")
	c := &lambdaExecutionClient{api: &fakeLambdaAPI{stateErr: wantErr}}

	_, err := c.GetExecutionState(context.Background(), GetExecutionStateInput{})
	if !errors.Is(err, wantErr) {
		t.Errorf("GetExecutionState() error = %v, want %v", err, wantErr)
	}
}

func TestLambdaClientAdapterCheckpoint(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := start.Add(time.Minute)
	fake := &fakeLambdaAPI{
		checkpointOut: &lambdaservice.CheckpointDurableExecutionOutput{
			CheckpointToken: aws.String("token-2"),
			NewExecutionState: &types.CheckpointUpdatedExecutionState{
				Operations: wireOperationsAllFields(start, end),
			},
		},
	}
	c := &lambdaExecutionClient{api: fake}

	// One update per option struct, together covering every field
	// updatesToWire maps.
	updates := []OperationUpdate{
		{
			Id:       aws.String("u-step"),
			Type:     OperationTypeStep,
			Action:   OperationActionRetry,
			SubType:  aws.String("sub"),
			Name:     aws.String("step-name"),
			ParentId: aws.String("parent-1"),
			Payload:  aws.String(`"p"`),
			Error: &ErrorObject{
				ErrorType:    aws.String("T"),
				ErrorMessage: aws.String("m"),
				ErrorData:    aws.String("d"),
				StackTrace:   []string{"f1"},
			},
			StepOptions: &StepOptions{NextAttemptDelaySeconds: aws.Int32(7)},
		},
		{
			Id:          aws.String("u-wait"),
			Type:        OperationTypeWait,
			Action:      OperationActionStart,
			WaitOptions: &WaitOptions{WaitSeconds: aws.Int32(30)},
		},
		{
			Id:     aws.String("u-cb"),
			Type:   OperationTypeCallback,
			Action: OperationActionStart,
			CallbackOptions: &CallbackOptions{
				TimeoutSeconds:          60,
				HeartbeatTimeoutSeconds: 15,
			},
		},
		{
			Id:     aws.String("u-inv"),
			Type:   OperationTypeChainedInvoke,
			Action: OperationActionStart,
			ChainedInvokeOptions: &ChainedInvokeOptions{
				FunctionName: aws.String("fn"),
				TenantId:     aws.String("tenant"),
			},
		},
		{
			Id:             aws.String("u-ctx"),
			Type:           OperationTypeContext,
			Action:         OperationActionSucceed,
			ContextOptions: &ContextOptions{ReplayChildren: aws.Bool(true)},
		},
	}

	out, err := c.Checkpoint(context.Background(), CheckpointInput{
		ExecutionArn:    "arn:exec",
		CheckpointToken: "token-1",
		Updates:         updates,
	})
	if err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}

	// Request conversion.
	in := fake.gotCheckpointIn
	if got := aws.ToString(in.DurableExecutionArn); got != "arn:exec" {
		t.Errorf("wire DurableExecutionArn = %q, want %q", got, "arn:exec")
	}
	if got := aws.ToString(in.CheckpointToken); got != "token-1" {
		t.Errorf("wire CheckpointToken = %q, want %q", got, "token-1")
	}
	wantWire := []types.OperationUpdate{
		{
			Id:       aws.String("u-step"),
			Type:     types.OperationType("STEP"),
			Action:   types.OperationAction("RETRY"),
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
			Type:        types.OperationType("WAIT"),
			Action:      types.OperationAction("START"),
			WaitOptions: &types.WaitOptions{WaitSeconds: aws.Int32(30)},
		},
		{
			Id:     aws.String("u-cb"),
			Type:   types.OperationType("CALLBACK"),
			Action: types.OperationAction("START"),
			CallbackOptions: &types.CallbackOptions{
				TimeoutSeconds:          60,
				HeartbeatTimeoutSeconds: 15,
			},
		},
		{
			Id:     aws.String("u-inv"),
			Type:   types.OperationType("CHAINED_INVOKE"),
			Action: types.OperationAction("START"),
			ChainedInvokeOptions: &types.ChainedInvokeOptions{
				FunctionName: aws.String("fn"),
				TenantId:     aws.String("tenant"),
			},
		},
		{
			Id:             aws.String("u-ctx"),
			Type:           types.OperationType("CONTEXT"),
			Action:         types.OperationAction("SUCCEED"),
			ContextOptions: &types.ContextOptions{ReplayChildren: aws.Bool(true)},
		},
	}
	if !reflect.DeepEqual(in.Updates, wantWire) {
		t.Errorf("wire Updates = %+v, want %+v", in.Updates, wantWire)
	}

	// Response conversion.
	if out.CheckpointToken != "token-2" {
		t.Errorf("CheckpointToken = %q, want %q", out.CheckpointToken, "token-2")
	}
	if want := ownOperationsAllFields(start, end); !reflect.DeepEqual(out.NewExecutionState, want) {
		t.Errorf("NewExecutionState = %+v, want %+v", out.NewExecutionState, want)
	}
}

func TestLambdaClientAdapterCheckpointNilOptionals(t *testing.T) {
	fake := &fakeLambdaAPI{
		checkpointOut: &lambdaservice.CheckpointDurableExecutionOutput{
			CheckpointToken: aws.String("token-2"),
		},
	}
	c := &lambdaExecutionClient{api: fake}

	out, err := c.Checkpoint(context.Background(), CheckpointInput{
		ExecutionArn:    "arn:exec",
		CheckpointToken: "token-1",
	})
	if err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}
	// nil Updates stays nil on the wire; option structs and Error are
	// only allocated when set; a missing NewExecutionState maps to nil.
	if fake.gotCheckpointIn.Updates != nil {
		t.Errorf("wire Updates = %+v, want nil", fake.gotCheckpointIn.Updates)
	}
	if out.NewExecutionState != nil {
		t.Errorf("NewExecutionState = %+v, want nil", out.NewExecutionState)
	}
	if out.CheckpointToken != "token-2" {
		t.Errorf("CheckpointToken = %q, want %q", out.CheckpointToken, "token-2")
	}
}

func TestLambdaClientAdapterCheckpointError(t *testing.T) {
	wantErr := errors.New("stale token")
	c := &lambdaExecutionClient{api: &fakeLambdaAPI{checkpointErr: wantErr}}

	_, err := c.Checkpoint(context.Background(), CheckpointInput{})
	if !errors.Is(err, wantErr) {
		t.Errorf("Checkpoint() error = %v, want %v", err, wantErr)
	}
}
