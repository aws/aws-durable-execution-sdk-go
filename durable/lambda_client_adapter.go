// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// This file is the only place in the package that names the AWS SDK's
// generated Lambda types. It adapts [ExecutionClient] calls onto the AWS
// Lambda service client, keeping the generated request and response
// shapes out of the exported surface.

package durable

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/config"
	lambdaservice "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	smithymw "github.com/aws/smithy-go/middleware"
)

// lambdaDurableAPI is the subset of the AWS Lambda service client that the
// adapter consumes. Satisfied by [lambdaservice.Client].
type lambdaDurableAPI interface {
	GetDurableExecutionState(ctx context.Context, in *lambdaservice.GetDurableExecutionStateInput, opts ...func(*lambdaservice.Options)) (*lambdaservice.GetDurableExecutionStateOutput, error)
	CheckpointDurableExecution(ctx context.Context, in *lambdaservice.CheckpointDurableExecutionInput, opts ...func(*lambdaservice.Options)) (*lambdaservice.CheckpointDurableExecutionOutput, error)
}

// lambdaExecutionClient adapts the AWS Lambda service client to
// [ExecutionClient].
type lambdaExecutionClient struct {
	api lambdaDurableAPI
}

var _ ExecutionClient = (*lambdaExecutionClient)(nil)

// defaultExecutionClient builds the default [ExecutionClient] from the
// default AWS config, tagging requests with the SDK's user agent.
func defaultExecutionClient(ctx context.Context) (ExecutionClient, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithAPIOptions([]func(*smithymw.Stack) error{
			awsmiddleware.AddUserAgentKeyValue(userAgentKey, Version),
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("durable: load AWS config: %w", err)
	}
	return &lambdaExecutionClient{api: lambdaservice.NewFromConfig(cfg)}, nil
}

func (c *lambdaExecutionClient) GetExecutionState(ctx context.Context, in GetExecutionStateInput) (GetExecutionStateOutput, error) {
	wireIn := &lambdaservice.GetDurableExecutionStateInput{
		DurableExecutionArn: aws.String(in.ExecutionArn),
		CheckpointToken:     aws.String(in.CheckpointToken),
	}
	if in.Marker != "" {
		wireIn.Marker = aws.String(in.Marker)
	}
	wireOut, err := c.api.GetDurableExecutionState(ctx, wireIn)
	if err != nil {
		return GetExecutionStateOutput{}, err
	}
	out := GetExecutionStateOutput{
		Operations: operationsFromWire(wireOut.Operations),
		NextMarker: aws.ToString(wireOut.NextMarker),
	}
	return out, nil
}

func (c *lambdaExecutionClient) Checkpoint(ctx context.Context, in CheckpointInput) (CheckpointOutput, error) {
	wireIn := &lambdaservice.CheckpointDurableExecutionInput{
		DurableExecutionArn: aws.String(in.ExecutionArn),
		CheckpointToken:     aws.String(in.CheckpointToken),
		Updates:             updatesToWire(in.Updates),
	}
	wireOut, err := c.api.CheckpointDurableExecution(ctx, wireIn)
	if err != nil {
		return CheckpointOutput{}, err
	}
	out := CheckpointOutput{
		CheckpointToken: aws.ToString(wireOut.CheckpointToken),
	}
	if wireOut.NewExecutionState != nil {
		out.NewExecutionState = operationsFromWire(wireOut.NewExecutionState.Operations)
	}
	return out, nil
}

// updatesToWire converts operation updates to the generated wire type.
func updatesToWire(updates []OperationUpdate) []types.OperationUpdate {
	if updates == nil {
		return nil
	}
	wire := make([]types.OperationUpdate, 0, len(updates))
	for _, u := range updates {
		w := types.OperationUpdate{
			Id:       u.Id,
			Type:     types.OperationType(u.Type),
			Action:   types.OperationAction(u.Action),
			SubType:  u.SubType,
			Name:     u.Name,
			ParentId: u.ParentId,
			Payload:  u.Payload,
			Error:    errorObjectToWire(u.Error),
		}
		if o := u.StepOptions; o != nil {
			w.StepOptions = &types.StepOptions{NextAttemptDelaySeconds: o.NextAttemptDelaySeconds}
		}
		if o := u.WaitOptions; o != nil {
			w.WaitOptions = &types.WaitOptions{WaitSeconds: o.WaitSeconds}
		}
		if o := u.CallbackOptions; o != nil {
			w.CallbackOptions = &types.CallbackOptions{
				TimeoutSeconds:          o.TimeoutSeconds,
				HeartbeatTimeoutSeconds: o.HeartbeatTimeoutSeconds,
			}
		}
		if o := u.ChainedInvokeOptions; o != nil {
			w.ChainedInvokeOptions = &types.ChainedInvokeOptions{
				FunctionName: o.FunctionName,
				TenantId:     o.TenantId,
			}
		}
		if o := u.ContextOptions; o != nil {
			w.ContextOptions = &types.ContextOptions{ReplayChildren: o.ReplayChildren}
		}
		wire = append(wire, w)
	}
	return wire
}

// operationsFromWire converts generated wire operations to the SDK's own
// operation records.
func operationsFromWire(ops []types.Operation) []Operation {
	if ops == nil {
		return nil
	}
	out := make([]Operation, 0, len(ops))
	for _, op := range ops {
		out = append(out, operationFromWire(op))
	}
	return out
}

func operationFromWire(op types.Operation) Operation {
	o := Operation{
		Id:             op.Id,
		Status:         OperationStatus(op.Status),
		Type:           OperationType(op.Type),
		SubType:        op.SubType,
		Name:           op.Name,
		ParentId:       op.ParentId,
		StartTimestamp: op.StartTimestamp,
		EndTimestamp:   op.EndTimestamp,
	}
	if d := op.ExecutionDetails; d != nil {
		o.ExecutionDetails = &ExecutionDetails{InputPayload: d.InputPayload}
	}
	if d := op.StepDetails; d != nil {
		o.StepDetails = &StepDetails{
			Attempt:              d.Attempt,
			Result:               d.Result,
			Error:                errorObjectFromWire(d.Error),
			NextAttemptTimestamp: d.NextAttemptTimestamp,
		}
	}
	if d := op.WaitDetails; d != nil {
		o.WaitDetails = &WaitDetails{ScheduledEndTimestamp: d.ScheduledEndTimestamp}
	}
	if d := op.CallbackDetails; d != nil {
		o.CallbackDetails = &CallbackDetails{
			CallbackId: d.CallbackId,
			Result:     d.Result,
			Error:      errorObjectFromWire(d.Error),
		}
	}
	if d := op.ChainedInvokeDetails; d != nil {
		o.ChainedInvokeDetails = &ChainedInvokeDetails{
			Result: d.Result,
			Error:  errorObjectFromWire(d.Error),
		}
	}
	if d := op.ContextDetails; d != nil {
		o.ContextDetails = &ContextDetails{
			Result:         d.Result,
			ReplayChildren: d.ReplayChildren,
			Error:          errorObjectFromWire(d.Error),
		}
	}
	return o
}

func errorObjectToWire(e *ErrorObject) *types.ErrorObject {
	if e == nil {
		return nil
	}
	return &types.ErrorObject{
		ErrorType:    e.ErrorType,
		ErrorMessage: e.ErrorMessage,
		ErrorData:    e.ErrorData,
		StackTrace:   e.StackTrace,
	}
}

func errorObjectFromWire(e *types.ErrorObject) *ErrorObject {
	if e == nil {
		return nil
	}
	return &ErrorObject{
		ErrorType:    e.ErrorType,
		ErrorMessage: e.ErrorMessage,
		ErrorData:    e.ErrorData,
		StackTrace:   e.StackTrace,
	}
}
