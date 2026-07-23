// Package awssdk provides the production checkpoint.Client implementation,
// backed by the real AWS SDK for Go v2's Lambda client
// (github.com/aws/aws-sdk-go-v2/service/lambda), rather than the two
// stopgaps this repo carried while that module was unreachable in this
// development environment (see awscli's and sigv4lambda's package docs
// for the full history):
//
//   - pkg/durable/awscli: shells out to the `aws` CLI per checkpoint call.
//   - pkg/durable/sigv4lambda: hand-rolled SigV4 signing against the raw
//     REST endpoints, with an unresolved signature-mismatch bug.
//
// Network access to the Go module proxy was re-verified as available via
// GOPROXY=direct (proxy.golang.org itself remains unreachable, but direct
// git/HTTPS fetches of the module source succeed) - see
// docs/remaining-work.md task 20/21's resolution note for how this was
// confirmed. This package supersedes both stopgaps for production use;
// they are retained only as documented historical/fallback options (see
// task 21's disposition), not because this package is incomplete.
//
// # Verified against the same request/response shapes
//
// github.com/aws/aws-sdk-go-v2/service/lambda/types (as of the pinned
// version in go.mod) defines OperationUpdate/Operation/StepDetails/
// WaitDetails/CallbackDetails/ChainedInvokeDetails/ContextDetails/
// ErrorObject/OperationStatus/OperationType/OperationAction with the
// EXACT same field and enum-value names this repo's own types/wire.go
// already verified independently against the official AWS API Reference
// and live traffic - confirming both were built against the same source
// of truth. The only field-shape difference is that the SDK v2 types use
// pointers for every optional string/int32/bool field (idiomatic for
// smithy-go-generated code) where types/wire.go uses a mix of value and
// pointer fields - translated explicitly in this package's
// toSDKUpdate/fromSDKOperation helpers (and their smaller per-field
// helpers) rather than relying on any implicit conversion.
//
// One real gap this package's implementation surfaced in types/wire.go
// while writing the translation: OperationStatus was missing READY (the
// SDK v2 types package's OperationStatus enum has eight values -
// STARTED, PENDING, READY, SUCCEEDED, FAILED, CANCELLED, TIMED_OUT,
// STOPPED - this repo's wire.go only had seven, missing READY). Fixed as
// part of this same change; see wire.go's updated doc comment.
package awssdk

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// LambdaAPI is the subset of *lambda.Client this package depends on,
// exposed as an interface so tests can substitute a fake without needing
// real AWS credentials or network access - the SDK v2 client itself
// satisfies this directly.
type LambdaAPI interface {
	CheckpointDurableExecution(ctx context.Context, params *lambda.CheckpointDurableExecutionInput, optFns ...func(*lambda.Options)) (*lambda.CheckpointDurableExecutionOutput, error)
}

// Client is the production checkpoint.Client implementation, calling the
// real Lambda CheckpointDurableExecution API via the AWS SDK for Go v2.
type Client struct {
	// API is the underlying Lambda client. If nil, New must be used to
	// construct a Client with a real *lambda.Client resolved from the
	// standard credential/region chain (matching every other AWS SDK's
	// default behavior - environment variables, shared config/credentials
	// files, EC2/ECS/Lambda execution-role metadata, in that order).
	API LambdaAPI
}

// New constructs a Client backed by a real *lambda.Client, resolving
// credentials and region via the standard AWS SDK for Go v2 default
// chain (github.com/aws/aws-sdk-go-v2/config.LoadDefaultConfig) - the
// same resolution order every other AWS SDK uses, and in particular the
// one that correctly picks up a Lambda function's execution-role
// credentials from the standard AWS_* environment variables the Lambda
// runtime sets, exactly like awscli's and sigv4lambda's stopgap clients
// did, but via the SDK's own well-tested credential provider chain
// instead of either shelling out or hand-rolling SigV4 signing.
//
// optFns are passed through to config.LoadDefaultConfig, letting callers
// override the region or supply explicit credentials if needed (e.g. for
// local testing against a real account without relying on ambient
// environment variables).
func New(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (*Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("awssdk.New: loading AWS config: %w", err)
	}
	return &Client{API: lambda.NewFromConfig(cfg)}, nil
}

// Checkpoint calls the real Lambda CheckpointDurableExecution API,
// translating between this SDK's own types.OperationUpdate/
// types.Operation (verified independently against the official API
// Reference, per types/wire.go's doc) and the AWS SDK for Go v2's
// equivalent lambdatypes.OperationUpdate/lambdatypes.Operation - the two
// shapes are field-for-field identical in substance (see this package's
// doc), so this translation is purely about the value/pointer field
// convention difference, not a semantic reinterpretation.
func (c *Client) Checkpoint(ctx context.Context, req types.CheckpointDurableExecutionRequest) (*types.CheckpointDurableExecutionResponse, error) {
	if c.API == nil {
		return nil, fmt.Errorf("awssdk.Client.Checkpoint: API is nil - construct Client via awssdk.New, or set API explicitly for testing")
	}

	input := &lambda.CheckpointDurableExecutionInput{
		DurableExecutionArn: aws.String(req.DurableExecutionArn),
		CheckpointToken:     aws.String(req.CheckpointToken),
		ClientToken:         req.ClientToken,
	}
	if len(req.Updates) > 0 {
		input.Updates = make([]lambdatypes.OperationUpdate, len(req.Updates))
		for i, u := range req.Updates {
			input.Updates[i] = toSDKUpdate(u)
		}
	}

	output, err := c.API.CheckpointDurableExecution(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("awssdk.Client.Checkpoint: %w", err)
	}

	resp := &types.CheckpointDurableExecutionResponse{
		NextCheckpointToken: output.CheckpointToken,
	}
	if output.NewExecutionState != nil {
		resp.UpdatedOperations = make([]types.Operation, len(output.NewExecutionState.Operations))
		for i, op := range output.NewExecutionState.Operations {
			resp.UpdatedOperations[i] = fromSDKOperation(op)
		}
	}
	return resp, nil
}

// toSDKUpdate translates this SDK's types.OperationUpdate into the AWS
// SDK for Go v2's lambdatypes.OperationUpdate, field by field.
func toSDKUpdate(u types.OperationUpdate) lambdatypes.OperationUpdate {
	out := lambdatypes.OperationUpdate{
		Id:      aws.String(u.ID),
		Type:    lambdatypes.OperationType(u.Type),
		Action:  lambdatypes.OperationAction(u.Action),
		Name:    stringOrNil(u.Name),
		SubType: stringOrNil(u.SubType),
		Payload: u.Payload,
		Error:   toSDKErrorObject(u.Error),
	}
	if u.ParentID != "" {
		out.ParentId = aws.String(u.ParentID)
	}
	if u.StepOptions != nil {
		out.StepOptions = &lambdatypes.StepOptions{NextAttemptDelaySeconds: int32PtrFromIntPtr(u.StepOptions.NextAttemptDelaySeconds)}
	}
	if u.WaitOptions != nil {
		out.WaitOptions = &lambdatypes.WaitOptions{WaitSeconds: int32PtrFromIntPtr(u.WaitOptions.WaitSeconds)}
	}
	if u.CallbackOptions != nil {
		out.CallbackOptions = &lambdatypes.CallbackOptions{
			TimeoutSeconds:          int32FromIntPtr(u.CallbackOptions.TimeoutSeconds),
			HeartbeatTimeoutSeconds: int32FromIntPtr(u.CallbackOptions.HeartbeatTimeoutSeconds),
		}
	}
	if u.ChainedInvokeOptions != nil {
		out.ChainedInvokeOptions = &lambdatypes.ChainedInvokeOptions{
			FunctionName: aws.String(u.ChainedInvokeOptions.FunctionName),
			TenantId:     stringOrNil(u.ChainedInvokeOptions.TenantID),
		}
	}
	if u.ContextOptions != nil {
		out.ContextOptions = &lambdatypes.ContextOptions{ReplayChildren: aws.Bool(u.ContextOptions.ReplayChildren)}
	}
	return out
}

// fromSDKOperation translates the AWS SDK for Go v2's lambdatypes.Operation
// (as returned in a CheckpointDurableExecutionOutput.NewExecutionState)
// back into this SDK's own types.Operation, the inverse of toSDKUpdate.
func fromSDKOperation(op lambdatypes.Operation) types.Operation {
	out := types.Operation{
		ID:       aws.ToString(op.Id),
		Type:     types.OperationType(op.Type),
		Status:   types.OperationStatus(op.Status),
		Name:     aws.ToString(op.Name),
		SubType:  aws.ToString(op.SubType),
		ParentID: aws.ToString(op.ParentId),

		StartTimestamp: toWireTime(op.StartTimestamp),
		EndTimestamp:   toWireTime(op.EndTimestamp),
	}
	if op.StepDetails != nil {
		out.StepDetails = &types.StepDetails{
			Attempt:              int(op.StepDetails.Attempt),
			Result:               op.StepDetails.Result,
			Error:                fromSDKErrorObject(op.StepDetails.Error),
			NextAttemptTimestamp: toWireTime(op.StepDetails.NextAttemptTimestamp),
		}
	}
	if op.WaitDetails != nil {
		out.WaitDetails = &types.WaitDetails{ScheduledEndTimestamp: toWireTime(op.WaitDetails.ScheduledEndTimestamp)}
	}
	if op.CallbackDetails != nil {
		out.CallbackDetails = &types.CallbackDetails{
			CallbackID: aws.ToString(op.CallbackDetails.CallbackId),
			Result:     op.CallbackDetails.Result,
			Error:      fromSDKErrorObject(op.CallbackDetails.Error),
		}
	}
	if op.ChainedInvokeDetails != nil {
		out.ChainedInvokeDetails = &types.ChainedInvokeDetails{
			Result: op.ChainedInvokeDetails.Result,
			Error:  fromSDKErrorObject(op.ChainedInvokeDetails.Error),
		}
	}
	if op.ContextDetails != nil {
		out.ContextDetails = &types.ContextDetails{
			Result:         op.ContextDetails.Result,
			Error:          fromSDKErrorObject(op.ContextDetails.Error),
			ReplayChildren: aws.ToBool(op.ContextDetails.ReplayChildren),
		}
	}
	if op.ExecutionDetails != nil {
		out.ExecutionDetails = &types.ExecutionDetails{InputPayload: op.ExecutionDetails.InputPayload}
	}
	return out
}

func toSDKErrorObject(e *types.ErrorObject) *lambdatypes.ErrorObject {
	if e == nil {
		return nil
	}
	return &lambdatypes.ErrorObject{
		ErrorMessage: stringOrNil(e.ErrorMessage),
		ErrorType:    stringOrNil(e.ErrorType),
		ErrorData:    stringOrNil(e.ErrorData),
		StackTrace:   e.StackTrace,
	}
}

func fromSDKErrorObject(e *lambdatypes.ErrorObject) *types.ErrorObject {
	if e == nil {
		return nil
	}
	return &types.ErrorObject{
		ErrorMessage: aws.ToString(e.ErrorMessage),
		ErrorType:    aws.ToString(e.ErrorType),
		ErrorData:    aws.ToString(e.ErrorData),
		StackTrace:   e.StackTrace,
	}
}

// toWireTime converts the SDK v2's *time.Time into this repo's own
// *types.Time wrapper (see wire.go's doc on why Time exists: it accepts
// either of the two timestamp encodings confirmed present on the real
// wire). The SDK v2 client already parses timestamps into time.Time
// itself as part of its own response deserialization, so no re-parsing
// of a raw encoding is needed here - just wrapping the already-parsed
// value.
func toWireTime(t *time.Time) *types.Time {
	if t == nil {
		return nil
	}
	return &types.Time{Time: *t}
}

func stringOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return aws.String(s)
}

func int32PtrFromIntPtr(p *int) *int32 {
	if p == nil {
		return nil
	}
	v := int32(*p)
	return &v
}

func int32FromIntPtr(p *int) int32 {
	if p == nil {
		return 0
	}
	return int32(*p)
}
