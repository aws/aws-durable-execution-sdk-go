// Command execution-client-custom demonstrates [durable.WithExecutionClient],
// which replaces the client the SDK uses to load and checkpoint execution
// state. The SDK's default client calls the AWS Lambda service with the
// AWS standard retryer, which is the only retry of a checkpoint call. This
// function supplies its own [durable.ExecutionClient] on the same service,
// built from an AWS config that allows more attempts, and the client
// reports each failure it records.
//
// serviceClient converts between the SDK's request and response types
// and the AWS SDK's Lambda types, and returns the service's errors
// unchanged. The SDK then classifies a failed checkpoint from the AWS
// error shape, as it does for the default client, and returns a
// [durable.CheckpointError] to the operation whose checkpoint failed. A
// client that does not produce AWS-shaped errors states the scope of each
// failure with [durable.ClientError] instead.
//
// For every operation update that records a failure, the client rebuilds
// the typed error with [durable.ErrorFromObject] and logs it. Matching on
// the rebuilt Go types replaces matching on ErrorType strings: a child
// context that failed because a step inside it failed records a StepError,
// which rebuilds as a *durable.StepError.
//
// The handler pays for an order by card inside a child context and falls
// back to an invoice when the card is declined, which it always is here,
// so every execution records two failures. When a checkpoint itself
// fails, the handler logs how far the failure reaches with
// [durable.CheckpointError.Scope], [durable.CheckpointError.Retryable],
// and [durable.IsCheckpointRetryable].
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// lambdaAPI is the part of the AWS SDK's Lambda client that serviceClient
// calls. *lambdasvc.Client satisfies it.
type lambdaAPI interface {
	GetDurableExecutionState(ctx context.Context, in *lambdasvc.GetDurableExecutionStateInput, opts ...func(*lambdasvc.Options)) (*lambdasvc.GetDurableExecutionStateOutput, error)
	CheckpointDurableExecution(ctx context.Context, in *lambdasvc.CheckpointDurableExecutionInput, opts ...func(*lambdasvc.Options)) (*lambdasvc.CheckpointDurableExecutionOutput, error)
}

// serviceClient is a durable.ExecutionClient on the Lambda service. Unlike
// the default client it sets no request timeouts and adds no user agent;
// a function that needs them configures them on its AWS config.
type serviceClient struct {
	api    lambdaAPI
	logger *slog.Logger
}

var _ durable.ExecutionClient = (*serviceClient)(nil)

func (c *serviceClient) GetExecutionState(ctx context.Context, in durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	req := &lambdasvc.GetDurableExecutionStateInput{
		DurableExecutionArn: aws.String(in.ExecutionArn),
		CheckpointToken:     aws.String(in.CheckpointToken),
	}
	if in.Marker != "" {
		req.Marker = aws.String(in.Marker)
	}
	resp, err := c.api.GetDurableExecutionState(ctx, req)
	if err != nil {
		return durable.GetExecutionStateOutput{}, err
	}
	return durable.GetExecutionStateOutput{
		Operations: operationsFromService(resp.Operations),
		NextMarker: aws.ToString(resp.NextMarker),
	}, nil
}

func (c *serviceClient) Checkpoint(ctx context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	for _, u := range in.Updates {
		if u.Action == durable.OperationActionFail {
			c.logFailure(ctx, u)
		}
	}

	resp, err := c.api.CheckpointDurableExecution(ctx, &lambdasvc.CheckpointDurableExecutionInput{
		DurableExecutionArn: aws.String(in.ExecutionArn),
		CheckpointToken:     aws.String(in.CheckpointToken),
		Updates:             updatesToService(in.Updates),
	})
	if err != nil {
		return durable.CheckpointOutput{}, err
	}
	out := durable.CheckpointOutput{CheckpointToken: aws.ToString(resp.CheckpointToken)}
	if resp.NewExecutionState != nil {
		out.NewExecutionState = operationsFromService(resp.NewExecutionState.Operations)
	}
	return out, nil
}

// logFailure logs a failure the update records. ErrorFromObject rebuilds
// the typed error from the record. Every rebuilt error matches
// *durable.OperationError, which carries the recorded type and message; a
// record of one of the SDK's own error types also matches that type.
func (c *serviceClient) logFailure(ctx context.Context, u durable.OperationUpdate) {
	err := durable.ErrorFromObject(u.Error)
	if err == nil {
		return
	}
	attrs := []any{"operation", aws.ToString(u.Name), "type", string(u.Type)}
	var opErr *durable.OperationError
	if errors.As(err, &opErr) {
		attrs = append(attrs, "errorType", opErr.ErrorType, "message", opErr.Message)
	}
	var stepErr *durable.StepError
	attrs = append(attrs, "stepFailure", errors.As(err, &stepErr))
	c.logger.WarnContext(ctx, "recording failure", attrs...)
}

// updatesToService converts operation updates to the AWS SDK's type,
// field for field.
func updatesToService(updates []durable.OperationUpdate) []types.OperationUpdate {
	out := make([]types.OperationUpdate, 0, len(updates))
	for _, u := range updates {
		s := types.OperationUpdate{
			Id:       u.Id,
			Type:     types.OperationType(u.Type),
			Action:   types.OperationAction(u.Action),
			SubType:  u.SubType,
			Name:     u.Name,
			ParentId: u.ParentId,
			Payload:  u.Payload,
			Error:    errorToService(u.Error),
		}
		if o := u.StepOptions; o != nil {
			s.StepOptions = &types.StepOptions{NextAttemptDelaySeconds: o.NextAttemptDelaySeconds}
		}
		if o := u.WaitOptions; o != nil {
			s.WaitOptions = &types.WaitOptions{WaitSeconds: o.WaitSeconds}
		}
		if o := u.CallbackOptions; o != nil {
			s.CallbackOptions = &types.CallbackOptions{
				TimeoutSeconds:          o.TimeoutSeconds,
				HeartbeatTimeoutSeconds: o.HeartbeatTimeoutSeconds,
			}
		}
		if o := u.ChainedInvokeOptions; o != nil {
			s.ChainedInvokeOptions = &types.ChainedInvokeOptions{FunctionName: o.FunctionName, TenantId: o.TenantId}
		}
		if o := u.ContextOptions; o != nil {
			s.ContextOptions = &types.ContextOptions{ReplayChildren: o.ReplayChildren}
		}
		out = append(out, s)
	}
	return out
}

// operationsFromService converts the AWS SDK's operations to the SDK's
// type, field for field.
func operationsFromService(ops []types.Operation) []durable.Operation {
	out := make([]durable.Operation, 0, len(ops))
	for _, op := range ops {
		o := durable.Operation{
			Id:             op.Id,
			Status:         durable.OperationStatus(op.Status),
			Type:           durable.OperationType(op.Type),
			SubType:        op.SubType,
			Name:           op.Name,
			ParentId:       op.ParentId,
			StartTimestamp: op.StartTimestamp,
			EndTimestamp:   op.EndTimestamp,
		}
		if d := op.ExecutionDetails; d != nil {
			o.ExecutionDetails = &durable.ExecutionDetails{InputPayload: d.InputPayload}
		}
		if d := op.StepDetails; d != nil {
			o.StepDetails = &durable.StepDetails{
				Attempt:              d.Attempt,
				Result:               d.Result,
				Error:                errorFromService(d.Error),
				NextAttemptTimestamp: d.NextAttemptTimestamp,
			}
		}
		if d := op.WaitDetails; d != nil {
			o.WaitDetails = &durable.WaitDetails{ScheduledEndTimestamp: d.ScheduledEndTimestamp}
		}
		if d := op.CallbackDetails; d != nil {
			o.CallbackDetails = &durable.CallbackDetails{CallbackId: d.CallbackId, Result: d.Result, Error: errorFromService(d.Error)}
		}
		if d := op.ChainedInvokeDetails; d != nil {
			o.ChainedInvokeDetails = &durable.ChainedInvokeDetails{Result: d.Result, Error: errorFromService(d.Error)}
		}
		if d := op.ContextDetails; d != nil {
			o.ContextDetails = &durable.ContextDetails{Result: d.Result, ReplayChildren: d.ReplayChildren, Error: errorFromService(d.Error)}
		}
		out = append(out, o)
	}
	return out
}

func errorToService(e *durable.ErrorObject) *types.ErrorObject {
	if e == nil {
		return nil
	}
	return &types.ErrorObject{ErrorType: e.ErrorType, ErrorMessage: e.ErrorMessage, ErrorData: e.ErrorData, StackTrace: e.StackTrace}
}

func errorFromService(e *types.ErrorObject) *durable.ErrorObject {
	if e == nil {
		return nil
	}
	return &durable.ErrorObject{ErrorType: e.ErrorType, ErrorMessage: e.ErrorMessage, ErrorData: e.ErrorData, StackTrace: e.StackTrace}
}

// Input is the order to pay for.
type Input struct {
	OrderID string `json:"orderId"`
}

// Output reports how the order was paid.
type Output struct {
	OrderID string `json:"orderId"`
	PaidBy  string `json:"paidBy"`
}

// errCardDeclined is the card payment's failure. The example's card is
// always declined, so the invoice fallback always runs.
var errCardDeclined = errors.New("card declined")

// noRetry makes a declined card fail its step on the first attempt.
var noRetry = durable.MustNewRetryStrategy(durable.RetryConfig{MaxAttempts: 1})

// checkpointFailed reports whether err is a failed checkpoint and, if it
// is, logs how far the failure reaches. The SDK acts on the scope whatever
// the handler returns; the handler can only report it.
func checkpointFailed(ctx durable.Context, err error) bool {
	var ce *durable.CheckpointError
	if !errors.As(err, &ce) {
		return false
	}
	msg := "checkpoint failed"
	switch ce.Scope() {
	case durable.ErrorScopeInvocation:
		msg = "checkpoint failed, the execution resumes in a new invocation"
	case durable.ErrorScopeExecution:
		msg = "checkpoint failed, the execution will fail"
	}
	level := slog.LevelError
	if durable.IsCheckpointRetryable(err) {
		level = slog.LevelWarn
	}
	ctx.Logger().Log(ctx, level, msg, "scope", ce.Scope(), "retryable", ce.Retryable())
	return true
}

func handler(ctx durable.Context, in Input) (Output, error) {
	_, err := durable.RunInChildContext(ctx, "pay-by-card", func(cctx durable.Context) (string, error) {
		return durable.Step(cctx, "charge-card", func(_ durable.StepContext) (string, error) {
			return "", errCardDeclined
		}, durable.WithRetry(noRetry))
	})
	if err == nil {
		return Output{OrderID: in.OrderID, PaidBy: "card"}, nil
	}
	if checkpointFailed(ctx, err) {
		return Output{}, err
	}

	// The card was declined: send an invoice instead.
	if _, err := durable.Step(ctx, "send-invoice", func(_ durable.StepContext) (string, error) {
		return "invoice sent for " + in.OrderID, nil
	}); err != nil {
		checkpointFailed(ctx, err)
		return Output{}, err
	}
	return Output{OrderID: in.OrderID, PaidBy: "invoice"}, nil
}

func main() {
	// Five attempts per call instead of the standard retryer's three.
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRetryMaxAttempts(5))
	if err != nil {
		log.Fatalf("load AWS config: %v", err)
	}
	client := &serviceClient{
		api:    lambdasvc.NewFromConfig(cfg),
		logger: slog.New(slog.NewJSONHandler(os.Stderr, nil)),
	}
	durable.Start(handler, durable.WithExecutionClient(client))
}
