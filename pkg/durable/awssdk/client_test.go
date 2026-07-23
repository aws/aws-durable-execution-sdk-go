package awssdk

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// fakeLambdaAPI is a test double for LambdaAPI, letting these tests
// verify the request/response translation without real AWS credentials
// or network access.
type fakeLambdaAPI struct {
	lastInput *lambda.CheckpointDurableExecutionInput
	output    *lambda.CheckpointDurableExecutionOutput
	err       error
}

func (f *fakeLambdaAPI) CheckpointDurableExecution(ctx context.Context, params *lambda.CheckpointDurableExecutionInput, optFns ...func(*lambda.Options)) (*lambda.CheckpointDurableExecutionOutput, error) {
	f.lastInput = params
	if f.err != nil {
		return nil, f.err
	}
	return f.output, nil
}

func TestClient_Checkpoint_RequestTranslation(t *testing.T) {
	fake := &fakeLambdaAPI{
		output: &lambda.CheckpointDurableExecutionOutput{
			CheckpointToken: aws.String("next-token"),
		},
	}
	c := &Client{API: fake}

	payload := `{"value":42}`
	_, err := c.Checkpoint(context.Background(), types.CheckpointDurableExecutionRequest{
		DurableExecutionArn: "arn:aws:lambda:us-east-1:123456789012:function:my-func:exec-1",
		CheckpointToken:     "token-0",
		Updates: []types.OperationUpdate{
			{
				ID:      "1",
				Type:    types.OperationTypeStep,
				Name:    "my-step",
				Action:  types.OperationActionSucceed,
				Payload: &payload,
			},
		},
	})
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	if fake.lastInput == nil {
		t.Fatal("expected CheckpointDurableExecution to be called")
	}
	if aws.ToString(fake.lastInput.DurableExecutionArn) != "arn:aws:lambda:us-east-1:123456789012:function:my-func:exec-1" {
		t.Errorf("unexpected DurableExecutionArn: %s", aws.ToString(fake.lastInput.DurableExecutionArn))
	}
	if aws.ToString(fake.lastInput.CheckpointToken) != "token-0" {
		t.Errorf("unexpected CheckpointToken: %s", aws.ToString(fake.lastInput.CheckpointToken))
	}
	if len(fake.lastInput.Updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(fake.lastInput.Updates))
	}
	u := fake.lastInput.Updates[0]
	if aws.ToString(u.Id) != "1" {
		t.Errorf("unexpected update Id: %s", aws.ToString(u.Id))
	}
	if u.Type != lambdatypes.OperationTypeStep {
		t.Errorf("unexpected update Type: %s", u.Type)
	}
	if u.Action != lambdatypes.OperationActionSucceed {
		t.Errorf("unexpected update Action: %s", u.Action)
	}
	if aws.ToString(u.Name) != "my-step" {
		t.Errorf("unexpected update Name: %s", aws.ToString(u.Name))
	}
	if aws.ToString(u.Payload) != payload {
		t.Errorf("unexpected update Payload: %s", aws.ToString(u.Payload))
	}
}

func TestClient_Checkpoint_ResponseTranslation(t *testing.T) {
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	result := `{"status":"ok"}`

	fake := &fakeLambdaAPI{
		output: &lambda.CheckpointDurableExecutionOutput{
			CheckpointToken: aws.String("next-token"),
			NewExecutionState: &lambdatypes.CheckpointUpdatedExecutionState{
				Operations: []lambdatypes.Operation{
					{
						Id:             aws.String("1"),
						Type:           lambdatypes.OperationTypeStep,
						Status:         lambdatypes.OperationStatusSucceeded,
						Name:           aws.String("my-step"),
						StartTimestamp: &now,
						StepDetails: &lambdatypes.StepDetails{
							Attempt: 2,
							Result:  &result,
						},
					},
				},
			},
		},
	}
	c := &Client{API: fake}

	resp, err := c.Checkpoint(context.Background(), types.CheckpointDurableExecutionRequest{
		DurableExecutionArn: "arn:test",
		CheckpointToken:     "token-0",
	})
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	if resp.NextCheckpointToken == nil || *resp.NextCheckpointToken != "next-token" {
		t.Fatalf("expected NextCheckpointToken 'next-token', got %v", resp.NextCheckpointToken)
	}
	if len(resp.UpdatedOperations) != 1 {
		t.Fatalf("expected 1 updated operation, got %d", len(resp.UpdatedOperations))
	}
	op := resp.UpdatedOperations[0]
	if op.ID != "1" {
		t.Errorf("unexpected op.ID: %s", op.ID)
	}
	if op.Type != types.OperationTypeStep {
		t.Errorf("unexpected op.Type: %s", op.Type)
	}
	if op.Status != types.OperationStatusSucceeded {
		t.Errorf("unexpected op.Status: %s", op.Status)
	}
	if op.Name != "my-step" {
		t.Errorf("unexpected op.Name: %s", op.Name)
	}
	if op.StartTimestamp == nil || !op.StartTimestamp.Time.Equal(now) {
		t.Errorf("unexpected op.StartTimestamp: %v", op.StartTimestamp)
	}
	if op.StepDetails == nil {
		t.Fatal("expected StepDetails to be populated")
	}
	if op.StepDetails.Attempt != 2 {
		t.Errorf("unexpected StepDetails.Attempt: %d", op.StepDetails.Attempt)
	}
	if op.StepDetails.Result == nil || *op.StepDetails.Result != result {
		t.Errorf("unexpected StepDetails.Result: %v", op.StepDetails.Result)
	}
}

func TestClient_Checkpoint_ErrorObjectTranslation(t *testing.T) {
	fake := &fakeLambdaAPI{
		output: &lambda.CheckpointDurableExecutionOutput{
			CheckpointToken: aws.String("next-token"),
			NewExecutionState: &lambdatypes.CheckpointUpdatedExecutionState{
				Operations: []lambdatypes.Operation{
					{
						Id:     aws.String("1"),
						Type:   lambdatypes.OperationTypeStep,
						Status: lambdatypes.OperationStatusFailed,
						StepDetails: &lambdatypes.StepDetails{
							Error: &lambdatypes.ErrorObject{
								ErrorMessage: aws.String("boom"),
								ErrorType:    aws.String("CustomError"),
							},
						},
					},
				},
			},
		},
	}
	c := &Client{API: fake}

	errMsg := "boom"
	errType := "CustomError"
	resp, err := c.Checkpoint(context.Background(), types.CheckpointDurableExecutionRequest{
		DurableExecutionArn: "arn:test",
		CheckpointToken:     "token-0",
		Updates: []types.OperationUpdate{
			{
				ID:     "1",
				Type:   types.OperationTypeStep,
				Action: types.OperationActionFail,
				Error:  &types.ErrorObject{ErrorMessage: errMsg, ErrorType: errType},
			},
		},
	})
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	if fake.lastInput.Updates[0].Error == nil {
		t.Fatal("expected request Error to be translated")
	}
	if aws.ToString(fake.lastInput.Updates[0].Error.ErrorMessage) != errMsg {
		t.Errorf("unexpected request Error.ErrorMessage: %s", aws.ToString(fake.lastInput.Updates[0].Error.ErrorMessage))
	}

	op := resp.UpdatedOperations[0]
	if op.StepDetails.Error == nil {
		t.Fatal("expected response StepDetails.Error to be translated")
	}
	if op.StepDetails.Error.ErrorMessage != "boom" {
		t.Errorf("unexpected response Error.ErrorMessage: %s", op.StepDetails.Error.ErrorMessage)
	}
	if op.StepDetails.Error.ErrorType != "CustomError" {
		t.Errorf("unexpected response Error.ErrorType: %s", op.StepDetails.Error.ErrorType)
	}
}

func TestClient_Checkpoint_AllOptionsTypesTranslated(t *testing.T) {
	fake := &fakeLambdaAPI{
		output: &lambda.CheckpointDurableExecutionOutput{
			CheckpointToken: aws.String("next-token"),
		},
	}
	c := &Client{API: fake}

	waitSeconds := 30
	nextAttemptDelay := 5
	timeoutSeconds := 60
	heartbeatSeconds := 10

	_, err := c.Checkpoint(context.Background(), types.CheckpointDurableExecutionRequest{
		DurableExecutionArn: "arn:test",
		CheckpointToken:     "token-0",
		Updates: []types.OperationUpdate{
			{ID: "1", Type: types.OperationTypeWait, Action: types.OperationActionStart, WaitOptions: &types.WaitOptions{WaitSeconds: &waitSeconds}},
			{ID: "2", Type: types.OperationTypeStep, Action: types.OperationActionRetry, StepOptions: &types.StepOptions{NextAttemptDelaySeconds: &nextAttemptDelay}},
			{ID: "3", Type: types.OperationTypeCallback, Action: types.OperationActionStart, CallbackOptions: &types.CallbackOptions{TimeoutSeconds: &timeoutSeconds, HeartbeatTimeoutSeconds: &heartbeatSeconds}},
			{ID: "4", Type: types.OperationTypeChainedInvoke, Action: types.OperationActionStart, ChainedInvokeOptions: &types.ChainedInvokeOptions{FunctionName: "target-fn", TenantID: "tenant-1"}},
			{ID: "5", Type: types.OperationTypeContext, Action: types.OperationActionStart, ContextOptions: &types.ContextOptions{ReplayChildren: true}},
		},
	})
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	updates := fake.lastInput.Updates
	if updates[0].WaitOptions == nil || aws.ToInt32(updates[0].WaitOptions.WaitSeconds) != 30 {
		t.Errorf("unexpected WaitOptions: %+v", updates[0].WaitOptions)
	}
	if updates[1].StepOptions == nil || aws.ToInt32(updates[1].StepOptions.NextAttemptDelaySeconds) != 5 {
		t.Errorf("unexpected StepOptions: %+v", updates[1].StepOptions)
	}
	if updates[2].CallbackOptions == nil || updates[2].CallbackOptions.TimeoutSeconds != 60 || updates[2].CallbackOptions.HeartbeatTimeoutSeconds != 10 {
		t.Errorf("unexpected CallbackOptions: %+v", updates[2].CallbackOptions)
	}
	if updates[3].ChainedInvokeOptions == nil || aws.ToString(updates[3].ChainedInvokeOptions.FunctionName) != "target-fn" || aws.ToString(updates[3].ChainedInvokeOptions.TenantId) != "tenant-1" {
		t.Errorf("unexpected ChainedInvokeOptions: %+v", updates[3].ChainedInvokeOptions)
	}
	if updates[4].ContextOptions == nil || !aws.ToBool(updates[4].ContextOptions.ReplayChildren) {
		t.Errorf("unexpected ContextOptions: %+v", updates[4].ContextOptions)
	}
}

func TestClient_Checkpoint_APIError(t *testing.T) {
	fake := &fakeLambdaAPI{err: context.DeadlineExceeded}
	c := &Client{API: fake}

	_, err := c.Checkpoint(context.Background(), types.CheckpointDurableExecutionRequest{
		DurableExecutionArn: "arn:test",
		CheckpointToken:     "token-0",
	})
	if err == nil {
		t.Fatal("expected an error when the underlying API call fails")
	}
}

func TestClient_Checkpoint_NilAPI(t *testing.T) {
	c := &Client{}
	_, err := c.Checkpoint(context.Background(), types.CheckpointDurableExecutionRequest{
		DurableExecutionArn: "arn:test",
		CheckpointToken:     "token-0",
	})
	if err == nil {
		t.Fatal("expected an error when API is nil")
	}
}
