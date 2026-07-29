package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/smithy-go"
)

// resultOfSerializedSize returns a string whose JSON serialization is
// exactly n bytes (the string plus two quote characters).
func resultOfSerializedSize(n int) string {
	return strings.Repeat("a", n-2)
}

func TestRootResultAtLimitReturnsInline(t *testing.T) {
	// A result whose serialized size equals the limit exactly is still
	// returned inline: only sizes strictly over the limit are checkpointed.
	fake := &fakeLambda{}
	large := resultOfSerializedSize(lambdaResponseSizeLimit)

	resp := invokeStep(t, fake, stepPayload(`""`), func(_ Context, _ string) (string, error) {
		return large, nil
	})

	var parsed invocationResponse
	if err := json.Unmarshal([]byte(resp), &parsed); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if parsed.Status != invocationSucceeded {
		t.Fatalf("response status = %q, want SUCCEEDED", parsed.Status)
	}
	if parsed.Result == nil || len(*parsed.Result) != lambdaResponseSizeLimit {
		t.Fatalf("inline Result missing or wrong size")
	}
	for _, u := range updateBatch(t, fake) {
		if u.Type == types.OperationTypeExecution {
			t.Errorf("unexpected EXECUTION checkpoint for an at-limit result")
		}
	}
}

func TestRootResultOverLimitIsCheckpointed(t *testing.T) {
	// One byte over the limit: the result is persisted via an
	// EXECUTION/SUCCEED checkpoint and the response Result is empty.
	fake := &fakeLambda{}
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)

	resp := invokeStep(t, fake, stepPayload(`""`), func(_ Context, _ string) (string, error) {
		return large, nil
	})

	var parsed invocationResponse
	if err := json.Unmarshal([]byte(resp), &parsed); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if parsed.Status != invocationSucceeded {
		t.Fatalf("response status = %q, want SUCCEEDED", parsed.Status)
	}
	if parsed.Result == nil || *parsed.Result != "" {
		t.Fatalf("Result = %v, want empty string", parsed.Result)
	}

	found := false
	for _, u := range updateBatch(t, fake) {
		if u.Type != types.OperationTypeExecution {
			continue
		}
		found = true
		if u.Action != types.OperationActionSucceed {
			t.Errorf("EXECUTION update Action = %q, want SUCCEED", u.Action)
		}
		if got := aws.ToString(u.Id); got != "exec-op" {
			t.Errorf("EXECUTION update Id = %q, want %q", got, "exec-op")
		}
		wantPayload, err := json.Marshal(large)
		if err != nil {
			t.Fatal(err)
		}
		if got := aws.ToString(u.Payload); got != string(wantPayload) {
			t.Errorf("EXECUTION update Payload size = %d, want %d", len(got), len(wantPayload))
		}
	}
	if !found {
		t.Fatal("expected an EXECUTION/SUCCEED checkpoint carrying the oversized result")
	}
}

func TestRootResultCheckpointCompletesBeforeResponse(t *testing.T) {
	// The invocation must not respond until the oversized-result
	// checkpoint has completed: the checkpoint is the only durable copy.
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)

	checkpointDone := false
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(_ context.Context, in *lambda.CheckpointDurableExecutionInput, _ ...func(*lambda.Options)) (*lambda.CheckpointDurableExecutionOutput, error) {
			for _, u := range in.Updates {
				if u.Type == types.OperationTypeExecution {
					checkpointDone = true
				}
			}
			tok := "token-1"
			return &lambda.CheckpointDurableExecutionOutput{CheckpointToken: &tok}, nil
		},
	}

	h := Wrap(func(_ Context, _ string) (string, error) {
		return large, nil
	}, withLambdaAPI(fake))
	got, err := h.Invoke(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatalf("Invoke error: %v", err)
	}
	if !checkpointDone {
		t.Fatal("Invoke returned before the oversized-result checkpoint completed")
	}
	var parsed invocationResponse
	if err := json.Unmarshal(got, &parsed); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if parsed.Status != invocationSucceeded || parsed.Result == nil || *parsed.Result != "" {
		t.Fatalf("response = %s, want SUCCEEDED with empty Result", got)
	}
}

func TestRootResultCheckpointFailureFailsInvocation(t *testing.T) {
	// If the oversized-result checkpoint fails, the invocation must not
	// return a success envelope: the result was never durably recorded.
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)

	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(_ context.Context, _ *lambda.CheckpointDurableExecutionInput, _ ...func(*lambda.Options)) (*lambda.CheckpointDurableExecutionOutput, error) {
			return nil, &smithy.GenericAPIError{
				Code:    "ValidationException",
				Message: "rejected",
				Fault:   smithy.FaultClient,
			}
		},
	}

	h := Wrap(func(_ Context, _ string) (string, error) {
		return large, nil
	}, withLambdaAPI(fake))
	_, err := h.Invoke(context.Background(), stepPayload(`""`))
	if err == nil {
		t.Fatal("Invoke succeeded despite the oversized-result checkpoint failing")
	}
	var cpErr *CheckpointError
	if !errors.As(err, &cpErr) {
		t.Errorf("error = %v (%T), want wrapped *CheckpointError", err, err)
	}
}

func TestRootResultOversizedCheckpointBeforePlugin(t *testing.T) {
	// Verify that the OnInvocationEnd(SUCCEEDED) hook fires only AFTER
	// the oversized-result checkpoint completes. This prevents false
	// success telemetry when the checkpoint is the only durable copy.
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)

	var events []string
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(_ context.Context, in *lambda.CheckpointDurableExecutionInput, _ ...func(*lambda.Options)) (*lambda.CheckpointDurableExecutionOutput, error) {
			for _, u := range in.Updates {
				if u.Type == types.OperationTypeExecution {
					events = append(events, "checkpoint")
				}
			}
			tok := "token-1"
			return &lambda.CheckpointDurableExecutionOutput{CheckpointToken: &tok}, nil
		},
	}

	plugin := Plugin{
		OnInvocationEnd: func(_ context.Context, info InvocationEndHookInfo) {
			events = append(events, "plugin:"+string(info.Status))
		},
	}

	h := Wrap(func(_ Context, _ string) (string, error) {
		return large, nil
	}, withLambdaAPI(fake), WithPlugins(plugin))

	_, err := h.Invoke(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatalf("Invoke error: %v", err)
	}

	// Events must be: checkpoint first, then plugin success.
	if len(events) != 2 {
		t.Fatalf("events = %v, want [checkpoint, plugin:SUCCEEDED]", events)
	}
	if events[0] != "checkpoint" {
		t.Errorf("events[0] = %q, want %q", events[0], "checkpoint")
	}
	if events[1] != "plugin:SUCCEEDED" {
		t.Errorf("events[1] = %q, want %q", events[1], "plugin:SUCCEEDED")
	}
}

func TestRootResultCheckpointFailureNoSuccessPlugin(t *testing.T) {
	// When the oversized-result checkpoint fails, the success plugin hook
	// must NOT fire — the result was never durably recorded.
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)

	var pluginFired bool
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(_ context.Context, _ *lambda.CheckpointDurableExecutionInput, _ ...func(*lambda.Options)) (*lambda.CheckpointDurableExecutionOutput, error) {
			return nil, &smithy.GenericAPIError{
				Code:    "ValidationException",
				Message: "rejected",
				Fault:   smithy.FaultClient,
			}
		},
	}

	plugin := Plugin{
		OnInvocationEnd: func(_ context.Context, info InvocationEndHookInfo) {
			pluginFired = true
		},
	}

	h := Wrap(func(_ Context, _ string) (string, error) {
		return large, nil
	}, withLambdaAPI(fake), WithPlugins(plugin))

	_, err := h.Invoke(context.Background(), stepPayload(`""`))
	if err == nil {
		t.Fatal("Invoke succeeded despite checkpoint failure")
	}
	if pluginFired {
		t.Error("OnInvocationEnd fired despite checkpoint failure — success telemetry emitted for non-durable result")
	}
}
