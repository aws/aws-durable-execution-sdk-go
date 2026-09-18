package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
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
		if u.Type == OperationTypeExecution {
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
		if u.Type != OperationTypeExecution {
			continue
		}
		found = true
		if u.Action != OperationActionSucceed {
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

func TestRootResultOverLimitTargetsExecutionOpByType(t *testing.T) {
	// The execution operation is located by type, not by position. With
	// a STEP operation at index 0 and the EXECUTION operation after it,
	// the oversized-result checkpoint must still mark the EXECUTION
	// operation SUCCEEDED.
	fake := &fakeLambda{}
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)

	ops := []wireOperation{
		checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"done"`}),
		{Id: "exec-op", Status: "STARTED", Type: "EXECUTION", ExecutionDetails: &wireExecutionDetails{InputPayload: `""`}},
	}
	in := invocationInput{
		DurableExecutionArn:   "arn:test",
		CheckpointToken:       "token-0",
		InitialExecutionState: initialExecutionState{Operations: ops},
	}
	payload, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}

	resp := invokeStep(t, fake, payload, func(_ Context, _ string) (string, error) {
		return large, nil
	})

	var parsed invocationResponse
	if err := json.Unmarshal([]byte(resp), &parsed); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if parsed.Status != invocationSucceeded || parsed.Result == nil || *parsed.Result != "" {
		t.Fatalf("response = %s, want SUCCEEDED with empty Result", resp)
	}

	var execUpdates []OperationUpdate
	for _, u := range updateBatch(t, fake) {
		if aws.ToString(u.Id) == hashID("1") {
			t.Errorf("STEP operation at index 0 received an update: %+v", u)
		}
		if u.Type == OperationTypeExecution {
			execUpdates = append(execUpdates, u)
		}
	}
	if len(execUpdates) != 1 {
		t.Fatalf("EXECUTION updates = %d, want exactly 1", len(execUpdates))
	}
	u := execUpdates[0]
	if got := aws.ToString(u.Id); got != "exec-op" {
		t.Errorf("EXECUTION update Id = %q, want %q", got, "exec-op")
	}
	if u.Action != OperationActionSucceed {
		t.Errorf("EXECUTION update Action = %q, want SUCCEED", u.Action)
	}
}

func TestRootResultOverLimitWithoutExecutionOpFails(t *testing.T) {
	// The initial state holds operations, but none of type EXECUTION.
	// The oversized result cannot be checkpointed, so the invocation
	// fails with a clear error instead of marking another operation
	// SUCCEEDED.
	fake := &fakeLambda{}
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)

	ops := []wireOperation{
		checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"done"`}),
	}
	in := invocationInput{
		DurableExecutionArn:   "arn:test",
		CheckpointToken:       "token-0",
		InitialExecutionState: initialExecutionState{Operations: ops},
	}
	payload, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}

	h := Wrap(func(_ Context, _ string) (string, error) {
		return large, nil
	}, withLambdaAPI(fake))
	_, err = h(context.Background(), payload)
	if err == nil {
		t.Fatal("Invoke succeeded despite missing execution operation")
	}
	if !strings.Contains(err.Error(), "no execution operation") {
		t.Fatalf("error = %v, want the no-execution-operation guard", err)
	}
	if updates := updateBatch(t, fake); len(updates) != 0 {
		t.Fatalf("unexpected checkpoint updates: %+v", updates)
	}
}

func TestRootResultCheckpointCompletesBeforeResponse(t *testing.T) {
	// The invocation must not respond until the oversized-result
	// checkpoint has completed: the checkpoint is the only durable copy.
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)

	checkpointDone := false
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
			for _, u := range in.Updates {
				if u.Type == OperationTypeExecution {
					checkpointDone = true
				}
			}
			tok := "token-1"
			return CheckpointOutput{CheckpointToken: tok}, nil
		},
	}

	h := Wrap(func(_ Context, _ string) (string, error) {
		return large, nil
	}, withLambdaAPI(fake))
	got, err := h(context.Background(), stepPayload(`""`))
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

func TestRootResultCheckpointInvocationScopeFailsInvocation(t *testing.T) {
	// An invocation-scoped failure of the oversized-result checkpoint ends
	// the invocation with an error, never a success envelope: the result
	// was not durably recorded, and the execution resumes later.
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)
	cause := errors.New("timeout")

	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(_ context.Context, _ CheckpointInput) (CheckpointOutput, error) {
			return CheckpointOutput{}, &ClientError{Scope: ErrorScopeInvocation, Err: cause}
		},
	}

	h := Wrap(func(_ Context, _ string) (string, error) {
		return large, nil
	}, withLambdaAPI(fake))
	raw, err := h(context.Background(), stepPayload(`""`))
	if err == nil {
		t.Fatalf("Invoke returned response %s, want an error", raw)
	}
	var cpErr *CheckpointError
	if !errors.As(err, &cpErr) {
		t.Fatalf("error = %v (%T), want wrapped *CheckpointError", err, err)
	}
	if cpErr.Scope() != ErrorScopeInvocation {
		t.Errorf("Scope() = %q, want %q", cpErr.Scope(), ErrorScopeInvocation)
	}
	if !errors.Is(err, cause) {
		t.Error("error chain must reach the client's cause")
	}
}

func TestRootResultCheckpointExecutionScopeFailsExecution(t *testing.T) {
	// An execution-scoped failure of the oversized-result checkpoint fails
	// the execution: the invocation responds FAILED instead of ending with
	// an error, because a later invocation would fail the same way.
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)

	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(_ context.Context, _ CheckpointInput) (CheckpointOutput, error) {
			return CheckpointOutput{}, &smithy.GenericAPIError{
				Code:    "ValidationException",
				Message: "rejected",
				Fault:   smithy.FaultClient,
			}
		},
	}

	h := Wrap(func(_ Context, _ string) (string, error) {
		return large, nil
	}, withLambdaAPI(fake))
	raw, err := h(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatalf("Invoke error = %v, want FAILED response", err)
	}
	resp := parseResponse(t, raw)
	if resp.Status != invocationFailed {
		t.Fatalf("status = %q, want %q", resp.Status, invocationFailed)
	}
	if resp.Error == nil || resp.Error.ErrorType != "CheckpointError" {
		t.Errorf("error = %+v, want ErrorType CheckpointError", resp.Error)
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
		checkpoint: func(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
			for _, u := range in.Updates {
				if u.Type == OperationTypeExecution {
					events = append(events, "checkpoint")
				}
			}
			tok := "token-1"
			return CheckpointOutput{CheckpointToken: tok}, nil
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

	_, err := h(context.Background(), stepPayload(`""`))
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
	// When the oversized-result checkpoint fails, exactly one FAILED
	// OnInvocationEnd hook must fire carrying the returned error, and the
	// success hook must NOT fire — the result was never durably recorded.
	// The failure here is invocation-scoped, so the invocation ends with
	// an error and the hook carries that exact error.
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)

	var hooks []InvocationEndHookInfo
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(_ context.Context, _ CheckpointInput) (CheckpointOutput, error) {
			return CheckpointOutput{}, &ClientError{Scope: ErrorScopeInvocation, Err: errors.New("timeout")}
		},
	}

	plugin := Plugin{
		OnInvocationEnd: func(_ context.Context, info InvocationEndHookInfo) {
			hooks = append(hooks, info)
		},
	}

	h := Wrap(func(_ Context, _ string) (string, error) {
		return large, nil
	}, withLambdaAPI(fake), WithPlugins(plugin))

	_, err := h(context.Background(), stepPayload(`""`))
	if err == nil {
		t.Fatal("Invoke succeeded despite checkpoint failure")
	}
	if len(hooks) != 1 {
		t.Fatalf("OnInvocationEnd fired %d times, want exactly 1", len(hooks))
	}
	if hooks[0].Status != PluginInvocationFailed {
		t.Errorf("hook Status = %q, want %q", hooks[0].Status, PluginInvocationFailed)
	}
	if hooks[0].ExecutionError != err { //nolint:errorlint // identity check is intentional
		t.Errorf("hook ExecutionError = %v, want the exact returned error %v", hooks[0].ExecutionError, err)
	}
}

func TestRootResultCheckpointExecutionScopeFiresFailedHookOnce(t *testing.T) {
	// An execution-scoped oversized-result checkpoint failure responds
	// FAILED. Exactly one FAILED OnInvocationEnd hook fires, carrying the
	// same checkpoint error the response reports; the success hook must
	// NOT fire.
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)

	var hooks []InvocationEndHookInfo
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(_ context.Context, _ CheckpointInput) (CheckpointOutput, error) {
			return CheckpointOutput{}, &ClientError{Scope: ErrorScopeExecution, Err: errors.New("rejected")}
		},
	}

	plugin := Plugin{
		OnInvocationEnd: func(_ context.Context, info InvocationEndHookInfo) {
			hooks = append(hooks, info)
		},
	}

	h := Wrap(func(_ Context, _ string) (string, error) {
		return large, nil
	}, withLambdaAPI(fake), WithPlugins(plugin))

	raw, err := h(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatalf("Invoke error = %v, want FAILED response", err)
	}
	if resp := parseResponse(t, raw); resp.Status != invocationFailed {
		t.Fatalf("status = %q, want %q", resp.Status, invocationFailed)
	}
	if len(hooks) != 1 {
		t.Fatalf("OnInvocationEnd fired %d times, want exactly 1", len(hooks))
	}
	if hooks[0].Status != PluginInvocationFailed {
		t.Errorf("hook Status = %q, want %q", hooks[0].Status, PluginInvocationFailed)
	}
	var cpErr *CheckpointError
	if !errors.As(hooks[0].ExecutionError, &cpErr) || cpErr.Scope() != ErrorScopeExecution {
		t.Errorf("hook ExecutionError = %v, want an execution-scoped *CheckpointError", hooks[0].ExecutionError)
	}
}

func TestRootResultSerializationFailureFiresFailedHook(t *testing.T) {
	// When the handler result cannot be serialized, exactly one FAILED
	// OnInvocationEnd hook must fire carrying the returned error, and the
	// success hook must NOT fire.
	var hooks []InvocationEndHookInfo
	fake := &fakeLambdaFunc{getState: emptyGetState}

	plugin := Plugin{
		OnInvocationEnd: func(_ context.Context, info InvocationEndHookInfo) {
			hooks = append(hooks, info)
		},
	}

	h := Wrap(func(_ Context, _ string) (chan int, error) {
		return make(chan int), nil // json.Marshal fails on channels
	}, withLambdaAPI(fake), WithPlugins(plugin))

	_, err := h(context.Background(), stepPayload(`""`))
	if err == nil {
		t.Fatal("Invoke succeeded despite unserializable result")
	}
	if !strings.Contains(err.Error(), "serialize handler result") {
		t.Fatalf("error = %v, want result serialization failure", err)
	}
	if len(hooks) != 1 {
		t.Fatalf("OnInvocationEnd fired %d times, want exactly 1", len(hooks))
	}
	if hooks[0].Status != PluginInvocationFailed {
		t.Errorf("hook Status = %q, want %q", hooks[0].Status, PluginInvocationFailed)
	}
	if hooks[0].ExecutionError != err { //nolint:errorlint // identity check is intentional
		t.Errorf("hook ExecutionError = %v, want the exact returned error %v", hooks[0].ExecutionError, err)
	}
}

func TestRootResultOversizedNoExecutionOpFiresFailedHook(t *testing.T) {
	// An oversized result with no execution operation in the initial
	// state cannot be checkpointed. Exactly one FAILED OnInvocationEnd
	// hook must fire carrying the returned error.
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)

	var hooks []InvocationEndHookInfo
	fake := &fakeLambdaFunc{getState: emptyGetState}

	plugin := Plugin{
		OnInvocationEnd: func(_ context.Context, info InvocationEndHookInfo) {
			hooks = append(hooks, info)
		},
	}

	// Payload with an empty initial state: no execution operation exists
	// to carry the oversized-result checkpoint.
	in := invocationInput{
		DurableExecutionArn: "arn:test",
		CheckpointToken:     "token-0",
	}
	payload, merr := json.Marshal(in)
	if merr != nil {
		t.Fatal(merr)
	}

	h := Wrap(func(_ Context, _ string) (string, error) {
		return large, nil
	}, withLambdaAPI(fake), WithPlugins(plugin))

	_, err := h(context.Background(), payload)
	if err == nil {
		t.Fatal("Invoke succeeded despite missing execution operation")
	}
	if !strings.Contains(err.Error(), "no execution operation") {
		t.Fatalf("error = %v, want the no-execution-operation guard", err)
	}
	if len(hooks) != 1 {
		t.Fatalf("OnInvocationEnd fired %d times, want exactly 1", len(hooks))
	}
	if hooks[0].Status != PluginInvocationFailed {
		t.Errorf("hook Status = %q, want %q", hooks[0].Status, PluginInvocationFailed)
	}
	if hooks[0].ExecutionError != err { //nolint:errorlint // identity check is intentional
		t.Errorf("hook ExecutionError = %v, want the exact returned error %v", hooks[0].ExecutionError, err)
	}
}
