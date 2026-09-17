// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/internal/wire"
)

// TestWirePayloadReadByDurable proves that the wire types this package
// encodes are the ones the durable package decodes. It marshals an
// InvocationInput whose operation log records a step, a callback, a
// chained invoke, and a child context as SUCCEEDED with results, then
// hands the bytes to a wrapped handler. If durable read the payload, each
// operation replays its checkpointed result instead of running.
func TestWirePayloadReadByDurable(t *testing.T) {
	handler := func(ctx durable.Context, _ string) (string, error) {
		step, err := durable.Step(ctx, "step", func(durable.StepContext) (string, error) {
			return "fresh-step", nil
		})
		if err != nil {
			return "", err
		}
		cb, err := durable.CreateCallback[string](ctx, "callback")
		if err != nil {
			return "", err
		}
		cbResult, err := cb.Result()
		if err != nil {
			return "", err
		}
		inv, err := durable.Invoke[string](ctx, "invoke", "downstream", "input")
		if err != nil {
			return "", err
		}
		child, err := durable.RunInChildContext(ctx, "child", func(durable.Context) (string, error) {
			return "fresh-child", nil
		})
		if err != nil {
			return "", err
		}
		return strings.Join([]string{step, cbResult, inv, child}, ","), nil
	}

	// A first pass through the LocalRunner checkpoints the operations and
	// yields their wire identities. It suspends on the callback and then on
	// the invoke; resolving the invoke lets it record every operation the
	// payload below must name.
	discovery := NewLocalRunner(handler)
	first := discovery.Run(t, "event")
	if first.Status != Pending {
		t.Fatalf("discovery status = %s, want PENDING", first.Status)
	}
	cbs := discovery.OpenCallbacks()
	if len(cbs) != 1 {
		t.Fatalf("open callbacks = %d, want 1", len(cbs))
	}
	if err := discovery.SendCallbackSuccess(cbs[0].CallbackID, "discovery-callback"); err != nil {
		t.Fatal(err)
	}
	first = discovery.Run(t, "event")
	if first.Status != Pending {
		t.Fatalf("discovery status after callback = %s, want PENDING", first.Status)
	}
	if err := discovery.CompleteChainedInvoke("invoke", "discovery-invoke"); err != nil {
		t.Fatal(err)
	}
	first = discovery.RunUntilComplete(t, "event")
	if first.Status != Succeeded {
		t.Fatalf("discovery status after invoke = %s, want SUCCEEDED", first.Status)
	}
	// identity copies the fields that name an operation on the wire, so
	// the payload below exercises the same identity checks a service
	// delivered payload would.
	identity := func(name string) wire.Operation {
		op := first.Operation(name)
		if op == nil {
			t.Fatalf("discovery run recorded no operation named %q", name)
		}
		return wire.Operation{Id: op.ID, Name: op.Name, Type: op.Type, SubType: op.SubType}
	}
	withStep := func(op wire.Operation, d *wire.StepDetails) wire.Operation {
		op.Status = "SUCCEEDED"
		op.StepDetails = d
		return op
	}
	withCallback := func(op wire.Operation, d *wire.CallbackDetails) wire.Operation {
		op.Status = "SUCCEEDED"
		op.CallbackDetails = d
		return op
	}
	withInvoke := func(op wire.Operation, d *wire.ChainedInvokeDetails) wire.Operation {
		op.Status = "SUCCEEDED"
		op.ChainedInvokeDetails = d
		return op
	}
	withContext := func(op wire.Operation, d *wire.ContextDetails) wire.Operation {
		op.Status = "SUCCEEDED"
		op.ContextDetails = d
		return op
	}

	input := wire.InvocationInput{
		DurableExecutionArn: "arn:aws:lambda:us-east-1:000:function:fn/durable-execution/test",
		CheckpointToken:     "token",
		InitialExecutionState: wire.InitialExecutionState{
			Operations: []wire.Operation{
				{
					Id:               "exec",
					Status:           "STARTED",
					Type:             "EXECUTION",
					ExecutionDetails: &wire.ExecutionDetails{InputPayload: `"event"`},
				},
				withStep(identity("step"), &wire.StepDetails{Attempt: 1, Result: `"replayed-step"`}),
				withCallback(identity("callback"), &wire.CallbackDetails{CallbackId: "cb-1", Result: `"replayed-callback"`}),
				withInvoke(identity("invoke"), &wire.ChainedInvokeDetails{Result: `"replayed-invoke"`}),
				withContext(identity("child"), &wire.ContextDetails{Result: `"replayed-child"`}),
			},
		},
	}
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	// A fresh handler with an empty client sees only what the payload
	// carries.
	raw := durable.Wrap(handler, durable.WithExecutionClient(newMemoryClient()))
	responseBytes, err := raw(context.Background(), payload)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	var response wire.InvocationResponse
	if err := json.Unmarshal(responseBytes, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != wire.StatusSucceeded {
		t.Fatalf("response = %s, want SUCCEEDED", responseBytes)
	}
	if response.Result == nil {
		t.Fatalf("response has no Result: %s", responseBytes)
	}
	var got string
	if err := json.Unmarshal([]byte(*response.Result), &got); err != nil {
		t.Fatalf("decode result %q: %v", *response.Result, err)
	}
	want := "replayed-step,replayed-callback,replayed-invoke,replayed-child"
	if got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
}

// TestWireErrorReadByDurable proves the failure branch of the shared
// types: a step recorded as FAILED with an ErrorObject surfaces through
// durable as a FAILED invocation response carrying that error.
func TestWireErrorReadByDurable(t *testing.T) {
	handler := func(ctx durable.Context, _ string) (string, error) {
		return durable.Step(ctx, "step", func(durable.StepContext) (string, error) {
			return "fresh", nil
		}, durable.WithRetry(durable.NoRetry()))
	}

	discovery := NewLocalRunner(handler)
	first := discovery.Run(t, "event")
	stepOp := first.Operation("step")
	if stepOp == nil {
		t.Fatal("discovery run recorded no step operation")
	}

	input := wire.InvocationInput{
		DurableExecutionArn: "arn:aws:lambda:us-east-1:000:function:fn/durable-execution/test",
		CheckpointToken:     "token",
		InitialExecutionState: wire.InitialExecutionState{
			Operations: []wire.Operation{
				{
					Id:               "exec",
					Status:           "STARTED",
					Type:             "EXECUTION",
					ExecutionDetails: &wire.ExecutionDetails{InputPayload: `"event"`},
				},
				{
					Id:      stepOp.ID,
					Name:    stepOp.Name,
					Type:    stepOp.Type,
					SubType: stepOp.SubType,
					Status:  "FAILED",
					StepDetails: &wire.StepDetails{
						Attempt: 1,
						Error:   &wire.ErrorObject{ErrorType: "Boom", ErrorMessage: "recorded failure"},
					},
				},
			},
		},
	}
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	raw := durable.Wrap(handler, durable.WithExecutionClient(newMemoryClient()))
	responseBytes, err := raw(context.Background(), payload)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	var response wire.InvocationResponse
	if err := json.Unmarshal(responseBytes, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != wire.StatusFailed || response.Error == nil {
		t.Fatalf("response = %s, want FAILED with Error", responseBytes)
	}
	if response.Error.ErrorType != "StepError" {
		t.Errorf("ErrorType = %q, want StepError", response.Error.ErrorType)
	}
	if !strings.Contains(response.Error.ErrorMessage, "recorded failure") {
		t.Errorf("ErrorMessage = %q, want it to carry the recorded message", response.Error.ErrorMessage)
	}
}

// TestBuildPayloadDecodesAsWireInput checks the runner's own payload
// against the shared type: what buildPayload produces must decode as an
// InvocationInput with the execution operation first.
func TestBuildPayloadDecodesAsWireInput(t *testing.T) {
	runner := NewLocalRunner(func(ctx durable.Context, n int) (int, error) {
		return durable.Step(ctx, "double", func(durable.StepContext) (int, error) {
			return n * 2, nil
		})
	})
	if got := runner.RunUntilComplete(t, 21); got.Status != Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", got.Status)
	}

	payload, err := runner.buildPayload(21)
	if err != nil {
		t.Fatalf("buildPayload: %v", err)
	}
	var in wire.InvocationInput
	if err := json.Unmarshal(payload, &in); err != nil {
		t.Fatalf("decode buildPayload output as wire.InvocationInput: %v", err)
	}
	ops := in.InitialExecutionState.Operations
	if len(ops) != 2 {
		t.Fatalf("operations = %d, want 2 (execution + step): %s", len(ops), payload)
	}
	if ops[0].Type != "EXECUTION" || ops[0].ExecutionDetails == nil || ops[0].ExecutionDetails.InputPayload != "21" {
		t.Errorf("ops[0] = %+v, want EXECUTION carrying input 21", ops[0])
	}
	if ops[1].Name != "double" || ops[1].StepDetails == nil || ops[1].StepDetails.Result != "42" {
		t.Errorf("ops[1] = %s, want step \"double\" with result 42", fmt.Sprintf("%+v", ops[1]))
	}
}
