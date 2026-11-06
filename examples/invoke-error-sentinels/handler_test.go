// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// The local runner settles an invoke as SUCCEEDED, FAILED, or TIMED_OUT.
// It cannot settle one as STOPPED or CANCELLED, so the handler tests drive
// the first three, and TestOutcomeOf checks the classification of the
// stopped and cancelled errors directly.

// runToInvoke starts the execution and returns once it awaits the invoke.
func runToInvoke(t *testing.T, runner *durabletest.LocalRunner[Input, Output], input Input) {
	t.Helper()
	result, err := runner.RunUntilComplete(input)
	if err != nil {
		t.Fatal(err)
	}
	// Execution suspends because the invoke needs external resolution.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invoke), got %s", result.Status)
	}
}

var input = Input{
	TargetFunction: "arn:aws:lambda:us-east-1:123456789012:function:target",
	OrderID:        "order-123",
}

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	runToInvoke(t, runner, input)

	reply := StageResult{OrderID: "order-123", Stage: "quote", Status: "completed"}
	if err := runner.CompleteChainedInvoke("quote", reply); err != nil {
		t.Fatalf("complete quote: %v", err)
	}

	result, err := runner.RunUntilComplete(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out.Outcome != "completed" || out.Result == nil || *out.Result != reply {
		t.Errorf("expected completed outcome with %+v, got %+v", reply, out)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// TestHandlerTimedOut times the invoke out. The InvokeError unwraps to
// ErrInvokeTimedOut, so the handler reports the timeout and succeeds.
func TestHandlerTimedOut(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	runToInvoke(t, runner, input)

	if err := runner.TimeoutChainedInvoke("quote"); err != nil {
		t.Fatalf("time out quote: %v", err)
	}

	result, err := runner.RunUntilComplete(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out != (Output{Outcome: "timed-out"}) {
		t.Errorf("expected timed-out outcome, got %+v", out)
	}

	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.timed-out.golden")
}

// TestHandlerFailed fails the invoke with the target's own error. It
// matches no sentinel, so the handler returns it and the execution fails.
func TestHandlerFailed(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	runToInvoke(t, runner, input)

	if err := runner.FailChainedInvoke("quote", "QuoteError", "no price for order-123"); err != nil {
		t.Fatalf("fail quote: %v", err)
	}

	result, err := runner.RunUntilComplete(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error details")
	}
	if !strings.Contains(result.Error.Message, "no price for order-123") {
		t.Errorf("expected the target's error message, got %q", result.Error.Message)
	}

	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.failed.golden")
}

// invokeError builds the error Invoke returns for a settled invoke: an
// *InvokeError whose Err is a stand-in for the recorded failure, wrapping
// the sentinel for the status when there is one.
func invokeError(status durable.OperationStatus, errorType, message string, sentinel error) error {
	cause := fmt.Errorf("%s: %s", errorType, message)
	if sentinel != nil {
		cause = fmt.Errorf("%s: %s: %w", errorType, message, sentinel)
	}
	return &durable.InvokeError{
		Name:       "quote",
		FunctionID: input.TargetFunction,
		Status:     status,
		ErrorType:  errorType,
		Message:    message,
		Err:        cause,
	}
}

func TestOutcomeOf(t *testing.T) {
	stopped := invokeError(durable.OperationStatusStopped, "Error", "execution stopped", durable.ErrExecutionStopped)
	cancelled := invokeError(durable.OperationStatusCancelled, "Error", "execution cancelled", durable.ErrExecutionCancelled)
	timedOut := invokeError(durable.OperationStatusTimedOut, "Error", "execution timed out", durable.ErrInvokeTimedOut)
	failed := invokeError(durable.OperationStatusFailed, "QuoteError", "no price for order-123", nil)

	tests := []struct {
		name    string
		err     error
		outcome string
		ok      bool
	}{
		{"stopped", stopped, "stopped", true},
		{"cancelled", cancelled, "cancelled", true},
		{"timed out", timedOut, "timed-out", true},
		{"stopped wrapped again", fmt.Errorf("order order-123: %w", stopped), "stopped", true},
		{"cancelled wrapped again", fmt.Errorf("order order-123: %w", cancelled), "cancelled", true},
		{"target failure", failed, "", false},
		{"other error", errors.New("connection reset"), "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outcome, ok := outcomeOf(tt.err)
			if outcome != tt.outcome || ok != tt.ok {
				t.Errorf("outcomeOf(%v) = (%q, %v), want (%q, %v)", tt.err, outcome, ok, tt.outcome, tt.ok)
			}
		})
	}
}
