// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// The local runner settles an invoke as SUCCEEDED, FAILED, or TIMED_OUT.
// It cannot settle one as STOPPED or CANCELLED, so the ErrExecutionStopped
// and ErrExecutionCancelled branches are not driven here: they run only
// when the service stops or cancels the invoked execution.

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
