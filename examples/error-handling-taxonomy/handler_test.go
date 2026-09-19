// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	// Set dummy credentials so config.LoadDefaultConfig resolves quickly
	// and the Lambda API call fails fast with an auth error rather than
	// timing out searching for real credentials.
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")

	runner := durabletest.NewLocalRunner(handler)

	// First run: Step fails synchronously (caught), Invoke suspends.
	result := runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invoke), got %s", result.Status)
	}

	// Fail the invoke externally.
	if err := runner.FailChainedInvoke("failing-invoke", "ServiceError", "target function failed"); err != nil {
		t.Fatalf("FailChainedInvoke: %v", err)
	}

	// Second run: replays past Step and Invoke failures (caught),
	// CreateCallback creates callback, send-callback-failure Step fails
	// (no real endpoint, error discarded), cb.Result() suspends.
	result = runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting callback), got %s", result.Status)
	}

	// Send a callback failure externally to produce a genuine CallbackError.
	callbacks := runner.OpenCallbacks()
	if len(callbacks) != 1 {
		t.Fatalf("expected 1 open callback, got %d", len(callbacks))
	}
	if err := runner.SendCallbackFailure(callbacks[0].CallbackID, "CallbackError", "deliberate callback failure"); err != nil {
		t.Fatalf("SendCallbackFailure: %v", err)
	}

	// Third run: callback resolves with CallbackError (caught), handler
	// returns Output with all three error types matched.
	result = runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	// Assert StepError taxonomy.
	if !out.StepErrorInfo.Matched {
		t.Error("StepErrorInfo: expected Matched=true")
	}
	if out.StepErrorInfo.TypeName != "StepError" {
		t.Errorf("StepErrorInfo.TypeName = %q, want %q", out.StepErrorInfo.TypeName, "StepError")
	}
	if out.StepErrorInfo.OperationName != "failing-step" {
		t.Errorf("StepErrorInfo.OperationName = %q, want %q", out.StepErrorInfo.OperationName, "failing-step")
	}
	if out.StepErrorInfo.Attempts != 1 {
		t.Errorf("StepErrorInfo.Attempts = %d, want %d", out.StepErrorInfo.Attempts, 1)
	}
	if !out.StepErrorInfo.IsOpError {
		t.Error("StepErrorInfo: expected IsOpError=true (OperationError super-type)")
	}
	if out.StepErrorInfo.OpErrorName != "failing-step" {
		t.Errorf("StepErrorInfo.OpErrorName = %q, want %q", out.StepErrorInfo.OpErrorName, "failing-step")
	}

	// Assert InvokeError taxonomy.
	if !out.InvokeErrorInfo.Matched {
		t.Error("InvokeErrorInfo: expected Matched=true")
	}
	if out.InvokeErrorInfo.TypeName != "InvokeError" {
		t.Errorf("InvokeErrorInfo.TypeName = %q, want %q", out.InvokeErrorInfo.TypeName, "InvokeError")
	}
	if out.InvokeErrorInfo.OperationName != "failing-invoke" {
		t.Errorf("InvokeErrorInfo.OperationName = %q, want %q", out.InvokeErrorInfo.OperationName, "failing-invoke")
	}
	if !out.InvokeErrorInfo.IsOpError {
		t.Error("InvokeErrorInfo: expected IsOpError=true (OperationError super-type)")
	}
	if out.InvokeErrorInfo.OpErrorName != "failing-invoke" {
		t.Errorf("InvokeErrorInfo.OpErrorName = %q, want %q", out.InvokeErrorInfo.OpErrorName, "failing-invoke")
	}

	// Assert CallbackError taxonomy.
	if !out.CallbackErrorInfo.Matched {
		t.Error("CallbackErrorInfo: expected Matched=true")
	}
	// An external failure is the CallbackExternalError subtype; it still
	// matches CallbackError, which is how the handler caught it.
	if out.CallbackErrorInfo.TypeName != "CallbackExternalError" {
		t.Errorf("CallbackErrorInfo.TypeName = %q, want %q", out.CallbackErrorInfo.TypeName, "CallbackExternalError")
	}
	if out.CallbackErrorInfo.OperationName != "failing-callback" {
		t.Errorf("CallbackErrorInfo.OperationName = %q, want %q", out.CallbackErrorInfo.OperationName, "failing-callback")
	}
	if !out.CallbackErrorInfo.IsOpError {
		t.Error("CallbackErrorInfo: expected IsOpError=true (OperationError super-type)")
	}
	if out.CallbackErrorInfo.OpErrorName != "failing-callback" {
		t.Errorf("CallbackErrorInfo.OpErrorName = %q, want %q", out.CallbackErrorInfo.OpErrorName, "failing-callback")
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
