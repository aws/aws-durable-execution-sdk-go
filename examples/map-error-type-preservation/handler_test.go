// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	// Core assertion: error type info is preserved across replay.
	if !out.Preserved {
		t.Errorf("error type preservation failed: before=%+v after=%+v",
			out.BeforeReplay, out.AfterReplay)
	}

	// Verify ChildContextError is present on both phases.
	if !out.BeforeReplay.IsChildCtxErr {
		t.Error("expected ChildContextError before replay")
	}
	if !out.AfterReplay.IsChildCtxErr {
		t.Error("expected ChildContextError after replay")
	}

	// Verify StepError is present on both phases — this is the key
	// assertion that the fix enables.
	if !out.BeforeReplay.IsStepErr {
		t.Error("expected StepError before replay")
	}
	if !out.AfterReplay.IsStepErr {
		t.Error("expected StepError after replay")
	}

	// Verify StepError field values match between live and replay.
	if out.BeforeReplay.StepName != out.AfterReplay.StepName {
		t.Errorf("StepName mismatch: before=%q after=%q",
			out.BeforeReplay.StepName, out.AfterReplay.StepName)
	}
	if out.BeforeReplay.StepAttempts != out.AfterReplay.StepAttempts {
		t.Errorf("StepAttempts mismatch: before=%d after=%d",
			out.BeforeReplay.StepAttempts, out.AfterReplay.StepAttempts)
	}

	// Verify the leaf type name is preserved (user type as string).
	if out.BeforeReplay.LeafTypeName != "PaymentError" {
		t.Errorf("expected leaf type name %q, got %q",
			"PaymentError", out.BeforeReplay.LeafTypeName)
	}
	if out.AfterReplay.LeafTypeName != out.BeforeReplay.LeafTypeName {
		t.Errorf("leaf type name mismatch: before=%q after=%q",
			out.BeforeReplay.LeafTypeName, out.AfterReplay.LeafTypeName)
	}

	// Verify the leaf message is preserved.
	if out.BeforeReplay.LeafMessage != out.AfterReplay.LeafMessage {
		t.Errorf("leaf message mismatch: before=%q after=%q",
			out.BeforeReplay.LeafMessage, out.AfterReplay.LeafMessage)
	}

	// Verify OperationError is matchable on both phases.
	if !out.BeforeReplay.IsOperationError {
		t.Error("expected OperationError before replay")
	}
	if !out.AfterReplay.IsOperationError {
		t.Error("expected OperationError after replay")
	}

	// Verify batch counts.
	if out.SuccessCount != 2 {
		t.Errorf("expected 2 successes, got %d", out.SuccessCount)
	}
	if out.FailureCount != 1 {
		t.Errorf("expected 1 failure, got %d", out.FailureCount)
	}
}
