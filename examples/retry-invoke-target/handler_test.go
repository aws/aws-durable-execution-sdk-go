// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	// Test with an attempt that should succeed (at threshold).
	input := TargetInput{Attempt: 3, FailUntilAttempt: 3}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, input)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	out, err := durabletest.ResultAs[TargetResult](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out.Attempt != 3 {
		t.Errorf("expected Attempt=3, got %d", out.Attempt)
	}
	if out.Message != "success on attempt 3" {
		t.Errorf("expected message %q, got %q", "success on attempt 3", out.Message)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

func TestHandler_Failure(t *testing.T) {
	// Test with an attempt below threshold — should fail.
	input := TargetInput{Attempt: 1, FailUntilAttempt: 3}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, input)

	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error details")
	}

	// The target checkpoints no operation of its own on either path;
	// the golden records that the failing path adds none.
	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.failure.golden")
}
