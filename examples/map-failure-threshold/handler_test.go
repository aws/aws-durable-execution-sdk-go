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

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output.CompletionReason != "FAILURE_TOLERANCE_EXCEEDED" {
		t.Errorf("expected CompletionReason=%q, got %q", "FAILURE_TOLERANCE_EXCEEDED", output.CompletionReason)
	}
	if output.FailureCount < 3 {
		t.Errorf("expected at least 3 failures, got %d", output.FailureCount)
	}
	if output.TotalCount != 5 {
		t.Errorf("expected TotalCount=5, got %d", output.TotalCount)
	}
}
