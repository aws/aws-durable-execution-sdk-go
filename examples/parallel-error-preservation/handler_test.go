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
	if len(output.Success) != 1 {
		t.Errorf("expected 1 success, got %d", len(output.Success))
	}
	if output.TotalErrors != 1 {
		t.Errorf("expected TotalErrors=1, got %d", output.TotalErrors)
	}
	if len(output.Errors) != 1 {
		t.Fatalf("expected 1 error info, got %d", len(output.Errors))
	}
	if !output.Errors[0].IsStep {
		t.Error("expected error to be a StepError")
	}

	// NOTE: Golden signature assertion is skipped for this example because
	// Parallel operations produce non-deterministic operation ordering.
}
