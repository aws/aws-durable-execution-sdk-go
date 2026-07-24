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
	if output.SuccessCount != 3 {
		t.Errorf("expected SuccessCount=3, got %d", output.SuccessCount)
	}
	if output.FailureCount != 2 {
		t.Errorf("expected FailureCount=2, got %d", output.FailureCount)
	}
	if output.TotalCount != 5 {
		t.Errorf("expected TotalCount=5, got %d", output.TotalCount)
	}
	if !output.HasFailure {
		t.Error("expected HasFailure=true")
	}

	// NOTE: Golden signature assertion is skipped for this example because
	// Map operations produce non-deterministic operation ordering.
}
