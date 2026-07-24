// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	input := Input{Name: "test-record"}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, input)

	// The handler uses rand.Float64() for transient failures and conditional
	// logic, so the outcome is non-deterministic. Either terminal state is valid.
	switch result.Status {
	case durabletest.Succeeded:
		output, err := durabletest.ResultAs[*Record](result)
		if err != nil {
			t.Fatalf("deserialize result: %v", err)
		}
		if output == nil {
			t.Fatal("expected non-nil Record on success")
		}
		if output.Name != "test-record" {
			t.Errorf("expected Name=%q, got %q", "test-record", output.Name)
		}
	case durabletest.Failed:
		if result.Error == nil {
			t.Fatal("expected error details on failure")
		}
	default:
		t.Fatalf("expected Succeeded or Failed, got %s", result.Status)
	}

	// Non-deterministic: golden file skipped due to random retry behavior.
}
