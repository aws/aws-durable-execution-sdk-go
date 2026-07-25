// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"sort"
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
	if output.SuccessCount != 2 {
		t.Errorf("expected SuccessCount=2, got %d", output.SuccessCount)
	}
	if output.FailureCount != 2 {
		t.Errorf("expected FailureCount=2, got %d", output.FailureCount)
	}
	if output.TotalCount != 4 {
		t.Errorf("expected TotalCount=4, got %d", output.TotalCount)
	}
	if output.CompletionReason != "FAILURE_TOLERANCE_EXCEEDED" {
		t.Errorf("expected CompletionReason=FAILURE_TOLERANCE_EXCEEDED, got %s", output.CompletionReason)
	}
	if !output.HasFailure {
		t.Error("expected HasFailure=true")
	}

	// Verify that branches which succeeded have their results preserved.
	// Sort because parallel branch completion order is nondeterministic.
	got := make([]string, len(output.SuccessResults))
	copy(got, output.SuccessResults)
	sort.Strings(got)
	want := []string{"result-1", "result-3"}
	if len(got) != len(want) {
		t.Fatalf("expected %d success results, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("success result[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// Assert that all expected operations are present using unordered
	// subset matching — parallel branch ordering is nondeterministic.
	durabletest.AssertSignatureContains(t, result, []durabletest.OperationSignature{
		{Type: "STEP", SubType: "Step", Name: "branch-1", Status: "SUCCEEDED"},
		{Type: "STEP", SubType: "Step", Name: "branch-2", Status: "FAILED"},
		{Type: "STEP", SubType: "Step", Name: "branch-3", Status: "SUCCEEDED"},
		{Type: "STEP", SubType: "Step", Name: "branch-4", Status: "FAILED"},
	})
}
