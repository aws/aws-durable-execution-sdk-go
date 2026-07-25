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

	// Assert terminal state.
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	// Assert correct results.
	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output.TotalCount != 3 {
		t.Errorf("expected TotalCount=3, got %d", output.TotalCount)
	}
	if output.SuccessCount != 3 {
		t.Errorf("expected SuccessCount=3, got %d", output.SuccessCount)
	}
	if len(output.Results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(output.Results))
	}

	// Verify result values. BatchResult.Results() returns items in input order
	// (reassembled by index after concurrent dispatch), so positional assertion
	// is valid regardless of goroutine scheduling.
	expected := []string{"fetched", "processed", "validated"}
	for i, want := range expected {
		if output.Results[i] != want {
			t.Errorf("Results[%d] = %q, want %q", i, output.Results[i], want)
		}
	}

	// Assert the checkpoint shape: the Parallel parent context MUST exist.
	durabletest.AssertSignatureContains(t, result, []durabletest.OperationSignature{
		{Type: "CONTEXT", SubType: "Parallel", Name: "parallel-tasks-virtual", Status: "SUCCEEDED"},
		{Type: "STEP", SubType: "Step", Name: "fetch", Status: "SUCCEEDED"},
		{Type: "STEP", SubType: "Step", Name: "process", Status: "SUCCEEDED"},
		{Type: "STEP", SubType: "Step", Name: "validate", Status: "SUCCEEDED"},
	})

	// THE DISTINGUISHING ASSERTION: flat nesting must suppress all
	// per-branch context operations. Under normal nesting these would
	// appear as CONTEXT/ParallelBranch entries; with NestingFlat they
	// must be absent.
	durabletest.AssertSignatureExcludes(t, result, []durabletest.OperationSignature{
		{Type: "CONTEXT", SubType: "ParallelBranch", Status: "SUCCEEDED"},
		{Type: "CONTEXT", SubType: "ParallelBranch", Status: "FAILED"},
		{Type: "CONTEXT", SubType: "ParallelBranch", Status: "STARTED"},
	})
}
