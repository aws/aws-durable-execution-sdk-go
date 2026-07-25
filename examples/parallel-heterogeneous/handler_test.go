// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)

	// First run: Step and Wait complete locally; Invoke suspends.
	result := runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invoke), got %s", result.Status)
	}

	// Complete the chained invoke with a string result.
	if err := runner.CompleteChainedInvoke("child-function", json.RawMessage(`"invoked: child returned"`)); err != nil {
		t.Fatalf("CompleteChainedInvoke: %v", err)
	}

	// Second run: all branches resolved.
	result = runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	// Verify all three results are present. Branch completion order is
	// nondeterministic, so sort before comparing.
	expected := []string{
		"computed: 7*6=42",
		"invoked: child returned",
		"waited: 1s elapsed",
	}
	got := make([]string, len(output.Results))
	copy(got, output.Results)
	sort.Strings(got)

	if len(got) != len(expected) {
		t.Fatalf("expected %d results, got %d: %v", len(expected), len(got), got)
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("result[%d] = %q, want %q", i, got[i], expected[i])
		}
	}

	if output.CompletionReason != "ALL_COMPLETED" {
		t.Errorf("expected CompletionReason=ALL_COMPLETED, got %s", output.CompletionReason)
	}

	// Verify the final invocation signature contains the Parallel context
	// and the chained invoke operation. Step and Wait operations may or
	// may not appear in the final invocation's signature depending on
	// whether they were terminally checkpointed in an earlier invocation.
	durabletest.AssertSignatureContains(t, result, []durabletest.OperationSignature{
		{Type: "CONTEXT", SubType: "Parallel", Name: "heterogeneous-branches", Status: "SUCCEEDED"},
		{Type: "CHAINED_INVOKE", SubType: "ChainedInvoke", Name: "child-function", Status: "SUCCEEDED"},
	})
}
