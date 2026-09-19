// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
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

	// With MaxConcurrency=1 (sequential), 10 items, and threshold=25%:
	//   - idx 2 fails: (1*100)/10 = 10%, not > 25 → continue
	//   - idx 5 fails: (2*100)/10 = 20%, not > 25 → continue
	//   - idx 8 fails: (3*100)/10 = 30%, IS > 25 → early stop
	// Item at index 9 is never started.
	// Dispatched: 9 items (indices 0-8), 6 succeeded, 3 failed.
	if output.SuccessCount != 6 {
		t.Errorf("expected SuccessCount=6, got %d", output.SuccessCount)
	}
	if output.FailureCount != 3 {
		t.Errorf("expected FailureCount=3, got %d", output.FailureCount)
	}
	if output.TotalCount != 9 {
		t.Errorf("expected TotalCount=9, got %d", output.TotalCount)
	}
	if !output.HasFailure {
		t.Error("expected HasFailure=true")
	}
	if output.CompletionReason != "FAILURE_TOLERANCE_EXCEEDED" {
		t.Errorf("expected CompletionReason=FAILURE_TOLERANCE_EXCEEDED, got %s", output.CompletionReason)
	}

	// Verify the successful results are preserved with their correct payloads.
	// MaxConcurrency=1 processes items sequentially; Results() returns them in
	// input order. Items at indices 0,1,3,4,6,7 succeed (index%3!=2).
	expected := []string{
		"Item 0 processed",
		"Item 1 processed",
		"Item 3 processed",
		"Item 4 processed",
		"Item 6 processed",
		"Item 7 processed",
	}
	if len(output.Results) != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), len(output.Results))
	}
	for i, want := range expected {
		if output.Results[i] != want {
			t.Errorf("Results[%d] = %q, want %q", i, output.Results[i], want)
		}
	}

	// Concurrent branches checkpoint in scheduling-dependent order, so the
	// signature is compared as a set.
	extest.AssertSignature(t, result, extest.Unordered)
}
