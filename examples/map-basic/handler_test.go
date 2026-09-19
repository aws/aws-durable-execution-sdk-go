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

	output, err := durabletest.ResultAs[[]int](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if len(output) != 5 {
		t.Fatalf("expected 5 results, got %d", len(output))
	}

	// Verify all expected values are present (order may vary due to concurrency).
	expected := map[int]bool{2: true, 4: true, 6: true, 8: true, 10: true}
	for _, v := range output {
		if !expected[v] {
			t.Errorf("unexpected value %d in results", v)
		}
		delete(expected, v)
	}
	if len(expected) > 0 {
		t.Errorf("missing values: %v", expected)
	}

	// Concurrent branches checkpoint in scheduling-dependent order, so the
	// signature is compared as a set.
	extest.AssertSignature(t, result, extest.Unordered)
}
