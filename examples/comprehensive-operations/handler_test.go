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

	output, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output.StepResult != "Step 1 completed successfully" {
		t.Errorf("unexpected step result: %q", output.StepResult)
	}
	if !output.WaitCompleted {
		t.Error("expected WaitCompleted=true")
	}
	if len(output.MapResults) != 5 {
		t.Errorf("expected 5 map results, got %d", len(output.MapResults))
	}
	expected := []int{2, 4, 6, 8, 10}
	for i, v := range output.MapResults {
		if v != expected[i] {
			t.Errorf("map result[%d]: expected %d, got %d", i, expected[i], v)
		}
	}
	if len(output.ParallelResults) != 3 {
		t.Errorf("expected 3 parallel results, got %d", len(output.ParallelResults))
	}

	// Concurrent branches checkpoint in scheduling-dependent order, so the
	// signature is compared as a set.
	extest.AssertSignature(t, result, extest.Unordered)
}
