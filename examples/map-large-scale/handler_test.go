// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large-scale map test in short mode")
	}

	// Known issue: the SDK has a concurrent map access race in
	// executionState.get when running Map with high concurrency (10) and
	// many items (50). Skip until the SDK race is resolved.
	t.Skip("skipping: known concurrent map race in SDK batch execution (state.go:139)")

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if !output.Success {
		t.Error("expected Success=true")
	}
	if output.Summary.ItemsProcessed != 50 {
		t.Errorf("expected 50 items processed, got %d", output.Summary.ItemsProcessed)
	}
	if !output.Summary.AllItemsProcessed {
		t.Error("expected AllItemsProcessed=true")
	}
	if output.Summary.MaxConcurrency != 10 {
		t.Errorf("expected MaxConcurrency=10, got %d", output.Summary.MaxConcurrency)
	}
}
