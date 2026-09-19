// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large-scale map test in short mode")
	}

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

	// Concurrent branches checkpoint in scheduling-dependent order, so the
	// signature is compared as a set.
	extest.AssertSignature(t, result, extest.Unordered)
}
