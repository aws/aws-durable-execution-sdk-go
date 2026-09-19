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

	output, err := durabletest.ResultAs[[]string](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if len(output) != 3 {
		t.Fatalf("expected 3 results, got %d", len(output))
	}
	// All returns results in input order.
	expected := []string{"result 1", "result 2", "result 3"}
	for i, want := range expected {
		if output[i] != want {
			t.Errorf("output[%d] = %q, want %q", i, output[i], want)
		}
	}

	// Concurrent branches checkpoint in scheduling-dependent order, so the
	// signature is compared as a set.
	extest.AssertSignature(t, result, extest.Unordered)
}
