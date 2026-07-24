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

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[[]string](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if len(output) != 2 {
		t.Fatalf("expected 2 results, got %d", len(output))
	}
	// Results from durable.All are returned in input order.
	if output[0] != "task 1 result" {
		t.Errorf("expected %q, got %q", "task 1 result", output[0])
	}
	if output[1] != "task 2 result" {
		t.Errorf("expected %q, got %q", "task 2 result", output[1])
	}

	// NOTE: Golden signature assertion is skipped for this example because
	// concurrent Go child contexts produce non-deterministic operation ordering.
}
