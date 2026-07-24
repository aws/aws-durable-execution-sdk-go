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

	output, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	// The race should resolve quickly (via the shorter wait timer).
	// ElapsedMs should be non-negative.
	if output.ElapsedMs < 0 {
		t.Errorf("expected non-negative ElapsedMs, got %d", output.ElapsedMs)
	}

	// NOTE: Golden signature assertion is skipped for this example because
	// WaitAsync futures with Race may produce non-deterministic operation
	// ordering across runs.
}
