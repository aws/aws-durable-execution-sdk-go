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

	// The step uses rand.Float64() so it may succeed or fail after retries.
	// Either outcome is valid — we just verify it reaches a terminal state.
	switch result.Status {
	case durabletest.Succeeded:
		output, err := durabletest.ResultAs[string](result)
		if err != nil {
			t.Fatalf("deserialize result: %v", err)
		}
		if output != "step succeeded" {
			t.Errorf("expected %q, got %q", "step succeeded", output)
		}
	case durabletest.Failed:
		if result.Error == nil {
			t.Fatal("expected error details on failure")
		}
	default:
		t.Fatalf("expected Succeeded or Failed, got %s", result.Status)
	}

	// Non-deterministic: golden file skipped due to random retry behavior.
}
