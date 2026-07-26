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

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	// With MinSuccessful=2 and MaxConcurrency=3 over 5 items (2 of which
	// always fail after MaxAttempts=2), the batch completes with reason
	// MIN_SUCCESSFUL_REACHED. The failing items (indices 1,3) always
	// exhaust their retries, so HasFailures is always true.
	//
	// The number of items dispatched before the completion signal is not
	// deterministic, so only the structural invariant is asserted:
	// TotalItems == SuccessfulCount + FailedCount.
	if output.TotalItems != output.SuccessfulCount+output.FailedCount {
		t.Errorf("expected TotalItems == SuccessfulCount + FailedCount, got %d != %d + %d",
			output.TotalItems, output.SuccessfulCount, output.FailedCount)
	}
	if output.SuccessfulCount < 2 {
		t.Errorf("expected SuccessfulCount >= 2 (MinSuccessful guarantee), got %d", output.SuccessfulCount)
	}
	if output.FailedCount != 2 {
		t.Errorf("expected FailedCount == 2 (items 1,3 always exhaust MaxAttempts=2), got %d", output.FailedCount)
	}
	if !output.HasFailures {
		t.Error("expected HasFailures == true (items 1,3 fail)")
	}
	if output.BatchStatus != "FAILED" {
		t.Errorf("expected BatchStatus == FAILED (HasFailures is true), got %s", output.BatchStatus)
	}
	if output.CompletionNote != "MIN_SUCCESSFUL_REACHED" {
		t.Errorf("expected CompletionNote == MIN_SUCCESSFUL_REACHED, got %s", output.CompletionNote)
	}
}
