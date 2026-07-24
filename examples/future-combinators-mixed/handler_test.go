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

	// All stage: 3 results in input order.
	if len(output.AllResults) != 3 {
		t.Fatalf("expected 3 AllResults, got %d", len(output.AllResults))
	}

	// Race stage: one of the two step results.
	if output.RaceResult == "" {
		t.Error("expected non-empty RaceResult")
	}

	// AllSettled stage: 2 settled outcomes (1 success + 1 failure).
	if output.SettledCount != 2 {
		t.Errorf("expected SettledCount=2, got %d", output.SettledCount)
	}

	// Any stage: should resolve to a success or "all failed" message.
	if output.AnyResult == "" {
		t.Error("expected non-empty AnyResult")
	}

	// NOTE: Golden signature assertion is skipped for this example because
	// StepAsync futures may produce non-deterministic operation ordering
	// due to concurrent goroutine scheduling.
}
