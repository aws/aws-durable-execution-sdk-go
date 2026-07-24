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
	if output.SuccessStep != "Success" {
		t.Errorf("expected SuccessStep=%q, got %q", "Success", output.SuccessStep)
	}
	if len(output.ScenariosTested) != 4 {
		t.Errorf("expected 4 scenarios tested, got %d", len(output.ScenariosTested))
	}

	// NOTE: Golden signature assertion is skipped for this example because
	// multiple concurrent StepAsync futures produce non-deterministic operation
	// ordering and the concurrent map accesses cause flaky panics in the local
	// test runner.
}
