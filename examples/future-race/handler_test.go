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

	output, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	// Race returns the first to settle; all are steps so one will win.
	if output == "" {
		t.Error("expected non-empty race result")
	}

	// NOTE: Golden signature assertion is skipped for this example because
	// StepAsync futures with Race may produce non-deterministic operation
	// ordering due to concurrent goroutine scheduling.
}
