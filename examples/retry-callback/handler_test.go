// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	input := Input{MaxAttempts: 3}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, input)

	// First callback suspends the execution.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting callback), got %s", result.Status)
	}

	// Complete the open callback with success.
	callbacks := runner.OpenCallbacks()
	if len(callbacks) == 0 {
		t.Fatal("expected at least one open callback")
	}
	if err := runner.SendCallbackSuccess(callbacks[0].CallbackID, "approved"); err != nil {
		t.Fatalf("send callback success: %v", err)
	}

	result = runner.RunUntilComplete(t, input)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output.Value != "approved" {
		t.Errorf("expected Value=%q, got %q", "approved", output.Value)
	}
	if output.Attempts != 1 {
		t.Errorf("expected Attempts=1, got %d", output.Attempts)
	}

	// NOTE: Golden signature assertion is skipped because callback timing
	// and retry patterns can produce non-deterministic signatures.
}
