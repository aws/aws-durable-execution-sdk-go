// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	input := Input{MaxAttempts: 3}

	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, input)

	if runner.Cloud() {
		// Nothing resolves the callbacks in the cloud: each attempt times
		// out and the handler fails after the last one, as documented in
		// examples/README.md. Locally the test approves the first
		// callback and the handler succeeds on attempt 1.
		if result.Status != durabletest.Failed {
			t.Fatalf("expected Failed (callbacks time out), got %s", result.Status)
		}
		if result.Error == nil || result.Error.Type != "CallbackTimeoutError" {
			t.Fatalf("expected CallbackTimeoutError, got %+v", result.Error)
		}
		// Every attempt times out, so the deployed run checkpoints three
		// callbacks; the local run below resolves the first. Each mode has
		// its own golden.
		extest.AssertSignatureFile(t, result, extest.Ordered, extest.CloudGoldenPath)
		return
	}

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

	// The local golden records the first callback being approved.
	extest.AssertSignature(t, result, extest.Ordered)
}
