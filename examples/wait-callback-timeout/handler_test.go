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

	// Execution suspends because the submitter succeeds (returns nil)
	// but no external system completes the callback.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting callback), got %s", result.Status)
	}

	// Simulate the callback timing out.
	callbacks := runner.OpenCallbacks()
	if len(callbacks) != 1 {
		t.Fatalf("expected 1 open callback, got %d", len(callbacks))
	}
	if err := runner.TimeoutCallback(callbacks[0].CallbackID); err != nil {
		t.Fatalf("timeout callback: %v", err)
	}

	result = runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if !output.TimedOut {
		t.Error("expected TimedOut=true")
	}
	if output.Error == "" {
		t.Error("expected non-empty Error message")
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
