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

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output.Receipt.ChargeID != "ch_123" || output.Receipt.Amount != 42.50 {
		t.Errorf("unexpected receipt %+v", output.Receipt)
	}
	if output.Reserved != 3 {
		t.Errorf("expected Reserved=3, got %d", output.Reserved)
	}
	if !output.Notified || !output.Completed {
		t.Errorf("expected Notified and Completed, got %+v", output)
	}

	// Concurrent branches checkpoint in scheduling-dependent order, so the
	// signature is compared as a set.
	extest.AssertSignature(t, result, extest.Unordered)
}
