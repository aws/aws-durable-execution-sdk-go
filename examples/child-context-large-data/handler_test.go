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
	if !output.Success {
		t.Error("expected Success=true")
	}
	if output.Summary.StepsExecuted != 5 {
		t.Errorf("expected 5 steps executed, got %d", output.Summary.StepsExecuted)
	}
	if !output.Summary.ChildUsed {
		t.Error("expected ChildContextUsed=true")
	}
	// Expect ~275KB (5 * 55KB)
	expectedSize := 5 * 55 * 1024
	if output.Summary.TotalDataSize != expectedSize {
		t.Errorf("expected TotalDataSize=%d, got %d", expectedSize, output.Summary.TotalDataSize)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
