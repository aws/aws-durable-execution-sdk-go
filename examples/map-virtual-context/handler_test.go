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

	// Assert results are correct and in order.
	expected := []int{2, 4, 6, 8, 10}
	if len(output.ProcessedItems) != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), len(output.ProcessedItems))
	}
	for i, v := range expected {
		if output.ProcessedItems[i] != v {
			t.Errorf("result[%d]: expected %d, got %d", i, v, output.ProcessedItems[i])
		}
	}
	if output.TotalCount != 5 {
		t.Errorf("expected TotalCount 5, got %d", output.TotalCount)
	}
	if output.SuccessCount != 5 {
		t.Errorf("expected SuccessCount 5, got %d", output.SuccessCount)
	}

	// The distinguishing assertion: NestingFlat suppresses per-item
	// MapIteration context events. With NestingNormal, each item would
	// produce a CONTEXT/MapIteration/SUCCEEDED operation. With NestingFlat,
	// these must NOT be present — item operations are minted directly under
	// the parent batch context.
	durabletest.AssertSignatureExcludes(t, result, []durabletest.OperationSignature{
		{Type: "CONTEXT", SubType: "MapIteration", Status: "SUCCEEDED"},
		{Type: "CONTEXT", SubType: "MapIteration", Status: "STARTED"},
	})

	// The parent Map context itself must still be present with status
	// SUCCEEDED — only per-item contexts are suppressed.
	durabletest.AssertSignatureContains(t, result, []durabletest.OperationSignature{
		{Type: "CONTEXT", SubType: "Map", Name: "process-items", Status: "SUCCEEDED"},
	})
}
