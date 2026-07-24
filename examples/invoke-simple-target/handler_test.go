// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	input := GenericInput{
		OrderID: "order-123",
		Stage:   "validate",
		Message: "test message",
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, input)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[GenericResult](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output.OrderID != "order-123" {
		t.Errorf("expected OrderID %q, got %q", "order-123", output.OrderID)
	}
	if output.Stage != "validate" {
		t.Errorf("expected Stage %q, got %q", "validate", output.Stage)
	}
	if output.Status != "completed" {
		t.Errorf("expected Status %q, got %q", "completed", output.Status)
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}
