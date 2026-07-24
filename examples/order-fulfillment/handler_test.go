// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	input := Order{
		OrderID:    "ORD-001",
		CustomerID: "CUST-42",
		Items: []Item{
			{SKU: "WIDGET-A", Quantity: 2, Price: 9.99},
			{SKU: "GADGET-B", Quantity: 1, Price: 24.99},
		},
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, input)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[FulfillmentResult](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output.OrderID != "ORD-001" {
		t.Errorf("expected OrderID=%q, got %q", "ORD-001", output.OrderID)
	}
	if output.Status != "FULFILLED" {
		t.Errorf("expected Status=%q, got %q", "FULFILLED", output.Status)
	}
	expectedTotal := 2*9.99 + 24.99
	if output.Total != expectedTotal {
		t.Errorf("expected Total=%v, got %v", expectedTotal, output.Total)
	}
	if output.PaymentID == "" {
		t.Error("expected non-empty PaymentID")
	}
	if output.TrackingNumber == "" {
		t.Error("expected non-empty TrackingNumber")
	}
	if output.LabelURL == "" {
		t.Error("expected non-empty LabelURL")
	}

	// NOTE: Golden signature assertion is skipped for this example because
	// Parallel with default concurrency produces non-deterministic operation ordering.
}
