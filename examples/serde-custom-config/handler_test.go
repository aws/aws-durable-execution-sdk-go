// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	input := event{
		OrderID: "order-42",
		Amount:  99.95,
	}

	runner := durabletest.NewLocalRunner(handler, durable.WithSerdes(&envelopeSerdes{}))
	result := runner.RunUntilComplete(t, input)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	out, err := durabletest.ResultAs[output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out.ID != "order-42" {
		t.Errorf("expected ID=%q, got %q", "order-42", out.ID)
	}
	if out.Amount != 99.95 {
		t.Errorf("expected Amount=99.95, got %v", out.Amount)
	}
	if out.Status != "processed" {
		t.Errorf("expected Status=%q, got %q", "processed", out.Status)
	}
	expectedSummary := "Order order-42: $99.95 (processed)"
	if out.Summary != expectedSummary {
		t.Errorf("expected Summary=%q, got %q", expectedSummary, out.Summary)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
