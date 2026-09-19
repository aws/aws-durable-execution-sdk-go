// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[BackorderResult](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output.Supplier != "supplier-c" {
		t.Errorf("expected supplier-c, got %s", output.Supplier)
	}
	if output.Attempt != 3 {
		t.Errorf("expected attempt 3, got %d", output.Attempt)
	}
	if output.Status != "backordered" {
		t.Errorf("expected status backordered, got %s", output.Status)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
