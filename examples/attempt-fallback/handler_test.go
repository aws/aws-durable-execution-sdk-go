// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
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

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}
