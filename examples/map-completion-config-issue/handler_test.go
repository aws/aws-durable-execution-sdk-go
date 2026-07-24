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
	if output.TotalItems != 5 {
		t.Errorf("expected TotalItems=5, got %d", output.TotalItems)
	}
	if output.SuccessfulCount < 2 {
		t.Errorf("expected at least 2 successful items, got %d", output.SuccessfulCount)
	}
	if !output.HasFailures {
		t.Error("expected HasFailures=true")
	}
}
