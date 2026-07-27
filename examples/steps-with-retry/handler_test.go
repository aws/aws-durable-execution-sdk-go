// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	input := Input{Name: "test-record"}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, input)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	output, err := durabletest.ResultAs[*Record](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output == nil {
		t.Fatal("expected non-nil Record on success")
	}
	if output.Name != "test-record" {
		t.Errorf("expected Name=%q, got %q", "test-record", output.Name)
	}
}
