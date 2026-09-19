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

	// The handler catches the error and returns a Result (Succeeded).
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if !output.Found {
		t.Error("expected Found=true (error was caught)")
	}
	if output.Depth < 1 {
		t.Errorf("expected depth >= 1, got %d", output.Depth)
	}
	if !output.IsChild {
		t.Error("expected IsChild=true")
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
