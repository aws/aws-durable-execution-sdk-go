// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	// The option is rejected before any branch starts, so the handler
	// returns the validation error and the execution fails.
	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected non-nil error on Failed result")
	}
	if !strings.Contains(result.Error.Message, "max concurrency must be positive") {
		t.Errorf("expected max concurrency validation error, got %q", result.Error.Message)
	}

	// Nothing was checkpointed: the batch never started.
	if len(result.Operations) != 0 {
		t.Errorf("expected no operations, got %d", len(result.Operations))
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
