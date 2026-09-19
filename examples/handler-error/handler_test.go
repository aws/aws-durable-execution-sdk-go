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

	// The handler returns an error directly, so execution should be FAILED.
	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}

	if result.Error == nil {
		t.Fatal("expected non-nil error on FAILED result")
	}
	if result.Error.Message == "" {
		t.Error("expected non-empty error message")
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
