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
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}

	if result.Error == nil {
		t.Fatal("expected non-nil error on Failed result")
	}
	if !strings.Contains(result.Error.Message, "persistent failure") {
		t.Errorf("expected error to contain %q, got %q", "persistent failure", result.Error.Message)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
