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

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}
