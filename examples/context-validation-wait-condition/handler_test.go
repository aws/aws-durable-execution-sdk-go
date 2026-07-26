// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build durablecheck

package main

import (
	"path/filepath"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	// The handler uses a parent context from a child goroutine inside
	// WaitForCondition, triggering ErrWrongGoroutine.
	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed (ErrWrongGoroutine), got %s", result.Status)
	}

	if result.Error == nil {
		t.Fatal("expected error details")
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}
