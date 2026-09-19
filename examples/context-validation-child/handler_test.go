// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build !durablenocheck

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	// The handler uses a parent context from a child goroutine, triggering
	// ErrWrongGoroutine. This propagates as a handler failure.
	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed (ErrWrongGoroutine), got %s", result.Status)
	}

	if result.Error == nil {
		t.Fatal("expected error details")
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
