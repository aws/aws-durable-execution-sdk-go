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
	// ErrorData must survive two RunInChildContext boundary crossings.
	if !output.Found {
		t.Error("expected Found=true (ErrorData preserved)")
	}
	if output.ErrorData != sentinel {
		t.Errorf("ErrorData = %q, want %q", output.ErrorData, sentinel)
	}
	if output.ErrorType != "ChildContextError" {
		t.Errorf("ErrorType = %q, want %q (the inner child context failed)", output.ErrorType, "ChildContextError")
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
