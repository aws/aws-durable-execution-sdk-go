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

	// The handler's submitter returns an error directly with NoRetry.
	// The execution continues with the error captured as a
	// CallbackSubmitterError, then verifies the error type in a
	// subsequent step.
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	out, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if !out.IsCallbackError {
		t.Error("expected IsCallbackError=true")
	}
	if !out.IsSubmitterError {
		t.Error("expected IsSubmitterError=true")
	}
	if out.ErrorType != "Error" {
		t.Errorf("ErrorType = %q, want %q (an unnamed error type)", out.ErrorType, "Error")
	}
	if out.ErrorMessage != "submitter failed" {
		t.Errorf("ErrorMessage = %q, want %q", out.ErrorMessage, "submitter failed")
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}
