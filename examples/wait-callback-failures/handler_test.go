// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	// Set dummy credentials so config.LoadDefaultConfig resolves quickly
	// and the Lambda API call fails fast with an auth error rather than
	// timing out searching for real credentials.
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	// The handler's submitter calls the real Lambda API
	// (SendDurableExecutionCallbackFailure) which is unavailable in local
	// testing. The submitter step exhausts retries, WaitForCallback returns
	// a CallbackSubmitterError, and the handler catches it as a
	// CallbackError. In the cloud the submitter succeeds and the external
	// failure is caught the same way.
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded (callback failure caught), got %s", result.Status)
	}
	out, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out.Success {
		t.Error("expected Success=false")
	}
	if out.Mode != "submitter" {
		t.Errorf("Mode = %q, want %q (no AWS endpoint locally)", out.Mode, "submitter")
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
