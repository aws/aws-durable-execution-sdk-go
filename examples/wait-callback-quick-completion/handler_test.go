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
	// (SendDurableExecutionCallbackSuccess) which is unavailable in local
	// testing. The submitter step exhausts retries and the execution fails.
	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed (no real AWS endpoint for callback submitter), got %s", result.Status)
	}

	if result.Error == nil {
		t.Fatal("expected error details")
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
