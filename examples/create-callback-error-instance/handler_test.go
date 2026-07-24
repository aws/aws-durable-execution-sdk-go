// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
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

	// First callback (timeout-test) suspends waiting for external resolution.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting timeout-test callback), got %s", result.Status)
	}

	// Simulate the timeout-test callback timing out.
	callbacks := runner.OpenCallbacks()
	if len(callbacks) != 1 {
		t.Fatalf("expected 1 open callback, got %d", len(callbacks))
	}
	if err := runner.TimeoutCallback(callbacks[0].CallbackID); err != nil {
		t.Fatalf("timeout callback: %v", err)
	}

	result = runner.RunUntilComplete(t, nil)

	// After timeout, the handler creates the failure-test callback then its
	// "send-failure" Step calls the real Lambda API which is unavailable
	// locally. The step exhausts retries and the execution fails.
	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed (no real AWS endpoint for send-failure step), got %s", result.Status)
	}

	if result.Error == nil {
		t.Fatal("expected error details")
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}
