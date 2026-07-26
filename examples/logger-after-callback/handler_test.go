// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
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
	res := runner.RunUntilComplete(t, nil)

	// The handler's submitter calls the real Lambda API
	// (SendDurableExecutionCallbackSuccess) which is unavailable in local
	// testing. The submitter step exhausts retries and the execution fails.
	if res.Status != durabletest.Failed {
		t.Fatalf("expected Failed (submitter needs real AWS), got %s", res.Status)
	}
}
