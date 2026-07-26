// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	// Set dummy credentials so config.LoadDefaultConfig resolves quickly.
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	// In the local runner, callback submitters cannot reach the Lambda
	// service, so only the 3 step branches succeed. In the cloud, all 5
	// branches succeed — the cloud integration test validates this.
	if output.SuccessCount < 3 {
		t.Errorf("expected SuccessCount >= 3, got %d", output.SuccessCount)
	}
	if output.TotalCount != 5 {
		t.Errorf("expected TotalCount == 5, got %d", output.TotalCount)
	}
	if output.CompletionReason != "MIN_SUCCESSFUL_REACHED" {
		t.Errorf("expected CompletionReason MIN_SUCCESSFUL_REACHED, got %q", output.CompletionReason)
	}

	// NOTE: Golden signature assertion is skipped for this example because
	// Parallel operations produce non-deterministic operation ordering.
}
