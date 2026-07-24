// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	input := Input{
		TargetFunction:   "arn:aws:lambda:us-east-1:123456789012:function:retry-invoke-target",
		FailUntilAttempt: 3,
		MaxAttempts:      3,
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, input)

	// First invoke suspends — awaiting external resolution.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invoke-1), got %s", result.Status)
	}

	// Fail invoke-1 (simulating target failure on attempt 1).
	if err := runner.FailChainedInvoke("invoke-1", "Error", "deliberate failure on attempt 1"); err != nil {
		t.Fatalf("fail invoke-1: %v", err)
	}

	result = runner.RunUntilComplete(t, input)
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invoke-2), got %s", result.Status)
	}

	// Fail invoke-2 (simulating target failure on attempt 2).
	if err := runner.FailChainedInvoke("invoke-2", "Error", "deliberate failure on attempt 2"); err != nil {
		t.Fatalf("fail invoke-2: %v", err)
	}

	result = runner.RunUntilComplete(t, input)
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invoke-3), got %s", result.Status)
	}

	// Succeed invoke-3 (target succeeds on attempt 3).
	targetResult := struct {
		Message string `json:"message"`
		Attempt int    `json:"attempt"`
	}{Message: "success on attempt 3", Attempt: 3}
	if err := runner.CompleteChainedInvoke("invoke-3", targetResult); err != nil {
		t.Fatalf("complete invoke-3: %v", err)
	}

	result = runner.RunUntilComplete(t, input)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	out, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out.Attempts != 3 {
		t.Errorf("expected Attempts=3, got %d", out.Attempts)
	}

	var response struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(out.Response, &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if response.Message != "success on attempt 3" {
		t.Errorf("expected message %q, got %q", "success on attempt 3", response.Message)
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}
