// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	// The target is the deployed retry-invoke-target companion in the
	// cloud. Locally the name is only a label: the test resolves each
	// invoke by hand below.
	input := Input{
		TargetFunction:   extest.TargetFunction("retry-invoke-target"),
		FailUntilAttempt: 3,
		MaxAttempts:      3,
	}

	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, input)

	if runner.Local() {
		// Locally each invoke suspends until the test resolves it. In
		// the cloud the companion answers: it fails attempts 1 and 2 and
		// succeeds on attempt 3, which is what these steps simulate.
		if result.Status != durabletest.Pending {
			t.Fatalf("expected Pending (awaiting invoke-1), got %s", result.Status)
		}
		if err := runner.FailChainedInvoke("invoke-1", "Error", "deliberate failure on attempt 1"); err != nil {
			t.Fatalf("fail invoke-1: %v", err)
		}

		result = runner.RunUntilComplete(t, input)
		if result.Status != durabletest.Pending {
			t.Fatalf("expected Pending (awaiting invoke-2), got %s", result.Status)
		}
		if err := runner.FailChainedInvoke("invoke-2", "Error", "deliberate failure on attempt 2"); err != nil {
			t.Fatalf("fail invoke-2: %v", err)
		}

		result = runner.RunUntilComplete(t, input)
		if result.Status != durabletest.Pending {
			t.Fatalf("expected Pending (awaiting invoke-3), got %s", result.Status)
		}
		targetResult := struct {
			Message string `json:"message"`
			Attempt int    `json:"attempt"`
		}{Message: "success on attempt 3", Attempt: 3}
		if err := runner.CompleteChainedInvoke("invoke-3", targetResult); err != nil {
			t.Fatalf("complete invoke-3: %v", err)
		}

		result = runner.RunUntilComplete(t, input)
	}

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
