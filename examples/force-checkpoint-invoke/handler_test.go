// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	input := event{
		FunctionNames: []string{
			"arn:aws:lambda:us-east-1:123456789012:function:fn1",
			"arn:aws:lambda:us-east-1:123456789012:function:fn2",
			"arn:aws:lambda:us-east-1:123456789012:function:fn3",
		},
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, input)

	// Execution suspends because chained invokes need external resolution.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting chained invokes), got %s", result.Status)
	}

	// Complete invoke-1.
	if err := runner.CompleteChainedInvoke("invoke-1", json.RawMessage(`{"status":"ok-1"}`)); err != nil {
		t.Fatalf("complete invoke-1: %v", err)
	}

	result = runner.RunUntilComplete(t, input)
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invoke-2), got %s", result.Status)
	}

	// Complete invoke-2.
	if err := runner.CompleteChainedInvoke("invoke-2", json.RawMessage(`{"status":"ok-2"}`)); err != nil {
		t.Fatalf("complete invoke-2: %v", err)
	}

	result = runner.RunUntilComplete(t, input)
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invoke-3), got %s", result.Status)
	}

	// Complete invoke-3.
	if err := runner.CompleteChainedInvoke("invoke-3", json.RawMessage(`{"status":"ok-3"}`)); err != nil {
		t.Fatalf("complete invoke-3: %v", err)
	}

	result = runner.RunUntilComplete(t, input)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	extest.AssertSignature(t, result, extest.Unordered)
}
