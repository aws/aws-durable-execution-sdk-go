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
	input := Input{
		FunctionName: "arn:aws:lambda:us-east-1:123456789012:function:tenant-target",
		TenantID:     "tenant-abc",
		Payload:      json.RawMessage(`{"seconds":1}`),
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, input)

	// Execution suspends because invokes need external resolution.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invoke-tenant), got %s", result.Status)
	}

	// Complete the chained invoke: "invoke-tenant"
	if err := runner.CompleteChainedInvoke("invoke-tenant", json.RawMessage(`"wait finished"`)); err != nil {
		t.Fatalf("complete invoke-tenant: %v", err)
	}

	result = runner.RunUntilComplete(t, input)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[json.RawMessage](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if string(output) != `"wait finished"` {
		t.Errorf("expected %q, got %q", `"wait finished"`, string(output))
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
