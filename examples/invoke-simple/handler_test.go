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
		FunctionName: "arn:aws:lambda:us-east-1:123456789012:function:target",
		Payload:      json.RawMessage(`{"message":"hello"}`),
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, input)

	// Execution suspends because invokes need external resolution.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invoke), got %s", result.Status)
	}

	// Complete the chained invoke: "invoke"
	if err := runner.CompleteChainedInvoke("invoke", json.RawMessage(`{"status":"ok"}`)); err != nil {
		t.Fatalf("complete invoke: %v", err)
	}

	result = runner.RunUntilComplete(t, input)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[json.RawMessage](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if string(output) != `{"status":"ok"}` {
		t.Errorf("expected %q, got %q", `{"status":"ok"}`, string(output))
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}
