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
		TargetFunction: "arn:aws:lambda:us-east-1:123456789012:function:target",
		OrderID:        "order-123",
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, input)

	// Execution suspends because chained invokes need external resolution.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting chained invoke), got %s", result.Status)
	}

	// Complete the first chained invoke: "chain-validate"
	stageResult := StageResult{OrderID: "order-123", Stage: "validate", Status: "ok"}
	if err := runner.CompleteChainedInvoke("chain-validate", stageResult); err != nil {
		t.Fatalf("complete chain-validate: %v", err)
	}

	result = runner.RunUntilComplete(t, input)
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting chain-process), got %s", result.Status)
	}

	// Complete the second chained invoke: "chain-process"
	stageResult = StageResult{OrderID: "order-123", Stage: "process", Status: "ok"}
	if err := runner.CompleteChainedInvoke("chain-process", stageResult); err != nil {
		t.Fatalf("complete chain-process: %v", err)
	}

	result = runner.RunUntilComplete(t, input)
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting chain-confirm), got %s", result.Status)
	}

	// Complete the third chained invoke: "chain-confirm"
	stageResult = StageResult{OrderID: "order-123", Stage: "confirm", Status: "ok"}
	if err := runner.CompleteChainedInvoke("chain-confirm", stageResult); err != nil {
		t.Fatalf("complete chain-confirm: %v", err)
	}

	result = runner.RunUntilComplete(t, input)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[json.RawMessage](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	var finalResult Result
	if err := json.Unmarshal(output, &finalResult); err != nil {
		t.Fatalf("unmarshal Result: %v", err)
	}
	if !finalResult.Complete {
		t.Error("expected Complete=true")
	}
	if len(finalResult.Stages) != 3 {
		t.Errorf("expected 3 stages, got %d", len(finalResult.Stages))
	}

	durabletest.AssertGoldenSignature(t, result, filepath.Join("testdata", "signature.golden"))
}
