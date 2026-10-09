// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// targetInput and targetResult mirror the wire types of the deployed
// invoke-simple-target.
type targetInput struct {
	OrderID string `json:"orderId,omitempty"`
	Stage   string `json:"stage,omitempty"`
	Message string `json:"message,omitempty"`
}

type targetResult struct {
	OrderID string          `json:"orderId,omitempty"`
	Stage   string          `json:"stage,omitempty"`
	Status  string          `json:"status"`
	Input   json.RawMessage `json:"input"`
}

// target stands in for the deployed invoke-simple-target: it reports the
// request as completed and echoes the input it decoded.
func target(_ context.Context, in targetInput) (targetResult, error) {
	echo, err := json.Marshal(in)
	if err != nil {
		return targetResult{}, err
	}
	return targetResult{OrderID: in.OrderID, Stage: in.Stage, Status: "completed", Input: echo}, nil
}

func TestHandler(t *testing.T) {
	targetFunction := extest.TargetFunction("invoke-simple-target")
	input := Input{TargetFunction: targetFunction, OrderID: "order-123"}

	runner := durabletest.NewLocalRunner(handler)
	runner.RegisterFunction(targetFunction, durabletest.PlainFunction(target))

	result, err := runner.RunUntilComplete(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	out, err := durabletest.ResultAs[StageResult](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out.OrderID != "order-123" || out.Stage != "validate" {
		t.Errorf("unexpected result %+v", out)
	}

	// resultSerdes decoded the result: the target sent "completed".
	if out.Status != "COMPLETED" {
		t.Errorf("expected Status %q from resultSerdes, got %q", "COMPLETED", out.Status)
	}

	// requestSerdes encoded the input: the target received the stamp the
	// handler never set.
	var received targetInput
	if err := json.Unmarshal(out.Input, &received); err != nil {
		t.Fatalf("unmarshal echoed input: %v", err)
	}
	want := targetInput{OrderID: "order-123", Stage: "validate", Message: requestStamp}
	if received != want {
		t.Errorf("target received %+v, want %+v", received, want)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
