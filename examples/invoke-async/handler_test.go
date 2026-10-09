// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// target stands in for the deployed invoke-simple-target: it reports the
// stage it received as completed.
func target(_ context.Context, req StageRequest) (StageResult, error) {
	return StageResult{OrderID: req.OrderID, Stage: req.Stage, Status: "completed"}, nil
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

	out, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if len(out.Stages) != len(stages) {
		t.Fatalf("expected %d stages, got %+v", len(stages), out.Stages)
	}
	// Results come back in the order the futures were awaited, which is
	// the order the stages were started.
	for i, stage := range stages {
		want := StageResult{OrderID: "order-123", Stage: stage, Status: "completed"}
		if out.Stages[i] != want {
			t.Errorf("stage %d: expected %+v, got %+v", i, want, out.Stages[i])
		}
	}

	// Every invoke starts before any of them completes. With a blocking
	// Invoke per stage, each invoke would complete before the next one
	// started.
	started, firstSettled := 0, -1
	for i, ev := range result.Events {
		switch ev.EventType {
		case types.EventTypeChainedInvokeStarted:
			started++
			if firstSettled >= 0 {
				t.Errorf("invoke started at event %d, after one settled at event %d", i, firstSettled)
			}
		case types.EventTypeChainedInvokeSucceeded:
			if firstSettled < 0 {
				firstSettled = i
			}
		}
	}
	if started != len(stages) {
		t.Errorf("expected %d ChainedInvokeStarted events, got %d", len(stages), started)
	}

	// The three invokes start on their own goroutines, so their START
	// checkpoints land in scheduling-dependent order.
	extest.AssertSignature(t, result, extest.Unordered)
}
