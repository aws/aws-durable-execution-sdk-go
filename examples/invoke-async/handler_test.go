// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

var input = Input{
	TargetFunction: "arn:aws:lambda:us-east-1:123456789012:function:target",
	OrderID:        "order-123",
}

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete(input)
	if err != nil {
		t.Fatal(err)
	}
	// Execution suspends because the invokes need external resolution.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invokes), got %s", result.Status)
	}

	// Every invoke is started before the invocation suspends on the
	// first result. With a blocking Invoke per stage, the invocation
	// would suspend on the first invoke and record only that one.
	var started []string
	for _, op := range result.Operations {
		if op.Type == "CHAINED_INVOKE" && op.Status == "STARTED" {
			started = append(started, op.Name)
		}
	}
	if len(started) != len(stages) {
		t.Fatalf("expected %d started invokes before suspending, got %v", len(stages), started)
	}

	for _, stage := range stages {
		reply := StageResult{OrderID: "order-123", Stage: stage, Status: "completed"}
		if err := runner.CompleteChainedInvoke(stage, reply); err != nil {
			t.Fatalf("complete %s: %v", stage, err)
		}
	}

	result, err = runner.RunUntilComplete(input)
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

	// InvokeAsync queues each START on the calling goroutine before it
	// returns, so the invokes are recorded in the order they were started.
	extest.AssertSignature(t, result, extest.Ordered)
}
