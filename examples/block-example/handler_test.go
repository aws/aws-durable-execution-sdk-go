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
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	// The wait inside the nested block suspends the first invocation; the
	// second invocation replays and completes.
	if got := len(result.Invocations); got != 2 {
		t.Fatalf("expected 2 invocations, got %d", got)
	}

	want := blockResult{NestedStep: "nested step result", NestedBlock: "nested block result"}
	out, err := durabletest.ResultAs[blockResult](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out != want {
		t.Errorf("result = %+v, want %+v", out, want)
	}

	// The parent block's checkpointed result is the same struct the
	// handler returned, so a replay restores it without re-entering the
	// block.
	parent := result.Operation("parent-block")
	if parent == nil || parent.ContextDetails == nil {
		t.Fatal("parent-block context operation not found")
	}
	var checkpointed blockResult
	if err := json.Unmarshal([]byte(parent.ContextDetails.Result), &checkpointed); err != nil {
		t.Fatalf("decode parent-block result %q: %v", parent.ContextDetails.Result, err)
	}
	if checkpointed != want {
		t.Errorf("parent-block checkpointed result = %+v, want %+v", checkpointed, want)
	}

	// Both nested operations are children of the parent block.
	for _, name := range []string{"nested-step", "nested-block"} {
		op := result.Operation(name)
		if op == nil {
			t.Fatalf("%s operation not found", name)
		}
		if op.ParentID != parent.ID {
			t.Errorf("%s ParentID = %q, want parent-block ID %q", name, op.ParentID, parent.ID)
		}
	}
	step := result.Operation("nested-step")
	if step.StepDetails == nil || step.StepDetails.Result != `"nested step result"` {
		t.Errorf("nested-step checkpointed result = %+v, want \"nested step result\"", step.StepDetails)
	}

	// Sequential nesting with no concurrency is deterministic, so the
	// operation sequence is compared position by position.
	extest.AssertSignature(t, result, extest.Ordered)
}
