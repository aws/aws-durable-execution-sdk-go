// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	// Branch A is slow; B and C finish first, satisfying the (B AND C) arm.
	if output.SuccessCount != 2 {
		t.Errorf("expected SuccessCount == 2, got %d", output.SuccessCount)
	}
	if output.TotalCount != 3 {
		t.Errorf("expected TotalCount == 3, got %d", output.TotalCount)
	}
	if output.CompletionReason != "CUSTOM_COMPLETION_SUCCEEDED" {
		t.Errorf("expected CompletionReason CUSTOM_COMPLETION_SUCCEEDED, got %q", output.CompletionReason)
	}
	// Results are ordered by branch index, so B precedes C.
	if want := []string{"Branch B done", "Branch C done"}; !reflect.DeepEqual(output.Results, want) {
		t.Errorf("expected Results %v, got %v", want, output.Results)
	}

	// A is still running when the quorum (B AND C) is met, so the batch
	// reports it as started and abandoned rather than succeeded.
	if output.StartedCount != 1 {
		t.Errorf("expected StartedCount == 1, got %d", output.StartedCount)
	}

	// The checkpoint log agrees with the result. The branches are unnamed,
	// so they are identified by position: the batch checkpoints each
	// branch's START in index order before dispatching it. Branch A stays
	// STARTED; B and C are SUCCEEDED.
	var branches []durabletest.TestOperation
	for _, op := range result.OperationsByType("CONTEXT") {
		if op.SubType == "ParallelBranch" {
			branches = append(branches, op)
		}
	}
	if len(branches) != 3 {
		t.Fatalf("expected 3 ParallelBranch operations, got %d", len(branches))
	}
	for i, want := range []string{"STARTED", "SUCCEEDED", "SUCCEEDED"} {
		if branches[i].Status != want {
			t.Errorf("branch %d status = %s, want %s", i, branches[i].Status, want)
		}
	}
	if parent := result.Operation("quorum-branches"); parent == nil || parent.Status != "SUCCEEDED" {
		t.Errorf("expected Parallel context quorum-branches SUCCEEDED, got %+v", parent)
	}

	// NOTE: Golden signature assertion is skipped for this example because
	// Parallel operations produce non-deterministic operation ordering.
}
