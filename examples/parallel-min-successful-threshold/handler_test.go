// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"slices"
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

	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	// The two quick branches reach the threshold; the other three were
	// started and then abandoned, so every branch is in TotalCount.
	if out.SuccessCount != 2 || out.StartedCount != 3 || out.TotalCount != 5 {
		t.Errorf("counts = %d succeeded, %d started, %d total; want 2, 3, 5", out.SuccessCount, out.StartedCount, out.TotalCount)
	}
	if out.CompletionReason != "MIN_SUCCESSFUL_REACHED" {
		t.Errorf("completionReason = %q, want MIN_SUCCESSFUL_REACHED", out.CompletionReason)
	}
	if !slices.Equal(out.Succeeded, []string{"fast", "quick"}) {
		t.Errorf("succeeded = %v, want [fast quick]", out.Succeeded)
	}
	if !slices.Equal(out.Abandoned, []string{"slow", "slower", "straggler"}) {
		t.Errorf("abandoned = %v, want [slow slower straggler]", out.Abandoned)
	}

	// The Parallel completes after two successes. An abandoned branch that
	// has already started its step finishes it; one that has not yet
	// started it unwinds without starting it. Which case each abandoned
	// branch falls into depends on scheduling, so the golden lists the
	// operations every run produces: the batch, all five branch contexts,
	// and the two steps that succeed.
	extest.AssertSignature(t, result, extest.Subset)
}
