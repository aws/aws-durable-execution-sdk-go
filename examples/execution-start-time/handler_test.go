// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	// The wait splits the execution into two invocations: the deadline
	// is recorded in the first and derived again in the second.
	if len(result.Invocations) < 2 {
		t.Fatalf("expected at least 2 invocations, got %d", len(result.Invocations))
	}

	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	// ExecutionStartTime returned the same value in both invocations, so
	// the deadline the second derived equals the one the first recorded.
	if !out.MatchesFirstInvocation {
		t.Errorf("deadline derived in the second invocation (%s) differs from the one recorded in the first", out.Deadline)
	}
	if got := out.Deadline.Sub(out.StartedAt); got != replyWindow {
		t.Errorf("deadline is %s after the start, want %s", got, replyWindow)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
