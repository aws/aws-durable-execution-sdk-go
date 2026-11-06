// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// executionStartedAt returns the time of the ExecutionStarted event.
func executionStartedAt(t *testing.T, result *durabletest.TestResult) time.Time {
	t.Helper()
	for _, ev := range result.Events {
		if ev.EventType == types.EventTypeExecutionStarted && ev.EventTimestamp != nil {
			return *ev.EventTimestamp
		}
	}
	t.Fatal("no ExecutionStarted event")
	return time.Time{}
}

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
	if out.StartedAt.IsZero() {
		t.Error("ExecutionStartTime returned the zero time")
	}
	if got := out.Deadline.Sub(out.StartedAt); got != replyWindow {
		t.Errorf("deadline is %s after the start, want %s", got, replyWindow)
	}
	// The local runner takes the start time from the clock reading it
	// records in the ExecutionStarted event, so the two are equal.
	if runner.Local() {
		if want := executionStartedAt(t, result); !out.StartedAt.Equal(want) {
			t.Errorf("startedAt = %s, want the ExecutionStarted time %s", out.StartedAt, want)
		}
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
