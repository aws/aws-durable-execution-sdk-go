// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// runAcrossSuspension drives the handler through its suspension on the
// after-map wait so the second invocation replays the map, and asserts
// both invocations happened.
func runAcrossSuspension(t *testing.T, event Event) *durabletest.TestResult {
	t.Helper()
	runner := durabletest.NewLocalRunner(handler)

	first := runner.Run(t, event)
	if first.Status != durabletest.Pending {
		t.Fatalf("first invocation status = %s, want PENDING (the after-map wait must suspend)", first.Status)
	}
	if !runner.CompletePendingTimers() {
		t.Fatal("no pending timer to complete after the first invocation")
	}

	result := runner.Run(t, event)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("second invocation status = %s, want SUCCEEDED", result.Status)
	}
	return result
}

func assertOutput(t *testing.T, result *durabletest.TestResult) {
	t.Helper()
	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	// Items 0 and 2 succeeded, item 1 was in flight when MinSuccessful
	// was reached, and items 3 and 4 never started. Replay reproduces
	// this shape exactly, including the started item.
	if output.SuccessCount != 2 {
		t.Errorf("successCount = %d, want 2", output.SuccessCount)
	}
	if output.StartedCount != 1 {
		t.Errorf("startedCount = %d, want 1", output.StartedCount)
	}
	if output.TotalCount != 3 {
		t.Errorf("totalCount = %d, want 3", output.TotalCount)
	}
	if output.CompletionReason != "MIN_SUCCESSFUL_REACHED" {
		t.Errorf("completionReason = %q, want MIN_SUCCESSFUL_REACHED", output.CompletionReason)
	}
	if want := []int{0, 1, 2}; !reflect.DeepEqual(output.ItemIndexes, want) {
		t.Errorf("itemIndexes = %v, want %v", output.ItemIndexes, want)
	}
}

func TestHandler(t *testing.T) {
	// Default payload: two 150 KiB successes exceed the checkpoint limit,
	// so the map is checkpointed as a record with the custom summary and
	// rebuilt from the item checkpoints on the second invocation.
	result := runAcrossSuspension(t, Event{})
	assertOutput(t, result)

	op := result.Operation("summarized-map")
	if op == nil || op.ContextDetails == nil {
		t.Fatal("summarized-map context operation not found")
	}
	if !op.ContextDetails.ReplayChildren {
		t.Fatal("summarized-map is not in ReplayChildren mode; the aggregate did not exceed the limit")
	}
	var record struct {
		TotalCount int    `json:"totalCount"`
		Summary    string `json:"summary"`
	}
	if err := json.Unmarshal([]byte(op.ContextDetails.Result), &record); err != nil {
		t.Fatalf("checkpointed payload is not the SDK record: %v", err)
	}
	if record.TotalCount != 3 {
		t.Errorf("record totalCount = %d, want 3 (the live started set)", record.TotalCount)
	}
	if !strings.HasPrefix(record.Summary, customSummaryPrefix) {
		t.Errorf("record summary = %q, want the custom summary starting with %q", record.Summary, customSummaryPrefix)
	}
}

func TestHandlerSmallPayload(t *testing.T) {
	// A small payload stays within one checkpoint: the full result is
	// stored, no summary is produced, and replay returns it from the
	// checkpoint.
	result := runAcrossSuspension(t, Event{ItemPayloadSize: 16})
	assertOutput(t, result)

	op := result.Operation("summarized-map")
	if op == nil || op.ContextDetails == nil {
		t.Fatal("summarized-map context operation not found")
	}
	if op.ContextDetails.ReplayChildren {
		t.Error("small result should not use ReplayChildren mode")
	}
	if strings.Contains(op.ContextDetails.Result, customSummaryPrefix) {
		t.Errorf("small result payload carries the summary: %s", op.ContextDetails.Result)
	}
}
