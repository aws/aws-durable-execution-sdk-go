// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func assertOutput(t *testing.T, result *durabletest.TestResult, wantLength int) {
	t.Helper()
	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output.TotalCount != 3 {
		t.Errorf("totalCount = %d, want 3", output.TotalCount)
	}
	if output.SuccessCount != 3 {
		t.Errorf("successCount = %d, want 3", output.SuccessCount)
	}
	if want := []int{wantLength, wantLength, wantLength}; !reflect.DeepEqual(output.ResultLengths, want) {
		t.Errorf("resultLengths = %v, want %v", output.ResultLengths, want)
	}
}

func TestHandler(t *testing.T) {
	// Default payload: the serialized result exceeds the checkpoint
	// limit, so the batch is checkpointed as a record carrying the custom
	// summary. The full results still reach the caller.
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, Event{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	assertOutput(t, result, defaultBranchPayloadSize)

	op := result.Operation("parallel-large")
	if op == nil || op.ContextDetails == nil {
		t.Fatal("parallel-large context operation not found")
	}
	if !op.ContextDetails.ReplayChildren {
		t.Fatal("parallel-large is not in ReplayChildren mode; the result did not exceed the limit")
	}
	var record struct {
		TotalCount int    `json:"totalCount"`
		Summary    string `json:"summary"`
	}
	if err := json.Unmarshal([]byte(op.ContextDetails.Result), &record); err != nil {
		t.Fatalf("checkpointed payload is not the SDK record: %v", err)
	}
	if record.TotalCount != 3 {
		t.Errorf("record totalCount = %d, want 3", record.TotalCount)
	}
	var summary Summary
	if err := json.Unmarshal([]byte(record.Summary), &summary); err != nil {
		t.Fatalf("record summary %q is not the custom JSON: %v", record.Summary, err)
	}
	if want := (Summary{Marker: customSummaryMarker, TotalCount: 3, SuccessCount: 3}); summary != want {
		t.Errorf("summary = %+v, want %+v", summary, want)
	}
}

func TestHandlerSmallPayload(t *testing.T) {
	// A small payload stays within one checkpoint: the full result is
	// stored and no summary is produced. This case also records the
	// event signature, which a large-payload run would bloat.
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, Event{BranchPayloadSize: 10})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	assertOutput(t, result, 10)

	op := result.Operation("parallel-large")
	if op == nil || op.ContextDetails == nil {
		t.Fatal("parallel-large context operation not found")
	}
	if op.ContextDetails.ReplayChildren {
		t.Error("small result should not use ReplayChildren mode")
	}

	// Branches run concurrently, so operation order varies between runs.
	durabletest.AssertGoldenSignatureUnordered(t, result, filepath.Join("testdata", "signature.golden"))
}
