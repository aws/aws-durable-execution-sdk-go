// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// runImport runs the handler to completion and checks the invocation
// count and the rows the handler received.
func runImport(t *testing.T, in Input, wantRows int) *durabletest.TestResult {
	t.Helper()
	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete(in)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s (%v)", result.Status, result.Error)
	}
	// The wait suspends the first invocation; the second replays the
	// import child and computes the result from the rows it returns.
	if got := len(result.Invocations); got != 2 {
		t.Fatalf("expected 2 invocations, got %d", got)
	}

	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if want := (Output{RowCount: wantRows, FirstID: 0, LastID: wantRows - 1}); out != want {
		t.Errorf("result = %+v, want %+v", out, want)
	}
	return result
}

func TestHandler(t *testing.T) {
	result := runImport(t, Input{}, defaultChunks*rowsPerChunk)

	// The rows exceed the checkpoint limit, so the child is in
	// ReplayChildren mode and its payload is the summary, not the rows.
	op := result.Operation("import-rows")
	if op == nil || op.ContextDetails == nil {
		t.Fatal("import-rows context operation not found")
	}
	if !op.ContextDetails.ReplayChildren {
		t.Fatal("import-rows is not in ReplayChildren mode; the rows did not exceed the limit")
	}
	if want := "4000 rows imported"; op.ContextDetails.Result != want {
		t.Errorf("checkpointed payload = %q, want summary %q", op.ContextDetails.Result, want)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

func TestHandlerWithinLimit(t *testing.T) {
	result := runImport(t, Input{Chunks: 1}, rowsPerChunk)

	// One chunk fits the checkpoint: the full rows are stored and the
	// summary function does not run.
	op := result.Operation("import-rows")
	if op == nil || op.ContextDetails == nil {
		t.Fatal("import-rows context operation not found")
	}
	if op.ContextDetails.ReplayChildren {
		t.Error("a result within the limit should not use ReplayChildren mode")
	}
	var rows []Row
	if err := json.Unmarshal([]byte(op.ContextDetails.Result), &rows); err != nil {
		t.Fatalf("checkpointed payload is not the rows: %v", err)
	}
	if len(rows) != rowsPerChunk {
		t.Errorf("checkpointed %d rows, want %d", len(rows), rowsPerChunk)
	}

	// One chunk runs one read step instead of four.
	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.within-limit.golden")
}
