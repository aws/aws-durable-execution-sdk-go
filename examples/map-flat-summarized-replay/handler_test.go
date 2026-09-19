// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// runAcrossSuspension drives the handler through its suspension so the
// second invocation replays the map, asserting both invocations happened.
// Without a suspension there is no replay and the test exercises nothing.
func runAcrossSuspension(t *testing.T, event Event) *durabletest.TestResult {
	t.Helper()
	runner := durabletest.NewLocalRunner(handler)

	first := runner.Run(t, event)
	if first.Status != durabletest.Pending {
		t.Fatalf("first invocation status = %s, want PENDING (the wait must suspend)", first.Status)
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

func assertFullRebuild(t *testing.T, result *durabletest.TestResult) {
	t.Helper()
	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	want := make([]int, itemCount)
	for i := range want {
		want[i] = i
	}
	if output.LiveResultCount != itemCount {
		t.Errorf("liveResultCount = %d, want %d", output.LiveResultCount, itemCount)
	}
	if output.ReplayedResultCount != itemCount {
		t.Errorf("replayedResultCount = %d, want %d", output.ReplayedResultCount, itemCount)
	}
	if !reflect.DeepEqual(output.ReplayedItems, want) {
		t.Errorf("replayedItems = %v, want %v", output.ReplayedItems, want)
	}
}

func assertSummarized(t *testing.T, result *durabletest.TestResult, want bool) {
	t.Helper()
	op := result.Operation("resolve-pages")
	if op == nil || op.ContextDetails == nil {
		t.Fatal("resolve-pages context operation not found")
	}
	if op.ContextDetails.ReplayChildren != want {
		t.Errorf("resolve-pages ReplayChildren = %v, want %v", op.ContextDetails.ReplayChildren, want)
	}
}

func TestHandler(t *testing.T) {
	for _, nesting := range []string{"FLAT", "NORMAL"} {
		t.Run(nesting, func(t *testing.T) {
			result := runAcrossSuspension(t, Event{Nesting: nesting})
			assertSummarized(t, result, true)
			assertFullRebuild(t, result)
			// Map items complete in scheduling-dependent order, so the
			// signatures are compared as sets. The default nesting has
			// the shared golden; NORMAL nesting adds a child context per
			// item and has its own.
			if nesting == "FLAT" {
				extest.AssertSignature(t, result, extest.Unordered)
			} else {
				extest.AssertSignatureFile(t, result, extest.Unordered, "testdata/signature.normal.golden")
			}
		})
	}
}

func TestHandlerNoDurableOperation(t *testing.T) {
	// A FLAT item whose body checkpoints nothing leaves no trace beneath
	// the batch; only the batch's record identifies it as finished.
	result := runAcrossSuspension(t, Event{Nesting: "FLAT", NoDurableOperation: true})
	assertSummarized(t, result, true)
	assertFullRebuild(t, result)

	// Without a step per item only the batch and the wait remain.
	extest.AssertSignatureFile(t, result, extest.Unordered, "testdata/signature.no-durable-operation.golden")
}

func TestHandlerSmallPayload(t *testing.T) {
	// A result within one checkpoint is stored whole and replayed by
	// deserialization, never entering the rebuild path.
	result := runAcrossSuspension(t, Event{Nesting: "FLAT", ItemPayloadSize: 16})
	assertSummarized(t, result, false)
	assertFullRebuild(t, result)

	// The payload size changes how the map result is checkpointed, not
	// which operations run, so both sizes share the golden.
	extest.AssertSignature(t, result, extest.Unordered)
}
