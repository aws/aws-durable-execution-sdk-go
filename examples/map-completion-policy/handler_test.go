// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// run runs the handler to completion and returns its output.
func run(t *testing.T, in Input) (*durabletest.TestResult, Output) {
	t.Helper()
	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete(in)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	return result, out
}

// TestHandler: every replica is available. The quorum rule commits after
// the third acknowledgement, and the last two replicas are never written.
func TestHandler(t *testing.T) {
	result, out := run(t, Input{})
	want := Output{
		Committed:    true,
		Reason:       "CUSTOM_COMPLETION_SUCCEEDED",
		Acknowledged: []string{"replica-0", "replica-1", "replica-2"},
		Failed:       []string{},
		NotAttempted: 2,
	}
	if !reflect.DeepEqual(out, want) {
		t.Errorf("output = %+v, want %+v", out, want)
	}
	// The writes run one at a time, so the checkpoint order is exact.
	extest.AssertSignature(t, result, extest.Ordered)
}

// TestHandlerToleratesFailures: two secondaries fail, but the remaining
// replicas still reach the quorum. The custom outcome is authoritative,
// so the batch succeeds with failed items.
func TestHandlerToleratesFailures(t *testing.T) {
	result, out := run(t, Input{Unavailable: []string{"replica-1", "replica-3"}})
	want := Output{
		Committed:    true,
		Reason:       "CUSTOM_COMPLETION_SUCCEEDED",
		Acknowledged: []string{"replica-0", "replica-2", "replica-4"},
		Failed:       []string{"replica-1", "replica-3"},
		NotAttempted: 0,
	}
	if !reflect.DeepEqual(out, want) {
		t.Errorf("output = %+v, want %+v", out, want)
	}
	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.tolerated.golden")
}

// TestHandlerPrimaryFails: the primary fails first, so primaryMustAck
// fails the batch before any secondary is written.
func TestHandlerPrimaryFails(t *testing.T) {
	result, out := run(t, Input{Unavailable: []string{"replica-0"}})
	want := Output{
		Committed:    false,
		Reason:       "CUSTOM_COMPLETION_FAILED",
		Acknowledged: []string{},
		Failed:       []string{"replica-0"},
		NotAttempted: 4,
	}
	if !reflect.DeepEqual(out, want) {
		t.Errorf("output = %+v, want %+v", out, want)
	}
	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.primary.golden")
}

// TestHandlerQuorumUnreachable: after three secondaries fail, one
// acknowledgement and one untried replica cannot make three, so
// quorumUnreachable fails the batch and replica-4 is never written.
func TestHandlerQuorumUnreachable(t *testing.T) {
	result, out := run(t, Input{Unavailable: []string{"replica-1", "replica-2", "replica-3"}})
	want := Output{
		Committed:    false,
		Reason:       "CUSTOM_COMPLETION_FAILED",
		Acknowledged: []string{"replica-0"},
		Failed:       []string{"replica-1", "replica-2", "replica-3"},
		NotAttempted: 1,
	}
	if !reflect.DeepEqual(out, want) {
		t.Errorf("output = %+v, want %+v", out, want)
	}
	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.unreachable.golden")
}

// progress builds a snapshot from the items' statuses, in index order.
func progress(statuses ...durable.BatchItemStatus) durable.BatchProgress {
	p := durable.BatchProgress{TotalCount: len(statuses)}
	for i, s := range statuses {
		p.Items = append(p.Items, durable.BatchItemProgress{Index: i, Status: s})
		switch s {
		case durable.BatchItemSucceeded:
			p.SuccessCount++
			p.CompletedCount++
		case durable.BatchItemFailed:
			p.FailureCount++
			p.CompletedCount++
		}
	}
	return p
}

// TestFirstOfFailedOutcomeWins checks the precedence the handler's
// sequential writes never reach: on a snapshot where both quorum and
// primaryMustAck complete the batch, as concurrent writes could produce,
// the failed outcome wins whatever the order of the rules.
func TestFirstOfFailedOutcomeWins(t *testing.T) {
	policies := map[string]rule{
		"quorum first":  firstOf(quorum(writeQuorum), primaryMustAck),
		"primary first": firstOf(primaryMustAck, quorum(writeQuorum)),
	}
	for name, policy := range policies {
		p := progress(durable.BatchItemFailed, durable.BatchItemSucceeded, durable.BatchItemSucceeded,
			durable.BatchItemSucceeded, durable.BatchItemNotStarted)
		if d := policy(p); !d.Complete() || d.Outcome() != durable.CompletionOutcomeFailed {
			t.Errorf("%s: decision = complete %v, outcome %v; want complete with FAILED", name, d.Complete(), d.Outcome())
		}

		// Without the failed primary the same quorum succeeds.
		p.Items[0].Status = durable.BatchItemSucceeded
		if d := policy(p); !d.Complete() || d.Outcome() != durable.CompletionOutcomeSucceeded {
			t.Errorf("%s: decision = complete %v, outcome %v; want complete with SUCCEEDED", name, d.Complete(), d.Outcome())
		}
	}
}
