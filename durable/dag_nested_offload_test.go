// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable

import "testing"

// TestDagEnvelopeToResult_TasklessPreservesAggregate is the nested-offload
// contract rule-1 guard for Go: restoring a *DagResult from an OFFLOADED
// (tasks-absent) envelope that reports failures MUST preserve totalCount, the
// three status counts, and completionReason from that envelope, and MUST NOT
// fabricate an ALL_COMPLETED / zeroed success. The per-task map is
// legitimately empty (the detail lives in the retained child operations), but
// a caller must never be told a DAG succeeded when the checkpoint says it did
// not.
func TestDagEnvelopeToResult_TasklessPreservesAggregate(t *testing.T) {
	env := &dagEnvelope{
		Type:             dagEnvelopeType,
		TotalCount:       5,
		SuccessCount:     2,
		FailureCount:     2,
		SkippedCount:     1,
		CompletionReason: string(CompletedWithFailures),
		StartedTaskNames: []string{},
		// Tasks intentionally nil: the offloaded shape.
	}

	r := dagEnvelopeToResult(env)

	if r.CompletionReason() != CompletedWithFailures {
		t.Fatalf("completionReason = %q, want %q (must not fabricate)", r.CompletionReason(), CompletedWithFailures)
	}
	if r.CompletionReason() == AllCompleted {
		t.Fatal("restore fabricated ALL_COMPLETED from a failing offloaded envelope")
	}
	if got := r.TotalCount(); got != 5 {
		t.Fatalf("TotalCount = %d, want 5", got)
	}
	if got := r.SucceededCount(); got != 2 {
		t.Fatalf("SucceededCount = %d, want 2 (discarded envelope count)", got)
	}
	if got := r.FailureCount(); got != 2 {
		t.Fatalf("FailureCount = %d, want 2 (discarded envelope count)", got)
	}
	if got := r.SkippedCount(); got != 1 {
		t.Fatalf("SkippedCount = %d, want 1 (discarded envelope count)", got)
	}
	// The per-task map is legitimately empty on the offloaded shape.
	if len(r.Results()) != 0 {
		t.Fatalf("expected empty per-task map on offloaded restore, got %d", len(r.Results()))
	}
	// A caller must never be told success when the checkpoint says otherwise.
	if err := r.ThrowIfError(); err == nil {
		t.Fatal("ThrowIfError returned nil despite the envelope reporting 2 failures")
	}
}

// TestDagEnvelopeToResult_InlineUnaffected proves the rule-1 fix does not
// perturb the normal inline path: when `tasks` is present the counts are
// still derived from the per-task executions, not from any stored aggregate.
func TestDagEnvelopeToResult_InlineUnaffected(t *testing.T) {
	succeeded := dagResultKind(dagKindPlain)
	tasks := []dagEnvelopeTask{
		{Name: "a", Status: StatusSucceeded, ResultKind: &succeeded, Result: []byte(`1`)},
		{Name: "b", Status: StatusFailed, Error: &dagErrorObject{ErrorType: "StepError", ErrorMessage: "boom"}},
	}
	env := &dagEnvelope{
		Type:             dagEnvelopeType,
		TotalCount:       2,
		SuccessCount:     1,
		FailureCount:     1,
		CompletionReason: string(CompletedWithFailures),
		StartedTaskNames: []string{},
		Tasks:            &tasks,
	}

	r := dagEnvelopeToResult(env)
	if r.aggregateOnly {
		t.Fatal("inline restore must not be marked aggregateOnly")
	}
	if r.SucceededCount() != 1 || r.FailureCount() != 1 || r.TotalCount() != 2 {
		t.Fatalf("inline counts wrong: s=%d f=%d t=%d", r.SucceededCount(), r.FailureCount(), r.TotalCount())
	}
	if len(r.Results()) != 2 {
		t.Fatalf("expected 2 per-task executions inline, got %d", len(r.Results()))
	}
}
