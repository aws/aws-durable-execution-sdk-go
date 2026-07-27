// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable

import (
	"encoding/json"
	"strings"
	"testing"
)

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

// A nested DAG's result is a *DagResult whose fields are all unexported, so
// encoding/json marshals it to "{}" and silently drops every inner task. Cloud
// validation of 10-17 caught this: Go's outer envelope embedded "result":{}, the
// outer stayed inline because it looked tiny, and the inner detail was gone after
// replay. The embed must carry the inner envelope, and the decode must rebuild
// from it rather than unmarshalling into unexported fields.
func TestDagNestedResult_EmbedsAndDecodesRecursively(t *testing.T) {
	inner := newDagResult(
		[]TaskExecution{
			{Name: "p1", Status: StatusSucceeded, result: "aaa", kind: dagKindPlain},
			{Name: "p2", Status: StatusSucceeded, result: "bbb", kind: dagKindPlain},
		},
		AllCompleted,
	)
	outer := newDagResult(
		[]TaskExecution{
			{Name: "inner", Status: StatusSucceeded, result: inner, kind: dagKindDag},
		},
		AllCompleted,
	)

	payload, replayChildren, err := dagEnvelopePayload(outer)
	if err != nil {
		t.Fatalf("dagEnvelopePayload: %v", err)
	}
	if replayChildren {
		t.Fatalf("a small nested DAG should stay inline, got ReplayChildren")
	}
	// The inner tasks must appear in the outer payload; "{}" means they were lost.
	for _, want := range []string{`"p1"`, `"p2"`, `"aaa"`, `"bbb"`, `"resultKind":"dag"`} {
		if !strings.Contains(payload, want) {
			t.Fatalf("outer payload missing %s\npayload: %s", want, payload)
		}
	}

	// Round-trip: the decoded nested result must carry the inner task detail.
	var env dagEnvelope
	if err := json.Unmarshal([]byte(payload), &env); err != nil {
		t.Fatalf("unmarshal outer: %v", err)
	}
	restoredOuter := dagEnvelopeToResult(&env)
	restoredInner, err := ResultByName[DagResult](restoredOuter, "inner")
	if err != nil {
		t.Fatalf("ResultByName(inner): %v", err)
	}
	if got := restoredInner.CompletionReason(); got != AllCompleted {
		t.Fatalf("inner reason = %q, want %q", got, AllCompleted)
	}
	if got := restoredInner.SucceededCount(); got != 2 {
		t.Fatalf("inner succeeded = %d, want 2", got)
	}
	v, err := ResultByName[string](&restoredInner, "p1")
	if err != nil {
		t.Fatalf("inner p1: %v", err)
	}
	if v != "aaa" {
		t.Fatalf("inner p1 = %q, want \"aaa\"", v)
	}
}
