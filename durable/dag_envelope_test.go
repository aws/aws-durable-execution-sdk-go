// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// canonical3TaskResult builds the contract's reference three-task DAG result:
// one SUCCEEDED, one FAILED, one SKIPPED. Timestamps are fixed so the
// serialized envelope is byte-stable for cross-SDK diffing.
func canonical3TaskResult() *DagResult {
	started := time.Date(2026, 7, 26, 3, 19, 1, 884*int(time.Millisecond), time.UTC)
	completed := time.Date(2026, 7, 26, 3, 19, 1, 885*int(time.Millisecond), time.UTC)

	stepErr := &StepError{Name: "charge-card", Attempts: 1, Err: errors.New("card declined")}
	taskErr := &DagTaskFailedError{Name: "charge-card", TaskID: "scope-DAG_NODE_T_charge-card", Err: stepErr}

	execs := []TaskExecution{
		{
			Name:        "load-order",
			Status:      StatusSucceeded,
			kind:        dagKindPlain,
			result:      map[string]any{"orderId": "A-1001", "amount": 4200},
			StartedAt:   started,
			CompletedAt: completed,
		},
		{
			Name:        "charge-card",
			Status:      StatusFailed,
			kind:        dagKindPlain,
			Err:         taskErr,
			StartedAt:   started,
			CompletedAt: completed,
		},
		{
			Name:        "reserve-stock",
			Status:      StatusSkipped,
			SkipReason:  SkipTriggerRule,
			kind:        dagKindPlain,
			StartedAt:   completed,
			CompletedAt: completed,
		},
	}
	r := newDagResult(execs, CompletedWithFailures)
	r.total = 3
	return r
}

// TestDagEnvelope_InlineExact pins the exact inline (tasks present) envelope
// bytes for the reference three-task DAG. It also logs the payload so the
// orchestrator can diff the four SDKs.
func TestDagEnvelope_InlineExact(t *testing.T) {
	r := canonical3TaskResult()
	payload, replayChildren, err := dagEnvelopePayload(r)
	if err != nil {
		t.Fatalf("dagEnvelopePayload: %v", err)
	}
	if replayChildren {
		t.Fatal("a small three-task DAG must checkpoint inline (no ReplayChildren)")
	}

	prettyBytes, _ := json.MarshalIndent(json.RawMessage(payload), "", "  ")
	t.Logf("INLINE ENVELOPE (three-task: 1 success, 1 failure, 1 skip):\n%s", string(prettyBytes))

	want := `{"type":"DagResult","totalCount":3,"successCount":1,"failureCount":1,"skippedCount":1,` +
		`"completionReason":"COMPLETED_WITH_FAILURES","startedTaskNames":[],"failedTaskNames":["charge-card"],` +
		`"tasks":[` +
		`{"name":"load-order","status":"SUCCEEDED","skipReason":null,"resultKind":"plain",` +
		`"result":{"amount":4200,"orderId":"A-1001"},"error":null,` +
		`"startedAt":"2026-07-26T03:19:01.884Z","completedAt":"2026-07-26T03:19:01.885Z"},` +
		`{"name":"charge-card","status":"FAILED","skipReason":null,"resultKind":null,"result":null,` +
		`"error":{"ErrorType":"StepError","ErrorMessage":"durable: step \"charge-card\" failed after 1 attempts: card declined","StackTrace":null},` +
		`"startedAt":"2026-07-26T03:19:01.884Z","completedAt":"2026-07-26T03:19:01.885Z"},` +
		`{"name":"reserve-stock","status":"SKIPPED","skipReason":"TRIGGER_RULE","resultKind":null,"result":null,"error":null,` +
		`"startedAt":"2026-07-26T03:19:01.885Z","completedAt":"2026-07-26T03:19:01.885Z"}` +
		`]}`
	if payload != want {
		t.Fatalf("inline envelope mismatch:\n got: %s\nwant: %s", payload, want)
	}
}

// TestDagEnvelope_OffloadedShape pins the aggregate-only (tasks dropped)
// envelope written on the offloaded path, and logs it for the diff.
func TestDagEnvelope_OffloadedShape(t *testing.T) {
	r := canonical3TaskResult()
	env, err := r.buildEnvelope(false, true)
	if err != nil {
		t.Fatalf("buildEnvelope: %v", err)
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	t.Logf("OFFLOADED ENVELOPE (tasks dropped, ReplayChildren=true):\n%s", string(b))

	want := `{"type":"DagResult","totalCount":3,"successCount":1,"failureCount":1,"skippedCount":1,` +
		`"completionReason":"COMPLETED_WITH_FAILURES","startedTaskNames":[],"failedTaskNames":["charge-card"]}`
	if string(b) != want {
		t.Fatalf("offloaded envelope mismatch:\n got: %s\nwant: %s", string(b), want)
	}
	// `tasks` must be ABSENT (omitted), not null: absence is the offload signal.
	if strings.Contains(string(b), "tasks") {
		t.Fatal("offloaded envelope must omit the tasks field entirely")
	}
}

// TestDagEnvelope_LastResortDropsFailedNames pins the final degradation step:
// failedTaskNames drops to null while counts, completionReason and
// startedTaskNames survive.
func TestDagEnvelope_LastResortDropsFailedNames(t *testing.T) {
	r := canonical3TaskResult()
	env, err := r.buildEnvelope(false, false)
	if err != nil {
		t.Fatalf("buildEnvelope: %v", err)
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"type":"DagResult","totalCount":3,"successCount":1,"failureCount":1,"skippedCount":1,` +
		`"completionReason":"COMPLETED_WITH_FAILURES","startedTaskNames":[],"failedTaskNames":null}`
	if string(b) != want {
		t.Fatalf("minimal envelope mismatch:\n got: %s\nwant: %s", string(b), want)
	}
}

// TestDagEnvelope_EmptyArraysNotNull verifies startedTaskNames and
// failedTaskNames render as [] (not null) when empty and present, while
// per-task null fields stay explicit null.
func TestDagEnvelope_EmptyArraysNotNull(t *testing.T) {
	r := newDagResult([]TaskExecution{
		{Name: "only", Status: StatusSucceeded, kind: dagKindPlain, result: 1},
	}, AllCompleted)
	r.total = 1
	env, err := r.buildEnvelope(true, true)
	if err != nil {
		t.Fatalf("buildEnvelope: %v", err)
	}
	b, _ := json.Marshal(env)
	s := string(b)
	if !strings.Contains(s, `"startedTaskNames":[]`) {
		t.Fatalf("startedTaskNames must be [] when empty, got: %s", s)
	}
	if !strings.Contains(s, `"failedTaskNames":[]`) {
		t.Fatalf("failedTaskNames must be [] when empty and present, got: %s", s)
	}
	// per-task unset skipReason/error must be explicit null.
	if !strings.Contains(s, `"skipReason":null`) || !strings.Contains(s, `"error":null`) {
		t.Fatalf("per-task absent values must be explicit null, got: %s", s)
	}
}

// TestDagEnvelope_UnknownFieldTolerated is the rule-4 guard: the format has no
// schemaVersion and evolves additive-only, so a reader MUST ignore unknown
// fields (top-level and per-task) rather than failing.
func TestDagEnvelope_UnknownFieldTolerated(t *testing.T) {
	withExtras := `{"type":"DagResult","totalCount":3,"successCount":1,"failureCount":1,"skippedCount":1,` +
		`"completionReason":"COMPLETED_WITH_FAILURES","startedTaskNames":[],"failedTaskNames":["charge-card"],` +
		`"someFutureAggregate":{"nested":true},"anotherNewField":42,` +
		`"tasks":[` +
		`{"name":"load-order","status":"SUCCEEDED","skipReason":null,"resultKind":"plain","result":7,"error":null,` +
		`"startedAt":null,"completedAt":null,"perTaskFutureField":"ignore-me"}` +
		`]}`
	env, err := unmarshalDagEnvelope([]byte(withExtras))
	if err != nil {
		t.Fatalf("unknown extra fields must not error, got: %v", err)
	}
	if env.Type != "DagResult" || env.TotalCount != 3 || env.SuccessCount != 1 {
		t.Fatalf("known fields not preserved alongside unknown ones: %+v", env)
	}
	if env.Tasks == nil || len(*env.Tasks) != 1 || (*env.Tasks)[0].Name != "load-order" {
		t.Fatalf("task not parsed past the unknown per-task field: %+v", env.Tasks)
	}
	if env.FailedTaskNames == nil || len(*env.FailedTaskNames) != 1 {
		t.Fatalf("failedTaskNames lost: %+v", env.FailedTaskNames)
	}
}

// TestDagEnvelope_RoundTripInline proves the inline envelope round-trips: a
// serialized payload deserializes back into a DagResult whose per-task
// statuses, lazily-typed results, skip reasons and counts are intact.
func TestDagEnvelope_RoundTripInline(t *testing.T) {
	r := canonical3TaskResult()
	payload, _, err := dagEnvelopePayload(r)
	if err != nil {
		t.Fatalf("dagEnvelopePayload: %v", err)
	}
	env, err := unmarshalDagEnvelope([]byte(payload))
	if err != nil {
		t.Fatalf("unmarshalDagEnvelope: %v", err)
	}
	got := dagEnvelopeToResult(env)

	if got.TotalCount() != 3 || got.SucceededCount() != 1 || got.FailureCount() != 1 || got.SkippedCount() != 1 {
		t.Fatalf("counts wrong after round-trip: t=%d s=%d f=%d k=%d",
			got.TotalCount(), got.SucceededCount(), got.FailureCount(), got.SkippedCount())
	}
	if got.CompletionReason() != CompletedWithFailures {
		t.Fatalf("reason=%q want COMPLETED_WITH_FAILURES", got.CompletionReason())
	}
	// Lazily-typed result decodes from the carried raw JSON.
	order, rerr := ResultByName[map[string]any](got, "load-order")
	if rerr != nil {
		t.Fatalf("ResultByName load-order: %v", rerr)
	}
	if order["orderId"] != "A-1001" {
		t.Fatalf("result not preserved: %+v", order)
	}
	// Skip reason preserved.
	if sr := got.Results()["reserve-stock"].SkipReason; sr != SkipTriggerRule {
		t.Fatalf("skip reason=%q want TRIGGER_RULE", sr)
	}
	// Failed task carries a reconstructed error.
	if st, _ := got.Status("charge-card"); st != StatusFailed {
		t.Fatalf("charge-card status=%q want FAILED", st)
	}
	if got.Results()["charge-card"].Err == nil {
		t.Fatal("failed task must carry a reconstructed error after round-trip")
	}
}
