// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// TestBeginInvocationSnapshotsAndClearsAtomically checks that a change is
// delivered exactly once: in the invocation payload when it happens before
// beginInvocation, and in the next checkpoint response when it happens
// after.
func TestBeginInvocationSnapshotsAndClearsAtomically(t *testing.T) {
	m := newMemoryClient()
	send(t, m,
		durable.OperationUpdate{
			Id: aws.String("cb-before"), Name: aws.String("cb-before"),
			Type: durable.OperationTypeCallback, Action: durable.OperationActionStart,
		},
		durable.OperationUpdate{
			Id: aws.String("cb-after"), Name: aws.String("cb-after"),
			Type: durable.OperationTypeCallback, Action: durable.OperationActionStart,
		},
	)
	callbackID := func(opID string) string {
		t.Helper()
		m.mu.Lock()
		defer m.mu.Unlock()
		op := m.operations[opID]
		if op == nil || op.CallbackDetails == nil || op.CallbackDetails.CallbackId == nil {
			t.Fatalf("operation %q has no callback ID", opID)
		}
		return *op.CallbackDetails.CallbackId
	}

	// A callback resolved before the invocation begins is in the payload.
	if err := m.completeCallback(callbackID("cb-before"), operationResult{status: statusSucceeded, result: `"x"`}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := m.beginInvocation()
	found := false
	for _, op := range snapshot {
		if op.ID == "cb-before" {
			found = true
			if op.Status != string(durable.OperationStatusSucceeded) {
				t.Fatalf("cb-before status in payload = %s, want SUCCEEDED", op.Status)
			}
		}
	}
	if !found {
		t.Fatal("cb-before missing from the payload snapshot")
	}

	// A callback resolved after the invocation begins is reported by the
	// next checkpoint response.
	if err := m.completeCallback(callbackID("cb-after"), operationResult{status: statusSucceeded, result: `"y"`}); err != nil {
		t.Fatal(err)
	}
	out := send(t, m)
	reported := map[string]durable.OperationStatus{}
	for _, op := range out.NewExecutionState {
		reported[aws.ToString(op.Id)] = op.Status
	}
	if got := reported["cb-after"]; got != durable.OperationStatusSucceeded {
		t.Fatalf("cb-after reported as %q in the poll response, want SUCCEEDED", got)
	}
	if _, ok := reported["cb-before"]; ok {
		t.Fatal("cb-before reported again in the poll response; the payload already carried it")
	}
}

// TestCallbackResolvedAfterPayloadBuiltResumesSameInvocation resolves a
// callback after the runner has built the invocation payload and before the
// handler runs. The payload carries the callback as STARTED. A concurrent
// step keeps the invocation running while the handler awaits the callback.
// So the handler must learn of the completion from a checkpoint response
// and finish in that same invocation.
func TestCallbackResolvedAfterPayloadBuiltResumesSameInvocation(t *testing.T) {
	handler := func(ctx durable.Context, _ string) (string, error) {
		cb, err := durable.CreateCallback[string](ctx, "approval", durable.WithCallbackSerdes(durable.JSONSerdes))
		if err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "gate", time.Second); err != nil {
			return "", err
		}
		long := durable.StepAsync(ctx, "long", func(durable.StepContext) (int, error) {
			time.Sleep(300 * time.Millisecond)
			return 1, nil
		})
		v, err := cb.Result(ctx)
		if err != nil {
			return "", err
		}
		if _, err := long.Result(ctx); err != nil {
			return "", err
		}
		return v, nil
	}
	runner := NewLocalRunner(handler)
	res, err := runner.Run("in")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != Pending {
		t.Fatalf("status after first Run = %s, want PENDING", res.Status)
	}
	cbs := runner.OpenCallbacks()
	if len(cbs) != 1 {
		t.Fatalf("open callbacks = %d, want 1", len(cbs))
	}
	if !runner.CompletePendingTimers() {
		t.Fatal("CompletePendingTimers did not complete the wait")
	}

	resolved := false
	runner.exec.afterBeginInvocation = func() {
		if resolved {
			return
		}
		resolved = true
		if err := runner.SendCallbackSuccess(cbs[0].CallbackID, "yes"); err != nil {
			t.Errorf("SendCallbackSuccess: %v", err)
		}
	}
	before := runner.exec.invocations
	res, err = runner.Run("in")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED in the same invocation (error: %+v)", res.Status, res.Error)
	}
	if n := runner.exec.invocations - before; n != 1 {
		t.Fatalf("invocations = %d, want 1", n)
	}
	got, err := ResultAs[string](res)
	if err != nil {
		t.Fatal(err)
	}
	if got != "yes" {
		t.Fatalf("result = %q, want %q", got, "yes")
	}
}

// TestStartedContextIsNotPending checks that a STARTED context alone does
// not count as a pending operation, while a STARTED wait does.
func TestStartedContextIsNotPending(t *testing.T) {
	m := newMemoryClient()
	send(t, m, durable.OperationUpdate{
		Id: aws.String("ctx"), Name: aws.String("ctx"),
		Type: durable.OperationTypeContext, Action: durable.OperationActionStart,
	})
	if m.hasPendingOperation() {
		t.Fatal("a STARTED context alone counts as pending; only the handler's checkpoints change it")
	}
	send(t, m, waitStart("w", 5))
	if !m.hasPendingOperation() {
		t.Fatal("a STARTED wait does not count as pending")
	}
}

// TestUpdatedOperationIdsSinceLastSuccessfulInvocation checks the updated
// list the client computes for each invocation payload. An operation is
// listed when its record changed since the last successful invocation
// ended. A failed invocation does not move that base, so the next payload
// lists again what the failed invocation received.
func TestUpdatedOperationIdsSinceLastSuccessfulInvocation(t *testing.T) {
	m := newMemoryClient()
	start := func(id string) {
		send(t, m, durable.OperationUpdate{
			Id: aws.String(id), Name: aws.String(id),
			Type: durable.OperationTypeCallback, Action: durable.OperationActionStart,
		})
	}
	resolve := func(id string) {
		t.Helper()
		m.mu.Lock()
		cbID := *m.operations[id].CallbackDetails.CallbackId
		m.mu.Unlock()
		if err := m.completeCallback(cbID, operationResult{status: statusSucceeded, result: `"x"`}); err != nil {
			t.Fatal(err)
		}
	}
	updated := func() []string {
		_, _, ids := m.beginInvocationWithUpdates()
		return ids
	}
	equal := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	// Invocation 1 starts both callbacks and succeeds.
	if got := updated(); len(got) != 0 {
		t.Fatalf("invocation 1 updated = %v, want none", got)
	}
	start("a")
	start("b")
	m.endInvocation(true)

	// a resolves while suspended. Invocation 2 lists it, then fails.
	resolve("a")
	if got := updated(); !equal(got, []string{"a"}) {
		t.Fatalf("invocation 2 updated = %v, want [a]", got)
	}
	m.endInvocation(false)

	// b resolves. Invocation 3 lists a again and b, and succeeds.
	resolve("b")
	if got := updated(); !equal(got, []string{"a", "b"}) {
		t.Fatalf("invocation 3 updated = %v, want [a b]", got)
	}
	m.endInvocation(true)

	// Nothing changed since invocation 3.
	if got := updated(); len(got) != 0 {
		t.Fatalf("invocation 4 updated = %v, want none", got)
	}
}
