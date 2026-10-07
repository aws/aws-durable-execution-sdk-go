// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// waitStart builds a WAIT START update of the given number of seconds.
func waitStart(id string, seconds int32) durable.OperationUpdate {
	return durable.OperationUpdate{
		Id:          aws.String(id),
		Name:        aws.String(id),
		Type:        durable.OperationTypeWait,
		Action:      durable.OperationActionStart,
		WaitOptions: &durable.WaitOptions{WaitSeconds: aws.Int32(seconds)},
	}
}

// send applies updates in one request and returns the response.
func send(t *testing.T, m *memoryClient, updates ...durable.OperationUpdate) durable.CheckpointOutput {
	t.Helper()
	out, err := m.Checkpoint(context.Background(), durable.CheckpointInput{
		CheckpointToken: m.currentToken(),
		Updates:         updates,
	})
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	return out
}

// statusOf returns the stored status of operation id.
func statusOf(t *testing.T, m *memoryClient, id string) durable.OperationStatus {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	op := m.operations[id]
	if op == nil {
		t.Fatalf("operation %q not stored", id)
	}
	return op.Status
}

// reported reports whether the response carries operation id.
func reported(out durable.CheckpointOutput, id string) bool {
	for _, op := range out.NewExecutionState {
		if aws.ToString(op.Id) == id {
			return true
		}
	}
	return false
}

// TestVirtualClockIgnoresWallClock asserts that the client's clock is fixed
// at the execution's start and that wall-clock time alone does not move it.
func TestVirtualClockIgnoresWallClock(t *testing.T) {
	m := newMemoryClient()
	m.recordExecutionStarted("{}")
	start := m.now()
	time.Sleep(20 * time.Millisecond)
	if got := m.now(); !got.Equal(start) {
		t.Fatalf("clock moved by %v with no virtual-time change", got.Sub(start))
	}

	// A request that changes no timer leaves the clock where it is.
	send(t, m, stepUpdate("s", durable.OperationActionStart, nil))
	time.Sleep(20 * time.Millisecond)
	send(t, m, stepUpdate("s", durable.OperationActionSucceed, aws.String(`"ok"`)))
	if got := m.now(); !got.Equal(start) {
		t.Fatalf("clock moved by %v with no pending timer", got.Sub(start))
	}

	// The wait's scheduled end is the virtual clock plus its seconds.
	send(t, m, waitStart("w", 30))
	m.mu.Lock()
	end := *m.operations["w"].WaitDetails.ScheduledEndTimestamp
	m.mu.Unlock()
	if want := start.Add(30 * time.Second); !end.Equal(want) {
		t.Fatalf("scheduled end = %v, want %v", end, want)
	}

	// A poll advances the clock to the wait's end, and no further.
	send(t, m)
	if got := m.now(); !got.Equal(end) {
		t.Fatalf("clock after poll = %v, want %v", got, end)
	}
}

// TestVirtualClockWaitSurvivesStepStart asserts that a wait stays STARTED
// through the START checkpoint of an unrelated step, so the step body runs
// before the wait completes, and that the wait is reported SUCCEEDED in the
// response to the checkpoint that reports the step finished.
func TestVirtualClockWaitSurvivesStepStart(t *testing.T) {
	m := newMemoryClient()
	m.recordExecutionStarted("{}")
	send(t, m, waitStart("w", 1))

	out := send(t, m, stepUpdate("s", durable.OperationActionStart, nil))
	if reported(out, "w") {
		t.Fatal("STEP START response reported the wait")
	}
	if got := statusOf(t, m, "w"); got != durable.OperationStatusStarted {
		t.Fatalf("wait status after STEP START = %s, want STARTED", got)
	}

	out = send(t, m, stepUpdate("s", durable.OperationActionSucceed, aws.String(`"ok"`)))
	if !reported(out, "w") {
		t.Fatal("STEP SUCCEED response did not report the wait")
	}
	if got := statusOf(t, m, "w"); got != durable.OperationStatusSucceeded {
		t.Fatalf("wait status after STEP SUCCEED = %s, want SUCCEEDED", got)
	}
}

// TestVirtualClockTwoWaitsEarliestFirst asserts that with two waits of
// different durations pending, an advance completes only the wait with the
// earliest scheduled end, whichever started first, and a later advance
// completes the other.
func TestVirtualClockTwoWaitsEarliestFirst(t *testing.T) {
	m := newMemoryClient()
	m.recordExecutionStarted("{}")
	start := m.now()
	send(t, m, waitStart("long", 60))
	if out := send(t, m, waitStart("short", 5)); reported(out, "long") {
		t.Fatal("the short wait's START response reported the long wait")
	}
	if got := statusOf(t, m, "long"); got != durable.OperationStatusStarted {
		t.Fatalf("long wait status after short START = %s, want STARTED", got)
	}

	out := send(t, m)
	if !reported(out, "short") || reported(out, "long") {
		t.Fatalf("first poll reported short=%v long=%v, want short only", reported(out, "short"), reported(out, "long"))
	}
	if got := statusOf(t, m, "long"); got != durable.OperationStatusStarted {
		t.Fatalf("long wait status = %s, want STARTED", got)
	}
	if got, want := m.now(), start.Add(5*time.Second); !got.Equal(want) {
		t.Fatalf("clock after first poll = %v, want %v", got, want)
	}

	out = send(t, m, stepUpdate("s", durable.OperationActionSucceed, aws.String(`"ok"`)))
	if !reported(out, "long") {
		t.Fatal("second advance did not report the long wait")
	}
	if got, want := m.now(), start.Add(60*time.Second); !got.Equal(want) {
		t.Fatalf("clock after second advance = %v, want %v", got, want)
	}
}
