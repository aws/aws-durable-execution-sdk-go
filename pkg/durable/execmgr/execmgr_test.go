package execmgr

import (
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// TestWaitForOperation_SuspendsWhenNoOtherActiveGoroutine verifies the
// core suspension trigger: a goroutine that deregisters itself while
// waiting on a not-yet-completed operation, with no other active
// goroutine, causes Suspended() to close.
func TestWaitForOperation_SuspendsWhenNoOtherActiveGoroutine(t *testing.T) {
	m := New(nil)
	m.Register() // the "handler goroutine"

	done := make(chan struct{})
	go func() {
		_, ok := m.WaitForOperation("1")
		if ok {
			t.Error("expected WaitForOperation to report suspension (ok=false), got a result instead")
		}
		close(done)
	}()

	select {
	case <-m.Suspended():
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for suspension")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for WaitForOperation to return after suspension")
	}
}

// TestWaitForOperation_WakesOnCompletion verifies that a goroutine blocked
// in WaitForOperation resumes (without suspension) once PutOperation
// records a terminal status for the awaited operation, as long as another
// goroutine remains active in the meantime.
func TestWaitForOperation_WakesOnCompletion(t *testing.T) {
	m := New(nil)
	m.Register() // "handler" goroutine - stays active for the whole test
	m.Register() // "step" goroutine - simulates a step still running elsewhere

	resultCh := make(chan bool, 1)
	go func() {
		_, ok := m.WaitForOperation("1")
		resultCh <- ok
	}()

	// Give the waiting goroutine a moment to register its wait and
	// deregister, without racing PutOperation ahead of it. This is a
	// best-effort scheduling nudge, not a correctness dependency: even if
	// PutOperation ran first, WaitForOperation's initial terminal-status
	// check under the same mutex would catch it.
	time.Sleep(50 * time.Millisecond)

	m.PutOperation("1", types.Operation{ID: "1", Status: types.OperationStatusSucceeded})

	select {
	case ok := <-resultCh:
		if !ok {
			t.Error("expected WaitForOperation to return ok=true after completion, got suspension")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for WaitForOperation to wake")
	}

	select {
	case <-m.Suspended():
		t.Error("did not expect suspension: the step goroutine was still active throughout")
	default:
		// expected: not suspended
	}
}

// TestWaitForOperation_FastPath verifies that if the operation is already
// terminal when WaitForOperation is called, it returns immediately without
// touching the active-goroutine count (mirrors the Java SDK's "fast step"
// shortcut, see docs/checkpoint-replay-design.md §4).
func TestWaitForOperation_FastPath(t *testing.T) {
	m := New([]types.Operation{{ID: "1", Status: types.OperationStatusSucceeded}})
	m.Register()

	op, ok := m.WaitForOperation("1")
	if !ok {
		t.Fatal("expected fast-path success")
	}
	if op.Status != types.OperationStatusSucceeded {
		t.Fatalf("expected Succeeded, got %v", op.Status)
	}

	select {
	case <-m.Suspended():
		t.Error("fast path must not trigger suspension")
	default:
	}
}
