package durable

import (
	"context"
	"errors"
	"testing"
)

func newTestContext(t *testing.T, ops []*operation) *execContext {
	t.Helper()
	return newExecContext(
		context.Background(),
		"arn:aws:lambda:us-west-2:123456789012:function:fn:1/durable-execution/test",
		invocationInfo{requestID: "req-1"},
		nopLogger{},
		newExecutionState(ops),
	)
}

// execOp is the always-present execution operation record. Its wire ID is
// not derived from a positional ID the minter produces.
func execOp() *operation {
	return &operation{id: hashID("execution"), status: statusStarted}
}

// checkpointed returns an operation record keyed the way the wire delivers
// it: by the hash of the positional ID.
func checkpointed(positionalID string, status operationStatus) *operation {
	return &operation{id: hashID(positionalID), status: status}
}

type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

func TestNewExecContextMode(t *testing.T) {
	tests := []struct {
		name          string
		ops           []*operation
		wantReplaying bool
	}{
		{
			name:          "empty state starts in execution mode",
			wantReplaying: false,
		},
		{
			name:          "only the execution operation starts in execution mode",
			ops:           []*operation{execOp()},
			wantReplaying: false,
		},
		{
			name:          "checkpointed operations beyond the execution operation start replay",
			ops:           []*operation{execOp(), checkpointed("1", statusSucceeded)},
			wantReplaying: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestContext(t, tt.ops)
			if got := c.IsReplaying(); got != tt.wantReplaying {
				t.Errorf("IsReplaying() = %v, want %v", got, tt.wantReplaying)
			}
		})
	}
}

func TestClaimOperationReplayTransition(t *testing.T) {
	// Two checkpointed operations, then live execution.
	c := newTestContext(t, []*operation{
		execOp(),
		checkpointed("1", statusSucceeded),
		checkpointed("2", statusSucceeded),
	})

	for want := 1; want <= 2; want++ {
		if !c.IsReplaying() {
			t.Fatalf("IsReplaying() before claim %d = false, want true", want)
		}
		id, err := c.claimOperation()
		if err != nil {
			t.Fatalf("claimOperation() %d: %v", want, err)
		}
		if wantID := (&opIDs{counter: want - 1}).peek(); id != wantID {
			t.Fatalf("claimOperation() %d = %q, want %q", want, id, wantID)
		}
	}

	// Operation 3 has no checkpoint: mode must flip to live execution at
	// claim time.
	if _, err := c.claimOperation(); err != nil {
		t.Fatalf("claimOperation() 3: %v", err)
	}
	if c.IsReplaying() {
		t.Error("IsReplaying() after claiming an uncheckpointed operation = true, want false")
	}
}

func TestRefreshReplayModeVirtualContextProbe(t *testing.T) {
	// The pending operation "1" is not checkpointed itself, but its first
	// child "1-1" is: a virtual child context. Replay must continue.
	c := newTestContext(t, []*operation{
		execOp(),
		checkpointed("1-1", statusSucceeded),
	})

	if _, err := c.claimOperation(); err != nil {
		t.Fatalf("claimOperation(): %v", err)
	}
	if !c.IsReplaying() {
		t.Error("IsReplaying() with checkpointed virtual-context child = false, want true")
	}
}

func TestClaimOperationForeignGoroutine(t *testing.T) {
	c := newTestContext(t, nil)

	errCh := make(chan error, 1)
	go func() {
		_, err := c.claimOperation()
		errCh <- err
	}()
	err := <-errCh
	if err == nil {
		// Goroutine ownership checking is disabled (no durablecheck tag).
		t.Skip("goroutine ownership checks disabled without -tags=durablecheck")
	}
	if !errors.Is(err, ErrWrongGoroutine) {
		t.Errorf("claimOperation() from foreign goroutine = %v, want ErrWrongGoroutine", err)
	}

	// The failed claim must not have consumed an operation ID.
	id, err := c.claimOperation()
	if err != nil {
		t.Fatalf("claimOperation() on owner: %v", err)
	}
	if id != "1" {
		t.Errorf("claimOperation() after rejected foreign claim = %q, want %q", id, "1")
	}
}

func TestChildContextInheritsStateAndPrefix(t *testing.T) {
	c := newTestContext(t, []*operation{
		execOp(),
		checkpointed("1", statusSucceeded),
		checkpointed("1-1", statusSucceeded),
	})

	entityID, err := c.claimOperation()
	if err != nil {
		t.Fatalf("claimOperation(): %v", err)
	}
	child := c.child(entityID, currentGoroutineOwner(), executionMode(c.mode.Load()))

	id, err := child.claimOperation()
	if err != nil {
		t.Fatalf("child claimOperation(): %v", err)
	}
	if id != "1-1" {
		t.Errorf("child claimOperation() = %q, want %q", id, "1-1")
	}
	if !child.IsReplaying() {
		t.Error("child IsReplaying() with checkpointed child op = false, want true")
	}
}

func TestOperationStatusTerminal(t *testing.T) {
	tests := []struct {
		status operationStatus
		want   bool
	}{
		{statusSucceeded, true},
		{statusFailed, true},
		{statusCancelled, true},
		{statusTimedOut, true},
		{statusStopped, true},
		{statusStarted, false},
		{statusPending, false},
		{statusReady, false},
	}
	for _, tt := range tests {
		if got := tt.status.terminal(); got != tt.want {
			t.Errorf("terminal(%q) = %v, want %v", tt.status, got, tt.want)
		}
	}
}

func TestRefreshReplayModeNoOpWhenCheckpointed(t *testing.T) {
	c := newTestContext(t, []*operation{
		execOp(),
		checkpointed("1", statusSucceeded),
	})

	c.refreshReplayMode()
	if !c.IsReplaying() {
		t.Error("IsReplaying() with pending operation checkpointed = false, want true")
	}
}
