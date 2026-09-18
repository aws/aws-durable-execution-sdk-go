package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// parkBound is the longest a bounded park may take in these tests. It is
// well above unfinishedReplayParkTimeout so slow CI does not flake, and
// well below the multi-second stall the park path used to produce.
const parkBound = 4 * unfinishedReplayParkTimeout

// activeBranches reads the suspend signal's active-branch count.
func activeBranches(s *suspendSignal) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

// parkOnChild parks a child of root on a fresh goroutine and returns the
// channel that receives the park outcome. op is the child operation's
// checkpoint, or nil.
func parkOnChild(root *execContext, op *operation) <-chan error {
	done := make(chan error, 1)
	child := root.child("1", root.owner, modeReplaySucceededContext)
	go func() {
		done <- child.parkUnfinishedReplay(op, "1-1", string(OperationTypeStep), operationSubTypeStep, "extra")
	}()
	return done
}

func TestParkUnfinishedReplayLastBranchReturnsWithinBound(t *testing.T) {
	// The parking goroutine is the only active branch and no pending
	// commitment exists, so nothing can fire the suspend signal. The park
	// must still return, with an error that names the operation.
	root := newTestContext(t, []*operation{execOp()})
	root.adoptBranchToken(root.suspend.registerBranchToken())

	start := time.Now()
	done := parkOnChild(root, nil)
	select {
	case err := <-done:
		var nd *NonDeterministicReplayError
		if !errors.As(err, &nd) {
			t.Fatalf("park returned %v (%T), want *NonDeterministicReplayError", err, err)
		}
		if nd.StepID != "1-1" || nd.Name != "extra" {
			t.Errorf("error names step %q / %q, want 1-1 / extra", nd.StepID, nd.Name)
		}
		if elapsed := time.Since(start); elapsed < unfinishedReplayParkTimeout {
			t.Errorf("park returned after %v, before the %v deadline", elapsed, unfinishedReplayParkTimeout)
		}
	case <-time.After(parkBound):
		t.Fatalf("park did not return within %v", parkBound)
	}
}

func TestParkUnfinishedReplayChildKeepsRootToken(t *testing.T) {
	// A child context inherits the root handler's branch token but does not
	// own it. Parking inside the child must leave the root registered, both
	// while parked and after the park returns.
	root := newTestContext(t, []*operation{execOp()})
	root.adoptBranchToken(root.suspend.registerBranchToken())
	if got := activeBranches(root.suspend); got != 1 {
		t.Fatalf("active before park = %d, want 1", got)
	}

	done := parkOnChild(root, nil)

	// Sample the count while the goroutine is parked.
	time.Sleep(unfinishedReplayParkTimeout / 4)
	select {
	case err := <-done:
		t.Fatalf("park returned %v before its deadline", err)
	default:
	}
	if got := activeBranches(root.suspend); got != 1 {
		t.Errorf("active while child parked = %d, want 1 (root token released by a child that does not own it)", got)
	}

	select {
	case <-done:
	case <-time.After(parkBound):
		t.Fatalf("park did not return within %v", parkBound)
	}
	if got := activeBranches(root.suspend); got != 1 {
		t.Errorf("active after child park returned = %d, want 1", got)
	}
	if root.suspend.fired() {
		t.Error("suspend signal fired: a child park with no commitment must not suspend the invocation")
	}
}

func TestParkUnfinishedReplayOwningBranchReleasesToken(t *testing.T) {
	// An asynchronous operation's branch owns its token. Parking on that
	// branch releases it, so a sibling's commitment can fire the signal
	// while the branch is parked, and the park then unwinds with
	// errSuspendExecution well before its deadline.
	root := newTestContext(t, []*operation{execOp()})
	root.adoptBranchToken(root.suspend.registerBranchToken())

	branch := root.branch(root.owner)
	branch.adoptBranchToken(root.suspend.registerBranchToken())
	branch.mode.Store(int32(modeReplaySucceededContext))
	if got := activeBranches(root.suspend); got != 2 {
		t.Fatalf("active before park = %d, want 2", got)
	}

	done := make(chan error, 1)
	go func() {
		done <- branch.parkUnfinishedReplay(nil, "2", string(OperationTypeWait), operationSubTypeWait, "w")
	}()

	// Wait for the branch to release its own token.
	deadline := time.Now().Add(parkBound)
	for activeBranches(root.suspend) != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("active = %d after branch parked, want 1", activeBranches(root.suspend))
		}
		time.Sleep(time.Millisecond)
	}

	// The root commits and deregisters: the parked branch is no longer
	// counted, so the signal fires and the park unwinds as a suspension.
	root.suspend.commitPending(nil)
	root.branchTok.release()
	select {
	case err := <-done:
		if !errors.Is(err, errSuspendExecution) {
			t.Fatalf("park returned %v, want errSuspendExecution", err)
		}
	case <-time.After(parkBound):
		t.Fatalf("park did not unwind within %v of the signal firing", parkBound)
	}
	if !branch.blocked.Load() {
		t.Error("branch not marked blocked after a suspended park")
	}
}

func TestParkUnfinishedReplayErrorDescribesOperation(t *testing.T) {
	root := newTestContext(t, []*operation{execOp()})
	root.adoptBranchToken(root.suspend.registerBranchToken())

	tests := []struct {
		name string
		op   *operation
		want string
	}{
		{
			name: "no checkpoint",
			op:   nil,
			want: "has no checkpoint",
		},
		{
			name: "started checkpoint",
			op: &operation{
				id: hashID("1-1"), status: statusStarted,
				opType: string(OperationTypeStep), subType: operationSubTypeStep, name: "extra",
			},
			want: "is checkpointed as STARTED",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			select {
			case err := <-parkOnChild(root, tc.op):
				var nd *NonDeterministicReplayError
				if !errors.As(err, &nd) {
					t.Fatalf("park returned %T, want *NonDeterministicReplayError", err)
				}
				msg := err.Error()
				for _, want := range []string{`step "1-1"`, `name "extra"`, "STEP/Step", tc.want, "already recorded"} {
					if !strings.Contains(msg, want) {
						t.Errorf("error %q does not mention %q", msg, want)
					}
				}
				if tc.op != nil && nd.ActualType != string(OperationTypeStep) {
					t.Errorf("ActualType = %q, want STEP", nd.ActualType)
				}
				if root.suspend.fired() {
					t.Error("suspend signal fired: the park error must not suspend the invocation")
				}
			case <-time.After(parkBound):
				t.Fatalf("park did not return within %v", parkBound)
			}
		})
	}
}

func TestReplayChildrenAwaitedUnfinishedStepFailsInvocation(t *testing.T) {
	// A step that had no checkpoint when its context's result was recorded
	// is awaited synchronously on replay. The parking goroutine is the last
	// active branch, so the invocation must return within the park deadline
	// with a determinism error instead of waiting for the Lambda deadline.
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{ReplayChildren: true}),
		wireOperation{
			Id:          hashID("1-1"),
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `"kept"`},
		},
	)
	start := time.Now()
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "big", func(childCtx Context) (string, error) {
			out, err := Step(childCtx, "s1", func(StepContext) (string, error) {
				return "", nil
			})
			if err != nil {
				return "", err
			}
			if _, err := Step(childCtx, "extra", func(StepContext) (int, error) {
				t.Error("unfinished step body must not execute on replay")
				return 1, nil
			}); err != nil {
				return "", err
			}
			return out, nil
		})
	})
	if elapsed := time.Since(start); elapsed > parkBound {
		t.Errorf("invocation took %v, want under %v", elapsed, parkBound)
	}

	var out struct {
		Status string
		Error  *struct {
			ErrorType    string
			ErrorMessage string
		}
	}
	if err := json.Unmarshal([]byte(resp), &out); err != nil {
		t.Fatalf("response %s: %v", resp, err)
	}
	if out.Status != "FAILED" || out.Error == nil {
		t.Fatalf("response = %s, want FAILED with an error", resp)
	}
	// The child body failed while re-executing, so the failure surfaces
	// in the same shape as a first-run child failure: a ChildContextError
	// whose message is the determinism error's own.
	if out.Error.ErrorType != "ChildContextError" {
		t.Errorf("ErrorType = %q, want ChildContextError", out.Error.ErrorType)
	}
	if !strings.Contains(out.Error.ErrorMessage, `name "extra"`) {
		t.Errorf("ErrorMessage = %q, does not name the unfinished step", out.Error.ErrorMessage)
	}
	if n := len(updateBatch(t, fake)); n != 0 {
		t.Errorf("replay sent %d updates, want 0", n)
	}
}

func TestParkUnfinishedReplayContextDoneWithoutCommitmentFails(t *testing.T) {
	// The Lambda context ends while parked and no pending commitment
	// exists. The park must not unwind as a suspension: that would respond
	// PENDING with no diagnostic and the next invocation would park again.
	// It returns the same determinism error the deadline produces, before
	// the deadline.
	ctx, cancel := context.WithCancel(context.Background())
	root := newExecContext(ctx, "arn:test", invocationInfo{}, nopLogger{}, newExecutionState([]*operation{execOp()}))
	root.adoptBranchToken(root.suspend.registerBranchToken())

	start := time.Now()
	done := parkOnChild(root, nil)
	cancel()
	select {
	case err := <-done:
		var nd *NonDeterministicReplayError
		if !errors.As(err, &nd) {
			t.Fatalf("park returned %v (%T), want *NonDeterministicReplayError", err, err)
		}
		if nd.StepID != "1-1" || nd.Name != "extra" {
			t.Errorf("error names step %q / %q, want 1-1 / extra", nd.StepID, nd.Name)
		}
		if elapsed := time.Since(start); elapsed >= unfinishedReplayParkTimeout {
			t.Errorf("park returned after %v; context cancellation should end it before the %v deadline", elapsed, unfinishedReplayParkTimeout)
		}
	case <-time.After(parkBound):
		t.Fatalf("park did not unwind within %v of context cancellation", parkBound)
	}
	if got := activeBranches(root.suspend); got != 1 {
		t.Errorf("active after child park = %d, want 1", got)
	}
}

func TestParkUnfinishedReplayContextDoneWithCommitmentSuspends(t *testing.T) {
	// The Lambda context ends while parked and a pending commitment exists
	// (another branch is still active, so the signal has not fired). The
	// invocation responds PENDING whatever the handler returns, so the park
	// unwinds as a suspension and the context is marked blocked.
	ctx, cancel := context.WithCancel(context.Background())
	root := newExecContext(ctx, "arn:test", invocationInfo{}, nopLogger{}, newExecutionState([]*operation{execOp()}))
	root.adoptBranchToken(root.suspend.registerBranchToken())
	sibling := root.suspend.registerBranchToken()
	defer sibling.release()
	root.suspend.commitPending(nil)

	child := root.child("1", root.owner, modeReplaySucceededContext)
	done := make(chan error, 1)
	go func() {
		done <- child.parkUnfinishedReplay(nil, "1-1", string(OperationTypeStep), operationSubTypeStep, "extra")
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, errSuspendExecution) {
			t.Fatalf("park returned %v, want errSuspendExecution", err)
		}
	case <-time.After(parkBound):
		t.Fatalf("park did not unwind within %v of context cancellation", parkBound)
	}
	if !child.blocked.Load() {
		t.Error("child context not marked blocked after suspension")
	}
	if got := activeBranches(root.suspend); got != 2 {
		t.Errorf("active after child park = %d, want 2", got)
	}
}
