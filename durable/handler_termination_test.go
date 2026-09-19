package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// orphanExitTiming bounds how long an orphaned branch may take to observe
// termination and exit once the invocation has responded.
const orphanExitTiming = 5 * time.Second

// terminalExit describes one way the root handler can finish while a
// durable.Go branch is still mid-flight.
type terminalExit struct {
	name string
	// finish is the handler's final action after launching the orphan.
	finish func() (string, error)
	// wantStatus is the response status the invocation must report.
	wantStatus string
}

var terminalExits = []terminalExit{
	{
		name:       "success",
		finish:     func() (string, error) { return "handler-done", nil },
		wantStatus: invocationSucceeded,
	},
	{
		name:       "error",
		finish:     func() (string, error) { return "", errors.New("handler failed") },
		wantStatus: invocationFailed,
	},
	{
		name:       "panic",
		finish:     func() (string, error) { panic("handler panicked") },
		wantStatus: invocationFailed,
	},
}

// orphanOutcome is what runOrphanAfterTerminalExit observed.
type orphanOutcome struct {
	// stepErr is the error the orphan's step returned.
	stepErr error
	// exited is whether the orphan goroutine finished within the bound.
	exited bool
	// root is the root context captured inside the handler.
	root *execContext
	fake *fakeLambda
}

// runOrphanAfterTerminalExit runs a handler that launches a durable.Go branch
// blocked on a gate and then finishes per exit. After Invoke returns it
// releases the gate so the orphan attempts a step, and reports what the
// orphan observed.
func runOrphanAfterTerminalExit(t *testing.T, exit terminalExit) orphanOutcome {
	t.Helper()
	out := orphanOutcome{fake: &fakeLambda{}}

	gate := make(chan struct{})
	done := make(chan struct{})

	h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
		ec, ok := ctx.(*execContext)
		if !ok {
			t.Errorf("Context is %T, want *execContext", ctx)
		}
		out.root = ec
		_ = Go(ctx, "orphan", func(c Context) (string, error) {
			defer close(done)
			<-gate
			_, err := Step(c, "orphan-step", func(_ StepContext) (string, error) {
				return "late", nil
			})
			out.stepErr = err
			return "orphan", err
		})
		return exit.finish()
	}, withLambdaAPI(out.fake))

	raw, err := h(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatalf("Invoke error: %v", err)
	}
	var resp invocationResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Status != exit.wantStatus {
		t.Fatalf("status = %q, want %q", resp.Status, exit.wantStatus)
	}

	// The invocation has responded. Let the orphan proceed.
	close(gate)
	select {
	case <-done:
		out.exited = true
	case <-time.After(orphanExitTiming):
		out.exited = false
	}
	return out
}

// TestOrphanBranchRefusedAfterTerminalExit asserts that after the handler
// finishes by success, error, or panic, an orphaned durable.Go branch's next
// Step is refused with a suspension error, records no checkpoint, and its
// goroutine exits within a bounded time.
func TestOrphanBranchRefusedAfterTerminalExit(t *testing.T) {
	for _, exit := range terminalExits {
		t.Run(exit.name, func(t *testing.T) {
			out := runOrphanAfterTerminalExit(t, exit)
			if !out.exited {
				t.Fatalf("orphan branch did not exit within %v after the invocation responded", orphanExitTiming)
			}
			if out.stepErr == nil {
				t.Fatal("orphan step succeeded after the invocation responded; want refusal")
			}
			if !errors.Is(out.stepErr, errSuspendExecution) {
				t.Errorf("orphan step error = %v, want errSuspendExecution", out.stepErr)
			}

			out.fake.mu.Lock()
			batches := make([][]OperationUpdate, len(out.fake.gotUpdateBatches))
			copy(batches, out.fake.gotUpdateBatches)
			out.fake.mu.Unlock()
			for _, batch := range batches {
				for _, u := range batch {
					if aws.ToString(u.Name) == "orphan-step" {
						t.Errorf("orphan-step checkpoint was recorded (action=%s); want refusal", u.Action)
					}
				}
			}
		})
	}
}

// TestRootBranchTokenReleasedAfterTerminalExit asserts that the root
// handler's branch token is released on every terminal exit, so once the
// orphaned branches deregister no active branch remains registered.
func TestRootBranchTokenReleasedAfterTerminalExit(t *testing.T) {
	for _, exit := range terminalExits {
		t.Run(exit.name, func(t *testing.T) {
			out := runOrphanAfterTerminalExit(t, exit)
			if !out.exited {
				t.Fatalf("orphan branch did not exit within %v", orphanExitTiming)
			}
			root := out.root
			// The orphan's deferred release runs after it closes done;
			// wait for the count to settle.
			deadline := time.Now().Add(orphanExitTiming)
			for {
				root.suspend.mu.Lock()
				active := root.suspend.active
				root.suspend.mu.Unlock()
				if active == 0 {
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("active branches = %d after invocation and orphan exit, want 0", active)
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
}

// TestOrphanBranchRefusedBeforeInvocationPostProcessing asserts that the
// checkpointer is terminated the moment the handler's outcome is decided,
// before WrapInvocation post-processing and the OnInvocationEnd hook run.
// A plugin's post-processing releases the orphan and observes its step
// refused while the plugin is still running, on every terminal exit.
func TestOrphanBranchRefusedBeforeInvocationPostProcessing(t *testing.T) {
	for _, exit := range terminalExits {
		t.Run(exit.name, func(t *testing.T) {
			fake := &fakeLambda{}
			gate := make(chan struct{})
			stepErr := make(chan error, 1)

			// observed is what the plugin saw the orphan's step return
			// during WrapInvocation post-processing; errNotObserved
			// means the orphan had not settled within the bound.
			errNotObserved := errors.New("orphan step did not settle during post-processing")
			var observedInWrap, observedAtEnd error
			plugin := Plugin{
				WrapInvocation: func(ctx context.Context, _ InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
					res, err := fn(ctx)
					// The handler's outcome is decided. Release the orphan
					// and wait for its step to settle.
					close(gate)
					select {
					case observedInWrap = <-stepErr:
						stepErr <- observedInWrap
					case <-time.After(orphanExitTiming):
						observedInWrap = errNotObserved
					}
					return res, err
				},
				OnInvocationEnd: func(_ context.Context, _ InvocationEndHookInfo) {
					select {
					case observedAtEnd = <-stepErr:
					default:
						observedAtEnd = errNotObserved
					}
				},
			}

			h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
				_ = Go(ctx, "orphan", func(c Context) (string, error) {
					<-gate
					_, err := Step(c, "orphan-step", func(_ StepContext) (string, error) {
						return "late", nil
					})
					stepErr <- err
					return "orphan", err
				})
				return exit.finish()
			}, withLambdaAPI(fake), WithPlugins(plugin))

			raw, err := h(context.Background(), stepPayload(`""`))
			if err != nil {
				t.Fatalf("Invoke error: %v", err)
			}
			if resp := parseResponse(t, raw); resp.Status != exit.wantStatus {
				t.Fatalf("status = %q, want %q", resp.Status, exit.wantStatus)
			}
			if !errors.Is(observedInWrap, errSuspendExecution) {
				t.Errorf("orphan step during WrapInvocation post-processing = %v, want errSuspendExecution", observedInWrap)
			}
			if !errors.Is(observedAtEnd, errSuspendExecution) {
				t.Errorf("orphan step at OnInvocationEnd = %v, want errSuspendExecution", observedAtEnd)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			for _, batch := range fake.gotUpdateBatches {
				if carriesName(batch, "orphan-step") {
					t.Error("orphan-step checkpoint was recorded; want refusal")
				}
			}
		})
	}
}

// TestOversizedResultCheckpointedBehindInFlightOrphan asserts that when an
// orphan branch's checkpoint call is in flight as the handler returns an
// oversized result, the invocation's final write is still sent, after that
// call and with the token it rotated to, while the orphan's own request is
// refused.
func TestOversizedResultCheckpointedBehindInFlightOrphan(t *testing.T) {
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)

	orphanInFlight := make(chan struct{})
	handlerDecided := make(chan struct{})
	var (
		mu      sync.Mutex
		tokens  []string
		batches [][]OperationUpdate
	)
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(ctx context.Context, in CheckpointInput) (CheckpointOutput, error) {
			mu.Lock()
			tokens = append(tokens, in.CheckpointToken)
			batches = append(batches, in.Updates)
			n := len(tokens)
			mu.Unlock()
			if carriesName(in.Updates, "orphan-step") {
				// The orphan's step START: hold it until the handler's
				// outcome has been decided.
				close(orphanInFlight)
				select {
				case <-handlerDecided:
				case <-ctx.Done():
					return CheckpointOutput{}, ctx.Err()
				}
			}
			return CheckpointOutput{CheckpointToken: "token-" + strconv.Itoa(n)}, nil
		},
	}

	stepErr := make(chan error, 1)
	plugin := Plugin{
		WrapInvocation: func(ctx context.Context, _ InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			res, err := fn(ctx)
			// runHandler has returned, so the checkpointer is terminated.
			// Let the orphan's in-flight call complete.
			close(handlerDecided)
			return res, err
		},
	}

	h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
		_ = Go(ctx, "orphan", func(c Context) (string, error) {
			_, err := Step(c, "orphan-step", func(_ StepContext) (string, error) {
				return "late", nil
			})
			stepErr <- err
			return "orphan", err
		})
		<-orphanInFlight
		return large, nil
	}, withLambdaAPI(fake), WithPlugins(plugin))

	raw, err := h(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatalf("Invoke error: %v", err)
	}
	resp := parseResponse(t, raw)
	if resp.Status != invocationSucceeded {
		t.Fatalf("status = %q, want %q", resp.Status, invocationSucceeded)
	}
	if resp.Result == nil || *resp.Result != "" {
		t.Fatalf("Result = %v, want empty (oversized result is checkpointed)", resp.Result)
	}

	select {
	case err := <-stepErr:
		if !errors.Is(err, errSuspendExecution) {
			t.Errorf("orphan step error = %v, want errSuspendExecution", err)
		}
	case <-time.After(orphanExitTiming):
		t.Fatal("orphan step did not settle after the invocation responded")
	}

	mu.Lock()
	defer mu.Unlock()
	// Calls: the child context START issued by the handler, the orphan's
	// step START held in flight, then the final result write.
	if len(batches) != 3 {
		t.Fatalf("checkpoint calls = %d, want 3 (child START, orphan step START, final result)", len(batches))
	}
	if !carriesName(batches[1], "orphan-step") {
		t.Fatalf("second call carried %+v, want the orphan's step START", batches[1])
	}
	if tokens[2] != "token-2" {
		t.Errorf("final write sent with token %q, want token-2 (rotated by the orphan's in-flight call)", tokens[2])
	}
	final := batches[2]
	if len(final) != 1 || final[0].Type != OperationTypeExecution || final[0].Action != OperationActionSucceed {
		t.Fatalf("final call carried %+v, want one EXECUTION/SUCCEED update", final)
	}
}

// carriesName reports whether any update in batch has the given Name.
func carriesName(batch []OperationUpdate, name string) bool {
	for _, u := range batch {
		if aws.ToString(u.Name) == name {
			return true
		}
	}
	return false
}

// TestOrphanStepBodyStartedBeforeTerminationIsRefusedAtNextCheckpoint
// asserts the boundary at which termination stops an orphan: a Step whose
// START checkpoint completed before the handler returned runs its body to
// completion, and the checkpoint that would record the body's result is the
// one refused.
func TestOrphanStepBodyStartedBeforeTerminationIsRefusedAtNextCheckpoint(t *testing.T) {
	fake := &fakeLambda{}
	bodyStarted := make(chan struct{})
	gate := make(chan struct{})
	stepErr := make(chan error, 1)
	var bodyRan atomic.Bool

	h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
		_ = Go(ctx, "orphan", func(c Context) (string, error) {
			_, err := Step(c, "orphan-step", func(_ StepContext) (string, error) {
				close(bodyStarted)
				<-gate
				bodyRan.Store(true)
				return "late", nil
			})
			stepErr <- err
			return "orphan", err
		})
		<-bodyStarted
		return "handler-done", nil
	}, withLambdaAPI(fake))

	raw, err := h(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatalf("Invoke error: %v", err)
	}
	if resp := parseResponse(t, raw); resp.Status != invocationSucceeded {
		t.Fatalf("status = %q, want %q", resp.Status, invocationSucceeded)
	}

	close(gate)
	select {
	case err := <-stepErr:
		if !errors.Is(err, errSuspendExecution) {
			t.Errorf("orphan step error = %v, want errSuspendExecution", err)
		}
	case <-time.After(orphanExitTiming):
		t.Fatal("orphan step did not settle after the invocation responded")
	}
	if !bodyRan.Load() {
		t.Error("step body did not run to completion; termination must not preempt user code")
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	var started, ended bool
	for _, batch := range fake.gotUpdateBatches {
		for _, u := range batch {
			if aws.ToString(u.Name) != "orphan-step" {
				continue
			}
			switch u.Action {
			case OperationActionStart:
				started = true
			default:
				ended = true
			}
		}
	}
	if !started {
		t.Error("orphan-step START was not recorded before the handler returned")
	}
	if ended {
		t.Error("orphan-step result was recorded after the invocation responded; want refusal")
	}
}
