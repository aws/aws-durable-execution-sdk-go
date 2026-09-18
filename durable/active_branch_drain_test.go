package durable

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// prematureResponseWindow is how long a test watches for a PENDING response
// that must not arrive while an asynchronous branch is still running user
// code. Without the drain the response arrives within microseconds of the
// blocking operation's checkpoint, so the window only needs to be long
// enough to observe it reliably.
const prematureResponseWindow = 300 * time.Millisecond

// drainOutcome is what a drain test observed after the invocation responded.
type drainOutcome struct {
	status string
	// batches holds every checkpoint call the fake received before the
	// invocation responded, in arrival order. A call that arrived after
	// the response is recorded in late instead.
	batches [][]OperationUpdate
	late    [][]OperationUpdate
}

// runSuspendWithRunningBranch runs a handler that launches an asynchronous
// branch whose body blocks on a gate, then blocks the root goroutine on a
// Wait so the invocation commits to PENDING. launch starts the branch and
// returns a function that awaits its result; it is called once the branch's
// body has started. The test releases the gate only after the Wait has been
// checkpointed and asserts that no response arrived while the branch was
// still running.
func runSuspendWithRunningBranch(t *testing.T, launch func(ctx Context, body func() (string, error)) func() (string, error)) drainOutcome {
	t.Helper()

	bodyStarted := make(chan struct{})
	gate := make(chan struct{})
	waitCheckpointed := make(chan struct{})
	var (
		mu        sync.Mutex
		batches   [][]OperationUpdate
		late      [][]OperationUpdate
		responded atomic.Bool
		closeOnce sync.Once
	)
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
			mu.Lock()
			if responded.Load() {
				late = append(late, in.Updates)
			} else {
				batches = append(batches, in.Updates)
			}
			mu.Unlock()
			for _, u := range in.Updates {
				if u.Type == OperationTypeWait && u.Action == OperationActionStart {
					closeOnce.Do(func() { close(waitCheckpointed) })
				}
			}
			return CheckpointOutput{CheckpointToken: "token-fake"}, nil
		},
	}

	h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
		await := launch(ctx, func() (string, error) {
			close(bodyStarted)
			<-gate
			return "completed", nil
		})
		// The branch has checkpointed its start and entered its body.
		<-bodyStarted
		if err := Wait(ctx, "short-wait", time.Second); err != nil {
			return "", err
		}
		return await()
	}, withLambdaAPI(fake))

	type response struct {
		raw []byte
		err error
	}
	respCh := make(chan response, 1)
	go func() {
		raw, err := h(context.Background(), stepPayload(`""`))
		mu.Lock()
		responded.Store(true)
		mu.Unlock()
		respCh <- response{raw: raw, err: err}
	}()

	<-waitCheckpointed
	select {
	case resp := <-respCh:
		if resp.err != nil {
			t.Fatalf("Invoke error while the branch was still running: %v", resp.err)
		}
		t.Fatalf("invocation responded %s while the asynchronous branch was still running", resp.raw)
	case <-time.After(prematureResponseWindow):
	}

	close(gate)
	var resp response
	select {
	case resp = <-respCh:
	case <-time.After(orphanExitTiming):
		t.Fatal("invocation did not respond after the branch finished")
	}
	if resp.err != nil {
		t.Fatalf("Invoke error: %v", resp.err)
	}

	mu.Lock()
	defer mu.Unlock()
	return drainOutcome{status: parseResponse(t, resp.raw).Status, batches: batches, late: late}
}

// requireRecordedBeforeResponse fails the test unless an update with the
// given name and action was checkpointed before the invocation responded.
// An update that arrived only after the response, or not at all, means the
// outcome was not recorded in this invocation.
func requireRecordedBeforeResponse(t *testing.T, out drainOutcome, name string, action OperationAction) *OperationUpdate {
	t.Helper()
	if u := findUpdate(out.batches, name, action); u != nil {
		return u
	}
	if findUpdate(out.late, name, action) != nil {
		t.Fatalf("%s %s was checkpointed only after the invocation responded", name, action)
	}
	t.Fatalf("%s %s was not checkpointed before the invocation responded", name, action)
	return nil
}

// findUpdate returns the first update in batches with the given name and
// action, or nil.
func findUpdate(batches [][]OperationUpdate, name string, action OperationAction) *OperationUpdate {
	for _, batch := range batches {
		for i := range batch {
			if aws.ToString(batch[i].Name) == name && batch[i].Action == action {
				return &batch[i]
			}
		}
	}
	return nil
}

// TestSuspendWaitsForRunningAsyncStep reproduces the reported scenario: an
// asynchronous step is still running its body when the handler blocks on a
// wait. The invocation must not respond PENDING until the step has finished,
// and the step's result must be checkpointed in this invocation.
func TestSuspendWaitsForRunningAsyncStep(t *testing.T) {
	out := runSuspendWithRunningBranch(t, func(ctx Context, body func() (string, error)) func() (string, error) {
		fut := StepAsync(ctx, "slow-step", func(_ StepContext) (string, error) { return body() })
		return fut.Result
	})
	if out.status != invocationPending {
		t.Fatalf("status = %q, want %q", out.status, invocationPending)
	}
	succeed := requireRecordedBeforeResponse(t, out, "slow-step", OperationActionSucceed)
	if got := aws.ToString(succeed.Payload); got != `"completed"` {
		t.Errorf("slow-step payload = %s, want %q", got, `"completed"`)
	}
}

// TestSuspendWaitsForRunningGoBranch is the durable.Go form of the reported
// scenario: a step running inside a child context branch keeps the
// invocation from responding PENDING until the step has recorded its
// outcome. The child context records its own completion after the step
// returns; that record belongs to this invocation too, so it must also be
// checkpointed before the response. Otherwise the next invocation would
// find the child context unfinished and run its body again.
func TestSuspendWaitsForRunningGoBranch(t *testing.T) {
	out := runSuspendWithRunningBranch(t, func(ctx Context, body func() (string, error)) func() (string, error) {
		fut := Go(ctx, "slow-branch", func(c Context) (string, error) {
			return Step(c, "inner-step", func(_ StepContext) (string, error) { return body() })
		})
		return fut.Result
	})
	if out.status != invocationPending {
		t.Fatalf("status = %q, want %q", out.status, invocationPending)
	}
	requireRecordedBeforeResponse(t, out, "inner-step", OperationActionSucceed)
	succeed := requireRecordedBeforeResponse(t, out, "slow-branch", OperationActionSucceed)
	if got := aws.ToString(succeed.Payload); got != `"completed"` {
		t.Errorf("slow-branch payload = %s, want %q", got, `"completed"`)
	}
}

// TestSuspendWaitsForRunningConditionCheck covers the other user-code body
// the drain tracks: a WaitForCondition check running on a child context
// branch while the handler blocks.
func TestSuspendWaitsForRunningConditionCheck(t *testing.T) {
	out := runSuspendWithRunningBranch(t, func(ctx Context, body func() (string, error)) func() (string, error) {
		fut := Go(ctx, "poller", func(c Context) (string, error) {
			return WaitForCondition(c, "poll",
				func(_ StepContext, _ string) (string, error) { return body() },
				ConditionConfig[string]{
					InitialState: "pending",
					WaitStrategy: func(state string, _ int) WaitDecision {
						return WaitDecision{Continue: state != "completed", Delay: time.Second}
					},
				})
		})
		return fut.Result
	})
	if out.status != invocationPending {
		t.Fatalf("status = %q, want %q", out.status, invocationPending)
	}
	requireRecordedBeforeResponse(t, out, "poll", OperationActionSucceed)
	requireRecordedBeforeResponse(t, out, "poller", OperationActionSucceed)
}

// TestSuspendDoesNotStallBehindBlockedBranch asserts that the drain waits
// only for step and condition bodies that are running. A branch that is
// itself blocked on a pending operation runs no body, so the invocation
// responds PENDING promptly.
func TestSuspendDoesNotStallBehindBlockedBranch(t *testing.T) {
	fake := &fakeLambda{}
	h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
		// Never awaited: its goroutine checkpoints START, commits, and
		// deregisters on its own.
		_ = WaitAsync(ctx, "async-wait", time.Second)
		if err := Wait(ctx, "root-wait", time.Second); err != nil {
			return "", err
		}
		return "unreached", nil
	}, withLambdaAPI(fake))

	respCh := make(chan []byte, 1)
	go func() {
		raw, err := h(context.Background(), stepPayload(`""`))
		if err != nil {
			t.Errorf("Invoke error: %v", err)
		}
		respCh <- raw
	}()
	select {
	case raw := <-respCh:
		if status := parseResponse(t, raw).Status; status != invocationPending {
			t.Fatalf("status = %q, want %q", status, invocationPending)
		}
	case <-time.After(orphanExitTiming):
		t.Fatal("invocation stalled behind a branch that is blocked, not running")
	}
}

// TestSuspendDrainEndsWhenContextEnds asserts that a running branch cannot
// hold the response past the end of the invocation's context: when the
// Lambda context is cancelled while a branch is still running, the
// invocation responds PENDING and the branch's later checkpoint is refused.
func TestSuspendDrainEndsWhenContextEnds(t *testing.T) {
	fake := &fakeLambda{}
	bodyStarted := make(chan struct{})
	gate := make(chan struct{})
	stepErr := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
		fut := StepAsync(ctx, "stuck-step", func(_ StepContext) (string, error) {
			close(bodyStarted)
			<-gate
			return "late", nil
		})
		go func() {
			_, err := fut.Result()
			stepErr <- err
		}()
		<-bodyStarted
		if err := Wait(ctx, "root-wait", time.Second); err != nil {
			return "", err
		}
		return "unreached", nil
	}, withLambdaAPI(fake))

	respCh := make(chan []byte, 1)
	go func() {
		raw, err := h(ctx, stepPayload(`""`))
		if err != nil {
			t.Errorf("Invoke error: %v", err)
		}
		respCh <- raw
	}()

	<-bodyStarted
	select {
	case <-respCh:
		t.Fatal("invocation responded while the step was still running")
	case <-time.After(prematureResponseWindow):
	}
	cancel()
	select {
	case raw := <-respCh:
		if status := parseResponse(t, raw).Status; status != invocationPending {
			t.Fatalf("status = %q, want %q", status, invocationPending)
		}
	case <-time.After(orphanExitTiming):
		t.Fatal("invocation did not respond after its context ended")
	}

	close(gate)
	select {
	case err := <-stepErr:
		if !errors.Is(err, errSuspendExecution) {
			t.Errorf("stuck-step error = %v, want errSuspendExecution", err)
		}
	case <-time.After(orphanExitTiming):
		t.Fatal("stuck-step did not settle after the invocation responded")
	}
}

// TestSuspendDrainWaitsForStepStartedDuringSettle covers a branch that is
// idle between operations when the handler blocks, and starts a step only
// after the invocation first became eligible to respond. The drain observes
// nothing executing and enters its settle period; the step begins inside
// that period, so the drain must notice and wait for the step and the
// child context's completion to be recorded before responding.
func TestSuspendDrainWaitsForStepStartedDuringSettle(t *testing.T) {
	// Long enough that the release below lands inside the settle period
	// under load. The test does not wait it out: once the branch has
	// recorded its completion and deregistered, the drain returns at once.
	const settle = 2 * time.Second

	branchIdle := make(chan struct{})
	release := make(chan struct{})
	bodyStarted := make(chan struct{})
	gate := make(chan struct{})
	waitCheckpointed := make(chan struct{})
	var (
		mu        sync.Mutex
		batches   [][]OperationUpdate
		late      [][]OperationUpdate
		responded atomic.Bool
		closeOnce sync.Once
	)
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
			mu.Lock()
			if responded.Load() {
				late = append(late, in.Updates)
			} else {
				batches = append(batches, in.Updates)
			}
			mu.Unlock()
			for _, u := range in.Updates {
				if u.Type == OperationTypeWait && u.Action == OperationActionStart {
					closeOnce.Do(func() { close(waitCheckpointed) })
				}
			}
			return CheckpointOutput{CheckpointToken: "token-fake"}, nil
		},
	}

	h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
		fut := Go(ctx, "late-branch", func(c Context) (string, error) {
			// Idle between operations: registered as a branch, but
			// not an executing span.
			close(branchIdle)
			<-release
			return Step(c, "late-step", func(_ StepContext) (string, error) {
				close(bodyStarted)
				<-gate
				return "completed", nil
			})
		})
		<-branchIdle
		if err := Wait(ctx, "short-wait", time.Second); err != nil {
			return "", err
		}
		return fut.Result()
	}, withLambdaAPI(fake), withSuspendSettle(settle))

	type response struct {
		raw []byte
		err error
	}
	respCh := make(chan response, 1)
	go func() {
		raw, err := h(context.Background(), stepPayload(`""`))
		mu.Lock()
		responded.Store(true)
		mu.Unlock()
		respCh <- response{raw: raw, err: err}
	}()

	<-waitCheckpointed
	// The handler goroutine unwinds and the drain begins its settle
	// period: the branch is registered, nothing is executing. Start the
	// step inside that period.
	time.Sleep(settle / 10)
	close(release)
	select {
	case <-bodyStarted:
	case resp := <-respCh:
		// The branch's step START was refused by a terminated
		// checkpointer, so its body never ran.
		if resp.err != nil {
			t.Fatalf("Invoke error before the late step ran: %v", resp.err)
		}
		t.Fatalf("invocation responded %s before the late step ran", resp.raw)
	case <-time.After(orphanExitTiming):
		t.Fatal("the late step did not start")
	}
	select {
	case resp := <-respCh:
		if resp.err != nil {
			t.Fatalf("Invoke error while the late step was running: %v", resp.err)
		}
		t.Fatalf("invocation responded %s while the late step was running", resp.raw)
	case <-time.After(prematureResponseWindow):
	}

	close(gate)
	var resp response
	select {
	case resp = <-respCh:
	case <-time.After(orphanExitTiming):
		t.Fatal("invocation did not respond after the late step finished")
	}
	if resp.err != nil {
		t.Fatalf("Invoke error: %v", resp.err)
	}
	mu.Lock()
	out := drainOutcome{status: parseResponse(t, resp.raw).Status, batches: batches, late: late}
	mu.Unlock()

	if out.status != invocationPending {
		t.Fatalf("status = %q, want %q", out.status, invocationPending)
	}
	requireRecordedBeforeResponse(t, out, "late-step", OperationActionSucceed)
	requireRecordedBeforeResponse(t, out, "late-branch", OperationActionSucceed)
}

// TestAwaitDrainReturnsWhenNoBranchRegistered asserts that the drain does
// not wait out a settle period when no branch remains that could begin an
// executing span.
func TestAwaitDrainReturnsWhenNoBranchRegistered(t *testing.T) {
	s := newSuspendSignal()
	start := time.Now()
	s.awaitDrain(context.Background(), time.Hour)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("awaitDrain took %v with no branch registered", elapsed)
	}
}

// TestAwaitDrainReturnsWhenLastBranchDeregisters asserts that a branch
// deregistering during the settle period ends the drain at once.
func TestAwaitDrainReturnsWhenLastBranchDeregisters(t *testing.T) {
	s := newSuspendSignal()
	tok := s.registerBranchToken()
	done := make(chan struct{})
	start := time.Now()
	go func() {
		s.awaitDrain(context.Background(), time.Hour)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	tok.release()
	select {
	case <-done:
	case <-time.After(orphanExitTiming):
		t.Fatal("awaitDrain did not return after the last branch deregistered")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("awaitDrain took %v after the last branch deregistered", elapsed)
	}
}

// TestAwaitDrainRestartsSettleAfterSpan asserts that a span which begins and
// ends during the settle period is detected through the generation stamp,
// and the settle period starts over. The drain therefore takes at least two
// settle periods.
func TestAwaitDrainRestartsSettleAfterSpan(t *testing.T) {
	const settle = 300 * time.Millisecond
	s := newSuspendSignal()
	tok := s.registerBranchToken()
	defer tok.release()

	done := make(chan struct{})
	start := time.Now()
	go func() {
		s.awaitDrain(context.Background(), settle)
		close(done)
	}()
	// Well inside the first settle period.
	time.Sleep(settle / 10)
	s.enterExecuting()
	s.exitExecuting()

	select {
	case <-done:
	case <-time.After(orphanExitTiming):
		t.Fatal("awaitDrain did not return")
	}
	if elapsed := time.Since(start); elapsed < 2*settle {
		t.Fatalf("awaitDrain returned after %v; a span during the settle period must restart it (want at least %v)", elapsed, 2*settle)
	}
}

// TestAwaitDrainWaitsForExecutingSpan asserts that the drain does not return
// while a span is executing, and returns once the span has ended and the
// settle period has passed with no other span.
func TestAwaitDrainWaitsForExecutingSpan(t *testing.T) {
	const settle = 50 * time.Millisecond
	s := newSuspendSignal()
	tok := s.registerBranchToken()
	defer tok.release()
	s.enterExecuting()

	done := make(chan struct{})
	go func() {
		s.awaitDrain(context.Background(), settle)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("awaitDrain returned while a span was executing")
	case <-time.After(prematureResponseWindow):
	}
	s.exitExecuting()
	select {
	case <-done:
	case <-time.After(orphanExitTiming):
		t.Fatal("awaitDrain did not return after the span ended")
	}
}

// TestAwaitDrainReturnsWhenContextEnds asserts that the invocation's context
// bounds the drain even while a span is executing.
func TestAwaitDrainReturnsWhenContextEnds(t *testing.T) {
	s := newSuspendSignal()
	tok := s.registerBranchToken()
	defer tok.release()
	s.enterExecuting()
	defer s.exitExecuting()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.awaitDrain(ctx, time.Hour)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(orphanExitTiming):
		t.Fatal("awaitDrain did not return after its context ended")
	}
}
