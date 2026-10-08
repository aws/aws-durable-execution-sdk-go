package durable

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// manualClock is a pollClock whose time advances only when a test moves it,
// and whose timers fire only when a test fires them.
type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*manualTimer
	armed  chan *manualTimer
}

type manualTimer struct {
	clock   *manualClock
	delay   time.Duration
	f       func()
	stopped bool
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{now: now, armed: make(chan *manualTimer, 1000)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) AfterFunc(d time.Duration, f func()) pollTimer {
	c.mu.Lock()
	t := &manualTimer{clock: c, delay: d, f: f}
	c.timers = append(c.timers, t)
	c.mu.Unlock()
	c.armed <- t
	return t
}

func (t *manualTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	was := !t.stopped
	t.stopped = true
	return was
}

func (t *manualTimer) isStopped() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	return t.stopped
}

// advance moves the clock forward by d.
func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// next returns the next timer armed, failing the test if none is armed
// within a second.
func (c *manualClock) next(t *testing.T) *manualTimer {
	t.Helper()
	select {
	case tm := <-c.armed:
		return tm
	case <-time.After(time.Second):
		t.Fatal("no poll was scheduled")
		return nil
	}
}

// requireNone fails the test if a timer is armed within a short window.
func (c *manualClock) requireNone(t *testing.T) {
	t.Helper()
	select {
	case tm := <-c.armed:
		t.Fatalf("a poll was scheduled after %s, want none", tm.delay)
	case <-time.After(50 * time.Millisecond):
	}
}

// fire advances the clock to the timer's due time and runs it.
func (tm *manualTimer) fire() {
	tm.clock.advance(tm.delay)
	tm.f()
}

// pollHarness is a suspension signal with a manual clock, a poll counter,
// and one active branch that never parks, so the invocation never suspends
// while the test drives the schedule.
type pollHarness struct {
	s     *suspendSignal
	clock *manualClock
	state *executionState
	mu    sync.Mutex
	polls int
}

func newPollHarness(ops ...*operation) *pollHarness {
	h := &pollHarness{
		s:     newSuspendSignal(),
		clock: newManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
		state: newExecutionState(ops),
	}
	h.s.clock = h.clock
	h.s.poller = func() error {
		h.mu.Lock()
		h.polls++
		h.mu.Unlock()
		return nil
	}
	h.s.registerBranch() // the branch that never parks
	h.s.registerBranch() // the branch that awaits
	return h
}

func (h *pollHarness) pollCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.polls
}

// await starts a goroutine that awaits the operation with positional ID id
// and returns a channel that receives its outcome.
func (h *pollHarness) await(id string, ready func(*operation) bool, endTime func(*operation) time.Time) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := h.s.awaitOperation(h.state, id, nil, false, ready, endTime)
		done <- err
	}()
	return done
}

// TestPollSchedule covers the poll schedule: the first poll at the
// operation's end time, or one second after the goroutine blocks for an
// operation with none; then min(n, 10) seconds after the n-th poll that
// finds no change, and 60 seconds from the 96th on.
func TestPollSchedule(t *testing.T) {
	t.Run("pollSchedule", func(t *testing.T) {
		for n, want := range map[int]time.Duration{
			1: time.Second, 2: 2 * time.Second, 9: 9 * time.Second, 10: 10 * time.Second,
			11: 10 * time.Second, 95: 10 * time.Second, 96: time.Minute, 500: time.Minute,
		} {
			if got := pollSchedule(n); got != want {
				t.Errorf("pollSchedule(%d) = %s, want %s", n, got, want)
			}
		}
	})

	t.Run("wait polls at its end time, then on the backoff", func(t *testing.T) {
		start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		h := newPollHarness(&operation{id: hashID("1"), status: statusStarted, scheduledEnd: start.Add(5 * time.Second)})
		h.await("1", terminalRecord, func(op *operation) time.Time { return op.scheduledEnd })
		tm := h.clock.next(t)
		if tm.delay != 5*time.Second {
			t.Fatalf("first poll after %s, want 5s", tm.delay)
		}
		for n := 1; n <= 100; n++ {
			tm.fire()
			if got := h.pollCount(); got != n {
				t.Fatalf("polls = %d after firing %d timers", got, n)
			}
			tm = h.clock.next(t)
			if want := pollSchedule(n); tm.delay != want {
				t.Fatalf("poll %d scheduled after %s, want %s", n+1, tm.delay, want)
			}
		}
		if tm.delay != time.Minute {
			t.Errorf("poll 101 scheduled after %s, want 60s", tm.delay)
		}
	})

	for _, c := range []struct {
		name   string
		opType string
	}{{"callback", string(OperationTypeCallback)}, {"invoke", string(OperationTypeChainedInvoke)}} {
		t.Run(c.name+" polls first after one second", func(t *testing.T) {
			h := newPollHarness(&operation{id: hashID("1"), status: statusStarted, opType: c.opType})
			h.await("1", terminalRecord, noEndTime)
			if tm := h.clock.next(t); tm.delay != time.Second {
				t.Fatalf("first poll after %s, want 1s", tm.delay)
			}
		})
	}

	t.Run("step retry polls at its next attempt", func(t *testing.T) {
		start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		h := newPollHarness(&operation{id: hashID("1"), status: statusPending,
			step: &stepDetails{attempt: 1, nextAttempt: start.Add(3 * time.Second)}})
		h.await("1", attemptDueRecord, nextAttemptTime(time.Time{}))
		if tm := h.clock.next(t); tm.delay != 3*time.Second {
			t.Fatalf("first poll after %s, want 3s", tm.delay)
		}
	})
}

// TestPollDeadline covers the invocation deadline: no poll is scheduled to
// fire less than one second before it, and a poll that becomes due with
// less than one second left is not sent.
func TestPollDeadline(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("no poll scheduled within a second of the deadline", func(t *testing.T) {
		h := newPollHarness(&operation{id: hashID("1"), status: statusStarted, scheduledEnd: start.Add(5 * time.Second)})
		h.s.deadline = start.Add(5500 * time.Millisecond)
		h.await("1", terminalRecord, func(op *operation) time.Time { return op.scheduledEnd })
		h.clock.requireNone(t)
	})

	t.Run("polling stops once less than a second remains", func(t *testing.T) {
		h := newPollHarness(&operation{id: hashID("1"), status: statusStarted, scheduledEnd: start.Add(2 * time.Second)})
		h.s.deadline = start.Add(3500 * time.Millisecond)
		h.await("1", terminalRecord, func(op *operation) time.Time { return op.scheduledEnd })
		tm := h.clock.next(t)
		if tm.delay != 2*time.Second {
			t.Fatalf("first poll after %s, want 2s", tm.delay)
		}
		tm.fire()
		if got := h.pollCount(); got != 1 {
			t.Fatalf("polls = %d, want 1", got)
		}
		// The next poll would fire at 3s, half a second before the
		// deadline, so none is scheduled.
		h.clock.requireNone(t)
	})

	t.Run("a due poll with less than a second left is not sent", func(t *testing.T) {
		h := newPollHarness(&operation{id: hashID("1"), status: statusStarted, scheduledEnd: start.Add(2 * time.Second)})
		h.s.deadline = start.Add(10 * time.Second)
		h.await("1", terminalRecord, func(op *operation) time.Time { return op.scheduledEnd })
		tm := h.clock.next(t)
		h.clock.advance(7500 * time.Millisecond) // the timer runs late
		tm.fire()
		if got := h.pollCount(); got != 0 {
			t.Fatalf("polls = %d, want 0", got)
		}
	})
}

// TestPollCancelledByStatusChange covers a status change of the awaited
// operation in the response to another checkpoint request: it cancels the
// pending poll. A change to a status the waiter does not resume on
// schedules a new first poll; a change to one it resumes on resumes it and
// schedules nothing.
func TestPollCancelledByStatusChange(t *testing.T) {
	t.Run("not ready", func(t *testing.T) {
		h := newPollHarness(&operation{id: hashID("1"), status: statusPending, step: &stepDetails{attempt: 1}})
		h.await("1", attemptDueRecord, nextAttemptTime(time.Time{}))
		first := h.clock.next(t)
		h.s.onStateMerged([]*operation{{id: hashID("1"), status: statusStarted, step: &stepDetails{attempt: 1}}})
		if !first.isStopped() {
			t.Fatal("the pending poll was not cancelled")
		}
		second := h.clock.next(t)
		if second.delay != initialPollDelay {
			t.Errorf("new first poll after %s, want %s", second.delay, initialPollDelay)
		}
		// The cancelled timer, should it run anyway, sends nothing.
		first.f()
		if got := h.pollCount(); got != 0 {
			t.Errorf("polls = %d after the cancelled timer ran, want 0", got)
		}
	})

	t.Run("ready", func(t *testing.T) {
		h := newPollHarness(&operation{id: hashID("1"), status: statusStarted})
		done := h.await("1", terminalRecord, noEndTime)
		first := h.clock.next(t)
		h.s.onStateMerged([]*operation{{id: hashID("1"), status: statusSucceeded}})
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("await returned %v, want nil", err)
			}
		case <-time.After(time.Second):
			t.Fatal("the waiter did not resume")
		}
		if !first.isStopped() {
			t.Error("the pending poll was not cancelled")
		}
		h.clock.requireNone(t)
	})

	t.Run("unrelated operation", func(t *testing.T) {
		h := newPollHarness(&operation{id: hashID("1"), status: statusStarted})
		h.await("1", terminalRecord, noEndTime)
		first := h.clock.next(t)
		h.s.onStateMerged([]*operation{{id: hashID("2"), status: statusSucceeded}})
		if first.isStopped() {
			t.Error("a record of another operation cancelled the poll")
		}
	})
}

// pollCountingClient answers every checkpoint and counts the requests that
// carry no updates.
type pollCountingClient struct {
	mu     sync.Mutex
	tokens int
	polls  int
}

func (c *pollCountingClient) GetExecutionState(context.Context, GetExecutionStateInput) (GetExecutionStateOutput, error) {
	return GetExecutionStateOutput{}, nil
}

func (c *pollCountingClient) Checkpoint(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(in.Updates) == 0 {
		c.polls++
	}
	c.tokens++
	return CheckpointOutput{CheckpointToken: "tok-" + time.Now().String()}, nil
}

func (c *pollCountingClient) pollCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.polls
}

// TestLoneWaitSuspendsWithoutPolling covers a handler that only awaits a
// one second Wait with nothing else running: it responds PENDING after the
// settle period, well before the wait's end time, and sends no poll, then
// or later.
func TestLoneWaitSuspendsWithoutPolling(t *testing.T) {
	client := &pollCountingClient{}
	h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
		if err := Wait(ctx, "pause", time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}, WithExecutionClient(client))

	start := time.Now()
	out, err := h(context.Background(), stepPayload(`""`))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	var resp struct{ Status string }
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "PENDING" {
		t.Fatalf("status = %s, want PENDING", resp.Status)
	}
	if elapsed >= 500*time.Millisecond {
		t.Errorf("responded after %s, want well under the wait's one second", elapsed)
	}
	if elapsed < defaultSuspendSettle {
		t.Errorf("responded after %s, before the settle period of %s", elapsed, defaultSuspendSettle)
	}
	time.Sleep(1500 * time.Millisecond)
	if n := client.pollCount(); n != 0 {
		t.Errorf("sent %d polls, want none", n)
	}
}

// pollAnsweringClient reports the callback it assigned SUCCEEDED in the
// response to the first poll, and nothing in response to updates.
type pollAnsweringClient struct {
	mu       sync.Mutex
	tokens   int
	callback *OperationUpdate
	polls    int
}

func (c *pollAnsweringClient) GetExecutionState(context.Context, GetExecutionStateInput) (GetExecutionStateOutput, error) {
	return GetExecutionStateOutput{}, nil
}

func (c *pollAnsweringClient) Checkpoint(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tokens++
	out := CheckpointOutput{CheckpointToken: "tok-" + time.Now().String()}
	for _, u := range in.Updates {
		if u.Type == OperationTypeCallback && u.Action == OperationActionStart {
			c.callback = &u
			out.NewExecutionState = append(out.NewExecutionState, Operation{
				Id: u.Id, Type: u.Type, SubType: u.SubType, Status: OperationStatusStarted,
				CallbackDetails: &CallbackDetails{CallbackId: ptrTo("cb-1")},
			})
		}
	}
	if len(in.Updates) == 0 {
		c.polls++
		if c.polls == 1 && c.callback != nil {
			out.NewExecutionState = append(out.NewExecutionState, Operation{
				Id: c.callback.Id, Type: c.callback.Type, SubType: c.callback.SubType, Status: OperationStatusSucceeded,
				CallbackDetails: &CallbackDetails{CallbackId: ptrTo("cb-1"), Result: ptrTo(`"approved"`)},
			})
		}
	}
	return out, nil
}

func ptrTo(s string) *string { return &s }

// TestPollReportsCallbackFinished covers a callback that only a poll
// reports finished: the handler awaits it while a step runs past the first
// poll, and the invocation returns the callback's result.
func TestPollReportsCallbackFinished(t *testing.T) {
	client := &pollAnsweringClient{}
	h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
		cb, err := CreateCallback[string](ctx, "approval", WithCallbackSerdes(JSONSerdes))
		if err != nil {
			return "", err
		}
		long := StepAsync(ctx, "work", func(StepContext) (string, error) {
			time.Sleep(1500 * time.Millisecond)
			return "worked", nil
		})
		v, err := cb.Result(ctx)
		if err != nil {
			return "", err
		}
		if _, err := long.Result(ctx); err != nil {
			return "", err
		}
		return v, nil
	}, WithExecutionClient(client))

	out, err := h(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"Status":"SUCCEEDED","Result":"\"approved\""}`; string(out) != want {
		t.Errorf("response = %s, want %s", out, want)
	}
}
