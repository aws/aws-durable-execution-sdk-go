// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// The tests in this file cover GitHub issue #103. A context replays the
// operations an earlier invocation checkpointed. It must leave replay when
// a blocking operation returns to code that no earlier invocation ran.
// Otherwise the first run of that code logs nothing and IsReplaying
// reports true there.

// lineRecord is one log record a lineRecorder received.
type lineRecord struct {
	message string
	// replay is true when the record carries the SDK's replay attribute.
	replay bool
}

// lineRecorder is a slog.Handler that records the message of every record
// and whether the record carries replay=true. Handlers derived with
// WithAttrs and WithGroup share the recording.
type lineRecorder struct {
	mu      *sync.Mutex
	records *[]lineRecord
	replay  bool
}

func newLineRecorder() *lineRecorder {
	return &lineRecorder{mu: &sync.Mutex{}, records: &[]lineRecord{}}
}

func (h *lineRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *lineRecorder) Handle(_ context.Context, r slog.Record) error {
	replay := h.replay
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "replay" && a.Value.Kind() == slog.KindBool && a.Value.Bool() {
			replay = true
		}
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.records = append(*h.records, lineRecord{message: r.Message, replay: replay})
	return nil
}

func (h *lineRecorder) WithAttrs(attrs []slog.Attr) slog.Handler {
	replay := h.replay
	for _, a := range attrs {
		if a.Key == "replay" && a.Value.Kind() == slog.KindBool && a.Value.Bool() {
			replay = true
		}
	}
	return &lineRecorder{mu: h.mu, records: h.records, replay: replay}
}

func (h *lineRecorder) WithGroup(string) slog.Handler { return h }

// count returns how many records with message msg were recorded.
func (h *lineRecorder) count(msg string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range *h.records {
		if r.message == msg {
			n++
		}
	}
	return n
}

// withMessage returns the records with message msg.
func (h *lineRecorder) withMessage(msg string) []lineRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []lineRecord
	for _, r := range *h.records {
		if r.message == msg {
			out = append(out, r)
		}
	}
	return out
}

// messages returns every recorded message, oldest first, one per line.
func (h *lineRecorder) messages() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	msgs := make([]string, 0, len(*h.records))
	for _, r := range *h.records {
		msgs = append(msgs, r.message)
	}
	return strings.Join(msgs, "\n")
}

// replayingAt records ctx.IsReplaying() each time a handler reaches a
// line. The handler goroutine is the only writer, and invocations run one
// after another.
type replayingAt []bool

func (r *replayingAt) mark(ctx durable.Context) { *r = append(*r, ctx.IsReplaying()) }

// assertLineOnce fails the test unless msg was logged exactly once and
// every observation in seen is false, that is, IsReplaying reported false
// each time the handler reached the line.
func assertLineOnce(t *testing.T, rec *lineRecorder, msg string, seen replayingAt) {
	t.Helper()
	if got := rec.count(msg); got != 1 {
		t.Errorf("%q logged %d times, want 1; log:\n%s", msg, got, rec.messages())
	}
	if len(seen) == 0 {
		t.Fatalf("the handler never reached the %q line", msg)
	}
	for i, replaying := range seen {
		if replaying {
			t.Errorf("IsReplaying() at the %q line = true on reach %d of %d, want false", msg, i+1, len(seen))
		}
	}
}

// twoAttempts is a retry strategy that makes two attempts with a one
// second delay between them.
func twoAttempts(t *testing.T) durable.RetryStrategy {
	t.Helper()
	s, err := durable.NewRetryStrategy(durable.RetryConfig{MaxAttempts: 2, InitialDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestLogLineAfterFinalWait is the reproduction from issue #103. The line
// after the final Wait runs for the first time in the second invocation.
// It must be logged there, and IsReplaying must report false on it.
func TestLogLineAfterFinalWait(t *testing.T) {
	rec := newLineRecorder()
	var seen replayingAt

	handler := func(ctx durable.Context, _ string) (string, error) {
		_, err := durable.Step(ctx, "fetch", func(durable.StepContext) (string, error) {
			return "x", nil
		})
		if err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "pause", 5*time.Second); err != nil {
			return "", err
		}
		seen.mark(ctx)
		ctx.Logger().Info("done")
		return "ok", nil
	}

	runner := durabletest.NewLocalRunner(handler, durable.WithLogHandler(rec))
	result := runner.RunUntilComplete(t, "event")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	if got := len(result.Invocations); got != 2 {
		t.Fatalf("invocations = %d, want 2", got)
	}
	assertLineOnce(t, rec, "done", seen)
}

// TestLineAfterReplayedOperationRunsLive covers each blocking operation
// that the replay mode check follows. In every case the operation returns
// its result in an invocation that started in replay, and no earlier
// invocation ran the line after it. So that line must be logged exactly
// once, with IsReplaying false. The line before the operation ran in the
// first invocation, so it must also be logged exactly once: replay still
// suppresses it.
func TestLineAfterReplayedOperationRunsLive(t *testing.T) {
	cases := []struct {
		name string
		// op runs the operation under test. It returns an error only
		// for the suspension signal or an unexpected failure.
		op func(t *testing.T, ctx durable.Context) error
		// resolve completes what the execution blocks on after the
		// first RunUntilComplete. Nil when timers alone drive it.
		resolve func(t *testing.T, runner *durabletest.LocalRunner[string, string])
	}{
		{
			// The first attempt fails and the retry delay suspends
			// the execution. The second invocation runs the second
			// attempt, which succeeds.
			name: "Step",
			op: func(t *testing.T, ctx durable.Context) error {
				_, err := durable.Step(ctx, "flaky", func(sc durable.StepContext) (int, error) {
					if sc.Attempt() == 1 {
						return 0, errors.New("transient")
					}
					return sc.Attempt(), nil
				}, durable.WithRetry(twoAttempts(t)))
				return err
			},
		},
		{
			name: "Wait",
			op: func(_ *testing.T, ctx durable.Context) error {
				return durable.Wait(ctx, "pause", 5*time.Second)
			},
		},
		{
			name: "Invoke",
			op: func(_ *testing.T, ctx durable.Context) error {
				_, err := durable.Invoke[string](ctx, "call", "target:$LATEST", "in")
				return err
			},
			resolve: func(t *testing.T, runner *durabletest.LocalRunner[string, string]) {
				if err := runner.CompleteChainedInvoke("call", "out"); err != nil {
					t.Fatalf("complete invoke: %v", err)
				}
			},
		},
		{
			// The child suspends on its wait. The second invocation
			// replays the child's wait and completes the child.
			name: "RunInChildContext",
			op: func(_ *testing.T, ctx durable.Context) error {
				_, err := durable.RunInChildContext(ctx, "child", func(c durable.Context) (string, error) {
					if err := durable.Wait(c, "inner", 5*time.Second); err != nil {
						return "", err
					}
					return "child-done", nil
				})
				return err
			},
		},
		{
			// The first check does not meet the condition and the
			// strategy suspends for a poll delay. The second
			// invocation's check meets it.
			name: "WaitForCondition",
			op: func(_ *testing.T, ctx durable.Context) error {
				_, err := durable.WaitForCondition(ctx, "poll",
					func(_ durable.StepContext, state int) (int, error) { return state + 1, nil },
					durable.ConditionConfig[int]{
						InitialState: 0,
						WaitStrategy: func(state int, _ int) durable.WaitDecision {
							if state >= 2 {
								return durable.WaitDecision{Continue: false}
							}
							return durable.WaitDecision{Continue: true, Delay: time.Second}
						},
					})
				return err
			},
		},
		{
			// The first attempt fails. The second attempt suspends on a
			// wait inside it, after the backoff wait. The last
			// invocation replays the attempts and the second attempt
			// completes there.
			name: "Retry",
			op: func(t *testing.T, ctx durable.Context) error {
				_, err := durable.Retry(ctx, "group", func(c durable.Context, attempt int) (int, error) {
					if attempt == 1 {
						return 0, errors.New("transient")
					}
					if err := durable.Wait(c, "inner", 5*time.Second); err != nil {
						return 0, err
					}
					return attempt, nil
				}, twoAttempts(t))
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := newLineRecorder()
			var seen replayingAt

			handler := func(ctx durable.Context, _ string) (string, error) {
				ctx.Logger().Info("before")
				if err := tc.op(t, ctx); err != nil {
					return "", err
				}
				seen.mark(ctx)
				ctx.Logger().Info("after")
				return "ok", nil
			}

			runner := durabletest.NewLocalRunner(handler, durable.WithLogHandler(rec))
			result := runner.RunUntilComplete(t, "event")
			if tc.resolve != nil {
				if result.Status != durabletest.Pending {
					t.Fatalf("status = %s, want PENDING before resolving", result.Status)
				}
				tc.resolve(t, runner)
				result = runner.RunUntilComplete(t, "event")
			}
			if result.Status != durabletest.Succeeded {
				t.Fatalf("status = %s, want SUCCEEDED; error %+v", result.Status, result.Error)
			}
			if got := len(result.Invocations); got < 2 {
				t.Fatalf("invocations = %d, want at least 2: the operation must return in a replaying invocation", got)
			}
			assertLineOnce(t, rec, "after", seen)
			if got := rec.count("before"); got != 1 {
				t.Errorf("\"before\" logged %d times, want 1: replay must still suppress it; log:\n%s", got, rec.messages())
			}
		})
	}
}

// TestLineAfterTerminalErrorRunsLive covers an operation that returns a
// terminal error in an invocation that started in replay. The handler
// catches the error and logs. No earlier invocation ran that line, so it
// must be logged exactly once with IsReplaying false.
func TestLineAfterTerminalErrorRunsLive(t *testing.T) {
	t.Run("Step", func(t *testing.T) {
		rec := newLineRecorder()
		var seen replayingAt

		// Both attempts fail. The retry delay after the first
		// suspends, so the second invocation starts in replay and
		// the step fails there for good.
		handler := func(ctx durable.Context, _ string) (string, error) {
			_, err := durable.Step(ctx, "charge", func(durable.StepContext) (int, error) {
				return 0, errors.New("declined")
			}, durable.WithRetry(twoAttempts(t)))
			var stepErr *durable.StepError
			if !errors.As(err, &stepErr) {
				return "", err
			}
			seen.mark(ctx)
			ctx.Logger().Info("caught")
			return "declined", nil
		}

		runner := durabletest.NewLocalRunner(handler, durable.WithLogHandler(rec))
		result := runner.RunUntilComplete(t, "event")
		if result.Status != durabletest.Succeeded {
			t.Fatalf("status = %s, want SUCCEEDED", result.Status)
		}
		if got := len(result.Invocations); got != 2 {
			t.Fatalf("invocations = %d, want 2", got)
		}
		assertLineOnce(t, rec, "caught", seen)
	})

	t.Run("Invoke", func(t *testing.T) {
		rec := newLineRecorder()
		var seen replayingAt

		handler := func(ctx durable.Context, _ string) (string, error) {
			_, err := durable.Invoke[string](ctx, "call", "target:$LATEST", "in")
			var invErr *durable.InvokeError
			if !errors.As(err, &invErr) {
				return "", err
			}
			seen.mark(ctx)
			ctx.Logger().Info("caught")
			return "failed", nil
		}

		runner := durabletest.NewLocalRunner(handler, durable.WithLogHandler(rec))
		if result := runner.RunUntilComplete(t, "event"); result.Status != durabletest.Pending {
			t.Fatalf("status = %s, want PENDING", result.Status)
		}
		if err := runner.FailChainedInvoke("call", "TargetError", "boom"); err != nil {
			t.Fatalf("fail invoke: %v", err)
		}
		if result := runner.RunUntilComplete(t, "event"); result.Status != durabletest.Succeeded {
			t.Fatalf("status = %s, want SUCCEEDED", result.Status)
		}
		assertLineOnce(t, rec, "caught", seen)
	})
}

// TestLinesAcrossSeveralReplayedOperations covers lines in the middle of a
// handler. Each line must be logged exactly once, in the invocation that
// first runs it.
//
//  1. "resumed" follows the first wait and precedes a new step. The second
//     invocation runs it first. Before the fix it was dropped, because the
//     context left replay only when it claimed the new step.
//  2. "after-next" follows that step.
//  3. "between" follows a wait whose successor, a second wait, is already
//     checkpointed in the third invocation. The second invocation runs it
//     first; the third invocation must suppress it.
func TestLinesAcrossSeveralReplayedOperations(t *testing.T) {
	rec := newLineRecorder()

	handler := func(ctx durable.Context, _ string) (string, error) {
		ctx.Logger().Info("start")
		if err := durable.Wait(ctx, "first", 5*time.Second); err != nil {
			return "", err
		}
		ctx.Logger().Info("resumed")
		if _, err := durable.Step(ctx, "next", func(durable.StepContext) (int, error) { return 1, nil }); err != nil {
			return "", err
		}
		ctx.Logger().Info("after-next")
		if err := durable.Wait(ctx, "second", 5*time.Second); err != nil {
			return "", err
		}
		ctx.Logger().Info("between")
		if err := durable.Wait(ctx, "third", 5*time.Second); err != nil {
			return "", err
		}
		ctx.Logger().Info("end")
		return "ok", nil
	}

	runner := durabletest.NewLocalRunner(handler, durable.WithLogHandler(rec))
	result := runner.RunUntilComplete(t, "event")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	if got := len(result.Invocations); got != 4 {
		t.Fatalf("invocations = %d, want 4", got)
	}
	want := "start\nresumed\nafter-next\nbetween\nend"
	if got := rec.messages(); got != want {
		t.Errorf("log =\n%s\nwant each line exactly once, in order:\n%s", got, want)
	}
}

// TestLineAfterReplayedOperationUnderEmit covers ReplayLogModeEmit. A
// replayed line is emitted with replay=true. The line after the replayed
// wait runs for the first time, so it must carry no replay attribute.
func TestLineAfterReplayedOperationUnderEmit(t *testing.T) {
	rec := newLineRecorder()

	handler := func(ctx durable.Context, _ string) (string, error) {
		ctx.Logger().Info("before")
		if err := durable.Wait(ctx, "pause", 5*time.Second); err != nil {
			return "", err
		}
		ctx.Logger().Info("after")
		return "ok", nil
	}

	runner := durabletest.NewLocalRunner(handler,
		durable.WithLogHandler(rec), durable.WithReplayLogMode(durable.ReplayLogModeEmit))
	if result := runner.RunUntilComplete(t, "event"); result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}

	before := rec.withMessage("before")
	if len(before) != 2 || before[0].replay || !before[1].replay {
		t.Errorf("\"before\" records = %+v, want a live record, then a replayed one", before)
	}
	after := rec.withMessage("after")
	if len(after) != 1 || after[0].replay {
		t.Errorf("\"after\" records = %+v, want one record without replay=true", after)
	}
}

// TestLineBetweenCreateCallbackAndResultStaysSuppressed guards the cases
// issue #103 excludes. The code between CreateCallback and Callback.Result
// ran before the previous suspension. So the context must stay in replay
// when CreateCallback returns, and the line there must be logged once, by
// the first invocation.
func TestLineBetweenCreateCallbackAndResultStaysSuppressed(t *testing.T) {
	rec := newLineRecorder()
	var callbackID string

	handler := func(ctx durable.Context, _ string) (string, error) {
		cb, err := durable.CreateCallback[string](ctx, "approval")
		if err != nil {
			return "", err
		}
		callbackID = cb.ID()
		ctx.Logger().Info("between")
		return cb.Result()
	}

	runner := durabletest.NewLocalRunner(handler, durable.WithLogHandler(rec))
	if result := runner.RunUntilComplete(t, "event"); result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", result.Status)
	}
	if err := runner.SendCallbackSuccess(callbackID, "approved"); err != nil {
		t.Fatalf("send callback: %v", err)
	}
	if result := runner.RunUntilComplete(t, "event"); result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	if got := rec.count("between"); got != 1 {
		t.Errorf("\"between\" logged %d times, want 1; log:\n%s", got, rec.messages())
	}
}

// TestPreClaimValidationErrorKeepsReplay guards a case where an operation
// returns without claiming an ID. RunInChildContext and Retry reject bad
// arguments before they claim one. Such a call awaits nothing, so the code
// after it ran before the previous suspension whenever the code before it
// did. Here that code sits between CreateCallback and Callback.Result. The
// next ID has no checkpoint there. So leaving replay after the rejected
// call would log the line a second time and report IsReplaying false.
func TestPreClaimValidationErrorKeepsReplay(t *testing.T) {
	cases := []struct {
		name   string
		reject func(ctx durable.Context) error
	}{
		{"RunInChildContext", func(ctx durable.Context) error {
			// The summary function takes an int, and the child returns a
			// string, so RunInChildContext rejects the option.
			_, err := durable.RunInChildContext(ctx, "child", func(durable.Context) (string, error) {
				return "", nil
			}, durable.WithChildSummary(func(int) string { return "" }))
			return err
		}},
		{"Retry", func(ctx durable.Context) error {
			_, err := durable.Retry[string](ctx, "group", nil, durable.NoRetry())
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := newLineRecorder()
			var callbackID string
			var seen replayingAt

			handler := func(ctx durable.Context, _ string) (string, error) {
				cb, err := durable.CreateCallback[string](ctx, "approval")
				if err != nil {
					return "", err
				}
				callbackID = cb.ID()
				if err := tc.reject(ctx); err == nil {
					return "", errors.New("validation error expected")
				}
				seen.mark(ctx)
				ctx.Logger().Info("between")
				return cb.Result()
			}

			runner := durabletest.NewLocalRunner(handler, durable.WithLogHandler(rec))
			if result := runner.RunUntilComplete(t, "event"); result.Status != durabletest.Pending {
				t.Fatalf("status = %s, want PENDING", result.Status)
			}
			if err := runner.SendCallbackSuccess(callbackID, "approved"); err != nil {
				t.Fatalf("send callback: %v", err)
			}
			if result := runner.RunUntilComplete(t, "event"); result.Status != durabletest.Succeeded {
				t.Fatalf("status = %s, want SUCCEEDED", result.Status)
			}
			if got := rec.count("between"); got != 1 {
				t.Errorf("\"between\" logged %d times, want 1; log:\n%s", got, rec.messages())
			}
			want := replayingAt{false, true}
			if len(seen) != len(want) || seen[0] != want[0] || seen[1] != want[1] {
				t.Errorf("IsReplaying() at the \"between\" line = %v, want %v", seen, want)
			}
		})
	}
}

// TestEmptyVirtualChildBetweenCreateCallbackAndResultKeepsReplay guards
// a virtual child context. A virtual child claims an ID but records no
// checkpoint, so its ID is absent from the checkpoint log on every
// invocation. Here the child sits between CreateCallback and
// Callback.Result, and its body runs no operation. So the child stays in
// replay, and the parent must stay in replay after it returns. The line
// after the child ran in the first invocation. So it must be logged once,
// and IsReplaying must report true on the second reach.
func TestEmptyVirtualChildBetweenCreateCallbackAndResultKeepsReplay(t *testing.T) {
	rec := newLineRecorder()
	var callbackID string
	var seen replayingAt

	handler := func(ctx durable.Context, _ string) (string, error) {
		cb, err := durable.CreateCallback[string](ctx, "approval")
		if err != nil {
			return "", err
		}
		callbackID = cb.ID()
		if _, err := durable.RunInChildContext(ctx, "group", func(durable.Context) (string, error) {
			return "grouped", nil
		}, durable.WithChildVirtual()); err != nil {
			return "", err
		}
		seen.mark(ctx)
		ctx.Logger().Info("between")
		return cb.Result()
	}

	runner := durabletest.NewLocalRunner(handler, durable.WithLogHandler(rec))
	if result := runner.RunUntilComplete(t, "event"); result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", result.Status)
	}
	if err := runner.SendCallbackSuccess(callbackID, "approved"); err != nil {
		t.Fatalf("send callback: %v", err)
	}
	if result := runner.RunUntilComplete(t, "event"); result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	if got := rec.count("between"); got != 1 {
		t.Errorf("\"between\" logged %d times, want 1; log:\n%s", got, rec.messages())
	}
	want := replayingAt{false, true}
	if len(seen) != len(want) || seen[0] != want[0] || seen[1] != want[1] {
		t.Errorf("IsReplaying() at the \"between\" line = %v, want %v", seen, want)
	}
}

// TestLineAfterVirtualChildThatReachesLiveRunsLive covers the other side
// of the virtual child rule. The virtual child holds a Wait. The first
// invocation suspends on the Wait. The second invocation replays the Wait,
// and the child leaves replay when the Wait returns, because no operation
// follows it in the child. So the parent must leave replay too, and the
// line after the child must be logged once, in the second invocation.
func TestLineAfterVirtualChildThatReachesLiveRunsLive(t *testing.T) {
	rec := newLineRecorder()
	var seen replayingAt

	handler := func(ctx durable.Context, _ string) (string, error) {
		if _, err := durable.RunInChildContext(ctx, "group", func(c durable.Context) (string, error) {
			if err := durable.Wait(c, "pause", time.Second); err != nil {
				return "", err
			}
			return "grouped", nil
		}, durable.WithChildVirtual()); err != nil {
			return "", err
		}
		seen.mark(ctx)
		ctx.Logger().Info("after-group")
		return "ok", nil
	}

	runner := durabletest.NewLocalRunner(handler, durable.WithLogHandler(rec))
	result := runner.RunUntilComplete(t, "event")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	if len(result.Invocations) != 2 {
		t.Fatalf("invocations = %d, want 2", len(result.Invocations))
	}
	assertLineOnce(t, rec, "after-group", seen)
}
