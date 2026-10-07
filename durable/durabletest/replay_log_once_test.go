// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestLogLineAfterFinalWait is the reproduction of a line after the last
// wait of a handler. The line runs for the first time in the second
// invocation, so it is logged there.
func TestLogLineAfterFinalWait(t *testing.T) {
	var logs bytes.Buffer
	var replaying []bool

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
		replaying = append(replaying, ctx.IsReplaying())
		ctx.Logger().Info("done")
		return "ok", nil
	}

	runner := durabletest.NewLocalRunner(handler,
		durable.WithLogHandler(slog.NewJSONHandler(&logs, nil)))
	result, err := runner.RunUntilComplete("event")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("status %s, invocations %d, IsReplaying() at the line %v",
		result.Status, len(result.Invocations), replaying)
	if !strings.Contains(logs.String(), `"msg":"done"`) {
		t.Errorf("no record with msg done; log output %q", logs.String())
	}
}

// lineRecorder records, per line name, the IsReplaying value of every run
// of the line. A line is one call of mark.
type lineRecorder struct {
	mu        sync.Mutex
	replaying map[string][]bool
}

// mark runs the line name on ctx: it records ctx.IsReplaying and logs a
// record whose message is name.
func (r *lineRecorder) mark(ctx durable.Context, name string) {
	r.mu.Lock()
	if r.replaying == nil {
		r.replaying = make(map[string][]bool)
	}
	r.replaying[name] = append(r.replaying[name], ctx.IsReplaying())
	r.mu.Unlock()
	ctx.Logger().Info(name)
}

// liveRuns returns how many runs of the line name saw IsReplaying false.
func (r *lineRecorder) liveRuns(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, replaying := range r.replaying[name] {
		if !replaying {
			n++
		}
	}
	return n
}

// replayLogCase is a handler whose lines each run once over the whole
// execution, and how to drive it.
type replayLogCase struct {
	name string

	// lines are the names of the lines the handler marks.
	lines []string

	// handler builds the handler body. It marks every line with rec.
	handler func(rec *lineRecorder) func(durable.Context, string) (string, error)

	// invokes are the names of chained invokes the driver completes
	// while the execution is suspended.
	invokes []string
}

// lineCounts holds, per message, how many records carry it, and how many
// of those carry no replay attribute.
type lineCounts struct {
	all  map[string]int
	live map[string]int
}

// countLines parses JSON log output.
func countLines(t *testing.T, out []byte) lineCounts {
	t.Helper()
	c := lineCounts{all: map[string]int{}, live: map[string]int{}}
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("decode log record %q: %v", sc.Text(), err)
		}
		msg, _ := rec["msg"].(string)
		c.all[msg]++
		if _, ok := rec["replay"]; !ok {
			c.live[msg]++
		}
	}
	return c
}

// driveToCompletion runs the execution until it ends. While it is
// suspended, it completes every open callback and the chained invokes
// named in invokes, one invocation's worth at a time.
func driveToCompletion(t *testing.T, runner *durabletest.LocalRunner[string, string], invokes []string) *durabletest.TestResult {
	t.Helper()
	pendingInvokes := append([]string(nil), invokes...)
	for range 20 {
		result, err := runner.RunUntilComplete("event")
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != durabletest.Pending {
			return result
		}
		progressed := false
		for _, cb := range runner.OpenCallbacks() {
			if err := runner.SendCallbackSuccess(cb.CallbackID, "ok"); err != nil {
				t.Fatal(err)
			}
			progressed = true
		}
		if !progressed && len(pendingInvokes) > 0 {
			if err := runner.CompleteChainedInvoke(pendingInvokes[0], "done"); err != nil {
				t.Fatal(err)
			}
			pendingInvokes = pendingInvokes[1:]
			progressed = true
		}
		if !progressed {
			t.Fatalf("execution is pending with nothing to complete")
		}
	}
	t.Fatalf("execution did not complete")
	return nil
}

// runReplayLogCase runs c under mode and checks every line of c: it is
// logged exactly once without the replay attribute, IsReplaying is false
// on exactly one of its runs, and under ReplayLogModeSuppress no record
// of it carries the replay attribute.
func runReplayLogCase(t *testing.T, c replayLogCase, mode durable.ReplayLogMode) {
	t.Helper()
	var logs bytes.Buffer
	rec := &lineRecorder{}
	runner := durabletest.NewLocalRunner(c.handler(rec),
		durable.WithLogHandler(slog.NewJSONHandler(&logs, nil)),
		durable.WithReplayLogMode(mode))
	result := driveToCompletion(t, runner, c.invokes)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, error %+v, want SUCCEEDED", result.Status, result.Error)
	}
	if len(result.Invocations) < 2 {
		t.Fatalf("invocations = %d, want at least 2 so that the handler replays", len(result.Invocations))
	}
	counts := countLines(t, logs.Bytes())
	for _, line := range c.lines {
		if got := counts.live[line]; got != 1 {
			t.Errorf("line %q: %d records without replay, want 1; all records %d", line, got, counts.all[line])
		}
		if mode == durable.ReplayLogModeSuppress && counts.all[line] != 1 {
			t.Errorf("line %q: %d records under ReplayLogModeSuppress, want 1", line, counts.all[line])
		}
		if got := rec.liveRuns(line); got != 1 {
			t.Errorf("line %q: IsReplaying false on %d runs, want 1; runs %v", line, got, rec.replaying[line])
		}
	}
}

// fetch runs the step every case starts with, so that the next invocation
// starts replaying.
func fetch(ctx durable.Context) error {
	_, err := durable.Step(ctx, "fetch", func(durable.StepContext) (string, error) { return "x", nil })
	return err
}

// waits returns n wait futures of 5 seconds on ctx.
func waits(ctx durable.Context, n int) []*durable.Future[durable.Void] {
	fs := make([]*durable.Future[durable.Void], n)
	for i := range fs {
		fs[i] = durable.WaitAsync(ctx, "", 5*time.Second)
	}
	return fs
}

// lineAfter builds a case that runs op after fetch and then the line
// "after" as the last line of the handler.
func lineAfter(name string, op func(ctx durable.Context) error, invokes ...string) replayLogCase {
	return replayLogCase{
		name:    name,
		lines:   []string{"after"},
		invokes: invokes,
		handler: func(rec *lineRecorder) func(durable.Context, string) (string, error) {
			return func(ctx durable.Context, _ string) (string, error) {
				if err := fetch(ctx); err != nil {
					return "", err
				}
				if err := op(ctx); err != nil {
					return "", err
				}
				rec.mark(ctx, "after")
				return "ok", nil
			}
		},
	}
}

func replayLogCases() []replayLogCase {
	return []replayLogCase{
		lineAfter("Wait", func(ctx durable.Context) error {
			return durable.Wait(ctx, "pause", 5*time.Second)
		}),
		lineAfter("Callback.Result", func(ctx durable.Context) error {
			cb, err := durable.CreateCallback[string](ctx, "approval")
			if err != nil {
				return err
			}
			_, err = cb.Result(ctx)
			return err
		}),
		lineAfter("WaitForCallback", func(ctx durable.Context) error {
			_, err := durable.WaitForCallback[string](ctx, "approval",
				func(durable.StepContext, string) error { return nil })
			return err
		}),
		lineAfter("Invoke", func(ctx durable.Context) error {
			_, err := durable.Invoke[string](ctx, "charge", "payments:$LATEST", "order")
			return err
		}, "charge"),
		lineAfter("Future.Result", func(ctx durable.Context) error {
			_, err := durable.WaitAsync(ctx, "pause", 5*time.Second).Result(ctx)
			return err
		}),
		lineAfter("All", func(ctx durable.Context) error {
			_, err := durable.All(ctx, "all", waits(ctx, 2))
			return err
		}),
		lineAfter("AllSettled", func(ctx durable.Context) error {
			_, err := durable.AllSettled(ctx, "all-settled", waits(ctx, 2))
			return err
		}),
		lineAfter("Any", func(ctx durable.Context) error {
			_, err := durable.Any(ctx, "any", waits(ctx, 2))
			return err
		}),
		lineAfter("Race", func(ctx durable.Context) error {
			_, err := durable.Race(ctx, "race", waits(ctx, 2))
			return err
		}),
		lineAfter("Join", func(ctx durable.Context) error {
			// Futures of two result types. Both wait, so the first
			// invocation suspends.
			branch := durable.Go(ctx, "branch", func(c durable.Context) (int, error) {
				return 1, durable.Wait(c, "branch-pause", 5*time.Second)
			})
			wait := durable.WaitAsync(ctx, "pause", 5*time.Second)
			return durable.Join(ctx, "join", []durable.Awaitable{branch, wait})
		}),
		lineAfter("Select", func(ctx durable.Context) error {
			pause := func(c durable.Context) (int, error) { return 1, durable.Wait(c, "pause", 5*time.Second) }
			_, _, err := durable.Select(ctx, "select", []durable.Branch[int]{
				{Name: "a", Func: pause},
				{Name: "b", Func: pause},
			})
			return err
		}),
		lineAfter("Map", func(ctx durable.Context) error {
			_, err := durable.Map(ctx, "map", []int{1, 2}, func(c durable.Context, item, _ int) (int, error) {
				return item, durable.Wait(c, "pause", 5*time.Second)
			})
			return err
		}),
		lineAfter("Parallel", func(ctx durable.Context) error {
			pause := func(c durable.Context) (int, error) { return 1, durable.Wait(c, "pause", 5*time.Second) }
			_, err := durable.Parallel(ctx, "parallel", []durable.Branch[int]{
				{Name: "a", Func: pause},
				{Name: "b", Func: pause},
			})
			return err
		}),
		lineAfter("RunInChildContext", func(ctx durable.Context) error {
			_, err := durable.RunInChildContext(ctx, "child", func(c durable.Context) (int, error) {
				return 1, durable.Wait(c, "pause", 5*time.Second)
			})
			return err
		}),
		lineAfter("virtual RunInChildContext", func(ctx durable.Context) error {
			_, err := durable.RunInChildContext(ctx, "child", func(c durable.Context) (int, error) {
				return 1, durable.Wait(c, "pause", 5*time.Second)
			}, durable.WithChildVirtual())
			return err
		}),
		lineAfter("virtual Go", func(ctx durable.Context) error {
			_, err := durable.Go(ctx, "child", func(c durable.Context) (int, error) {
				return 1, durable.Wait(c, "pause", 5*time.Second)
			}, durable.WithChildVirtual()).Result(ctx)
			return err
		}),
		lineAfter("WaitForCondition", func(ctx durable.Context) error {
			_, err := durable.WaitForCondition(ctx, "poll",
				func(_ durable.StepContext, n int) (int, error) { return n + 1, nil },
				durable.ConditionConfig[int]{
					WaitStrategy: func(n int, _ int) durable.WaitDecision {
						return durable.WaitDecision{Continue: n < 2, Delay: time.Second}
					},
				})
			return err
		}),
		lineAfter("Retry", func(ctx durable.Context) error {
			_, err := durable.Retry(ctx, "retry", func(c durable.Context, attempt int) (int, error) {
				return durable.Step(c, "try", func(durable.StepContext) (int, error) {
					if attempt < 2 {
						return 0, errors.New("not yet")
					}
					return attempt, nil
				}, durable.WithRetry(durable.NoRetry()))
			}, durable.ExponentialBackoff())
			return err
		}),
		{
			// A line in the middle of the handler, after a replayed wait
			// and before a new step, and a line after that step.
			name:  "resumed and after-next",
			lines: []string{"resumed", "after-next"},
			handler: func(rec *lineRecorder) func(durable.Context, string) (string, error) {
				return func(ctx durable.Context, _ string) (string, error) {
					if err := fetch(ctx); err != nil {
						return "", err
					}
					if err := durable.Wait(ctx, "pause", 5*time.Second); err != nil {
						return "", err
					}
					rec.mark(ctx, "resumed")
					if _, err := durable.Step(ctx, "next", func(durable.StepContext) (int, error) { return 1, nil }); err != nil {
						return "", err
					}
					rec.mark(ctx, "after-next")
					return "ok", nil
				}
			},
		},
		{
			// The first invocation suspends on a wait after the line, with
			// the callback still open.
			name:  "between CreateCallback and Result, suspended on a later wait",
			lines: []string{"between", "after-wait", "after"},
			handler: func(rec *lineRecorder) func(durable.Context, string) (string, error) {
				return func(ctx durable.Context, _ string) (string, error) {
					if err := fetch(ctx); err != nil {
						return "", err
					}
					cb, err := durable.CreateCallback[string](ctx, "approval")
					if err != nil {
						return "", err
					}
					rec.mark(ctx, "between")
					if err := durable.Wait(ctx, "pause", 5*time.Second); err != nil {
						return "", err
					}
					rec.mark(ctx, "after-wait")
					if _, err := cb.Result(ctx); err != nil {
						return "", err
					}
					rec.mark(ctx, "after")
					return "ok", nil
				}
			},
		},
		{
			// The first invocation suspends at the Result after the line.
			name:  "between CreateCallback and Result, suspended at Result",
			lines: []string{"between", "after"},
			handler: func(rec *lineRecorder) func(durable.Context, string) (string, error) {
				return func(ctx durable.Context, _ string) (string, error) {
					if err := fetch(ctx); err != nil {
						return "", err
					}
					cb, err := durable.CreateCallback[string](ctx, "approval")
					if err != nil {
						return "", err
					}
					rec.mark(ctx, "between")
					if _, err := cb.Result(ctx); err != nil {
						return "", err
					}
					rec.mark(ctx, "after")
					return "ok", nil
				}
			},
		},
		{
			name:  "between WaitAsync and Result, suspended at Result",
			lines: []string{"between", "after"},
			handler: func(rec *lineRecorder) func(durable.Context, string) (string, error) {
				return func(ctx durable.Context, _ string) (string, error) {
					if err := fetch(ctx); err != nil {
						return "", err
					}
					wait := durable.WaitAsync(ctx, "pause", 5*time.Second)
					rec.mark(ctx, "between")
					if _, err := wait.Result(ctx); err != nil {
						return "", err
					}
					rec.mark(ctx, "after")
					return "ok", nil
				}
			},
		},
		{
			// A Go branch reads a future its parent created, then its own
			// future. The branch runs a step first, so it replays on the
			// next invocation.
			name:  "Go branch reads parent and own futures",
			lines: []string{"branch-after-parent", "branch-after-own", "after"},
			handler: func(rec *lineRecorder) func(durable.Context, string) (string, error) {
				return func(ctx durable.Context, _ string) (string, error) {
					if err := fetch(ctx); err != nil {
						return "", err
					}
					parentWait := durable.WaitAsync(ctx, "parent-pause", 5*time.Second)
					branch := durable.Go(ctx, "branch", func(c durable.Context) (int, error) {
						if _, err := durable.Step(c, "pre", func(durable.StepContext) (int, error) { return 1, nil }); err != nil {
							return 0, err
						}
						if _, err := parentWait.Result(c); err != nil {
							return 0, err
						}
						rec.mark(c, "branch-after-parent")
						if _, err := durable.WaitAsync(c, "own-pause", 5*time.Second).Result(c); err != nil {
							return 0, err
						}
						rec.mark(c, "branch-after-own")
						return 1, nil
					})
					if _, err := branch.Result(ctx); err != nil {
						return "", err
					}
					rec.mark(ctx, "after")
					return "ok", nil
				}
			},
		},
	}
}

// TestReplayLogLinesLoggedOnce runs every case under both replay log modes.
// Every line is logged exactly once over the whole execution, in the
// invocation that first runs it, and IsReplaying is false there.
func TestReplayLogLinesLoggedOnce(t *testing.T) {
	for _, c := range replayLogCases() {
		t.Run(c.name, func(t *testing.T) {
			t.Run("suppress", func(t *testing.T) { runReplayLogCase(t, c, durable.ReplayLogModeSuppress) })
			t.Run("emit", func(t *testing.T) { runReplayLogCase(t, c, durable.ReplayLogModeEmit) })
		})
	}
}
