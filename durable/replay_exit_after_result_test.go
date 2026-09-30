// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// The tests in this file cover the second half of GitHub issue #103: code
// that runs after Future.Result, Callback.Result, a combinator, or a batch.
//
// The rule they check is "a log line is never suppressed on its first
// run". Where an asynchronous operation created earlier is still being
// awaited, the SDK cannot tell whether an earlier invocation already ran
// the code, and it writes the line again rather than risk losing it. The
// counts below state those duplicates exactly, so a change that turns a
// duplicate into a lost line fails, and so does a change that adds a
// duplicate elsewhere.

// runToSuccess drives the execution to SUCCEEDED. Whenever it blocks on a
// callback, it completes every open callback with "ok". So each callback is
// completed only after the invocation that awaits it has suspended.
func runToSuccess(t *testing.T, runner *durabletest.LocalRunner[string, string]) {
	t.Helper()
	for range 5 {
		result := runner.RunUntilComplete(t, "event")
		switch result.Status {
		case durabletest.Succeeded:
			return
		case durabletest.Pending:
			callbacks := runner.OpenCallbacks()
			if len(callbacks) == 0 {
				t.Fatalf("execution is PENDING with no open callback")
			}
			for _, cb := range callbacks {
				if err := runner.SendCallbackSuccess(cb.CallbackID, "ok"); err != nil {
					t.Fatalf("send callback: %v", err)
				}
			}
		default:
			t.Fatalf("status = %s, want SUCCEEDED or PENDING", result.Status)
		}
	}
	t.Fatalf("execution did not succeed after 5 rounds")
}

// step runs a step that returns 1.
func step(ctx durable.Context, name string) error {
	_, err := durable.Step(ctx, name, func(durable.StepContext) (int, error) { return 1, nil })
	return err
}

// waitBranch is a branch that waits one second.
func waitBranch(name string) durable.Branch[int] {
	return durable.Branch[int]{Name: name, Func: func(c durable.Context) (int, error) {
		return 1, durable.Wait(c, "w", time.Second)
	}}
}

func TestLinesAfterAwaitAreNeverDropped(t *testing.T) {
	tests := []struct {
		name    string
		handler durable.Handler[string, string]
		// want is the number of times each line must be logged over the
		// whole execution.
		want map[string]int
	}{
		{
			// The line after Future.Result runs first in the second
			// invocation: the wait was awaited in the first one, so no
			// earlier invocation got past w.Result. The callback created
			// before it is still unread, so the context cannot tell that
			// "between" is new, and the third invocation writes it again.
			// "after" follows the last Result and is written once.
			name: "Future.Result with a later callback outstanding",
			handler: func(ctx durable.Context, _ string) (string, error) {
				w := durable.WaitAsync(ctx, "w", time.Second)
				cb, err := durable.CreateCallback[string](ctx, "cb")
				if err != nil {
					return "", err
				}
				if _, err := w.Result(); err != nil {
					return "", err
				}
				ctx.Logger().Info("between")
				if _, err := cb.Result(); err != nil {
					return "", err
				}
				ctx.Logger().Info("after")
				return "ok", nil
			},
			want: map[string]int{"between": 2, "after": 1},
		},
		{
			// The first invocation runs "between" and suspends at
			// cb.Result. The second one replays the step. The next ID has
			// no checkpoint, because the callback's ID was claimed before
			// the step, so "between" is written again. "after" runs first in
			// the second invocation and is written once.
			name: "Callback.Result after a step",
			handler: func(ctx durable.Context, _ string) (string, error) {
				cb, err := durable.CreateCallback[string](ctx, "cb")
				if err != nil {
					return "", err
				}
				if err := step(ctx, "s"); err != nil {
					return "", err
				}
				ctx.Logger().Info("between")
				if _, err := cb.Result(); err != nil {
					return "", err
				}
				ctx.Logger().Info("after")
				return "ok", nil
			},
			want: map[string]int{"between": 2, "after": 1},
		},
		{
			name: "Callback.Result after a wait",
			handler: func(ctx durable.Context, _ string) (string, error) {
				cb, err := durable.CreateCallback[string](ctx, "cb")
				if err != nil {
					return "", err
				}
				if err := durable.Wait(ctx, "w", time.Second); err != nil {
					return "", err
				}
				ctx.Logger().Info("between")
				if _, err := cb.Result(); err != nil {
					return "", err
				}
				ctx.Logger().Info("after")
				return "ok", nil
			},
			want: map[string]int{"between": 2, "after": 1},
		},
		{
			name: "All",
			handler: func(ctx durable.Context, _ string) (string, error) {
				a := durable.WaitAsync(ctx, "a", time.Second)
				b := durable.WaitAsync(ctx, "b", time.Second)
				if _, err := durable.All(ctx, "all", []*durable.Future[durable.Void]{a, b}); err != nil {
					return "", err
				}
				ctx.Logger().Info("after")
				return "ok", nil
			},
			want: map[string]int{"after": 1},
		},
		{
			name: "AllSettled",
			handler: func(ctx durable.Context, _ string) (string, error) {
				a := durable.WaitAsync(ctx, "a", time.Second)
				if _, err := durable.AllSettled(ctx, "settled", []*durable.Future[durable.Void]{a}); err != nil {
					return "", err
				}
				ctx.Logger().Info("after")
				return "ok", nil
			},
			want: map[string]int{"after": 1},
		},
		{
			name: "Any",
			handler: func(ctx durable.Context, _ string) (string, error) {
				a := durable.WaitAsync(ctx, "a", time.Second)
				if _, err := durable.Any(ctx, "any", []*durable.Future[durable.Void]{a}); err != nil {
					return "", err
				}
				ctx.Logger().Info("after")
				return "ok", nil
			},
			want: map[string]int{"after": 1},
		},
		{
			name: "Race",
			handler: func(ctx durable.Context, _ string) (string, error) {
				a := durable.WaitAsync(ctx, "a", time.Second)
				if _, err := durable.Race(ctx, "race", []*durable.Future[durable.Void]{a}); err != nil {
					return "", err
				}
				ctx.Logger().Info("after")
				return "ok", nil
			},
			want: map[string]int{"after": 1},
		},
		{
			name: "Join",
			handler: func(ctx durable.Context, _ string) (string, error) {
				a := durable.WaitAsync(ctx, "a", time.Second)
				if err := durable.Join(ctx, "join", []durable.Awaitable{a}); err != nil {
					return "", err
				}
				ctx.Logger().Info("after")
				return "ok", nil
			},
			want: map[string]int{"after": 1},
		},
		{
			name: "Select",
			handler: func(ctx durable.Context, _ string) (string, error) {
				if _, _, err := durable.Select(ctx, "select", []durable.Branch[int]{waitBranch("x")}); err != nil {
					return "", err
				}
				ctx.Logger().Info("after")
				return "ok", nil
			},
			want: map[string]int{"after": 1},
		},
		{
			name: "Map",
			handler: func(ctx durable.Context, _ string) (string, error) {
				_, err := durable.Map(ctx, "map", []int{1, 2}, func(c durable.Context, _ int, _ int) (int, error) {
					return 1, durable.Wait(c, "w", time.Second)
				})
				if err != nil {
					return "", err
				}
				ctx.Logger().Info("after")
				return "ok", nil
			},
			want: map[string]int{"after": 1},
		},
		{
			name: "Parallel",
			handler: func(ctx durable.Context, _ string) (string, error) {
				if _, err := durable.Parallel(ctx, "parallel", []durable.Branch[int]{waitBranch("x"), waitBranch("y")}); err != nil {
					return "", err
				}
				ctx.Logger().Info("after")
				return "ok", nil
			},
			want: map[string]int{"after": 1},
		},
		{
			name: "WaitForCallback",
			handler: func(ctx durable.Context, _ string) (string, error) {
				_, err := durable.WaitForCallback[string](ctx, "approval", func(durable.StepContext, string) error { return nil })
				if err != nil {
					return "", err
				}
				ctx.Logger().Info("after")
				return "ok", nil
			},
			want: map[string]int{"after": 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := newLineRecorder()
			runToSuccess(t, durabletest.NewLocalRunner(tt.handler, durable.WithLogHandler(rec)))
			for msg, want := range tt.want {
				if got := rec.count(msg); got != want {
					t.Errorf("%q logged %d times, want %d; log:\n%s", msg, got, want, rec.messages())
				}
			}
		})
	}
}
