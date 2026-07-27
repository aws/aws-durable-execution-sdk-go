// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

type inlineEnvOut struct {
	OkResult   int
	BoomStatus string
	SkipStatus string
	SkipReason string
	Success    int
	Failure    int
	Skipped    int
	Total      int
	Reason     string
}

// TestDagE2E_InlineEnvelopeReplay is the inline-path counterpart to the
// large-payload re-execution guard. A SMALL three-task DAG (one success, one
// failure, one skip) fits under the checkpoint limit, so its container is
// checkpointed INLINE (full envelope with `tasks`, no ReplayChildren). A Wait
// AFTER the DAG suspends the invocation; on resume the now-terminal container
// is replayed through the envelope inline path — deserialize-and-return, with
// NO re-scheduling. This asserts task bodies run EXACTLY ONCE across that
// replay (the contract's "bodies run once" requirement) and that the
// reconstructed aggregate is faithful.
func TestDagE2E_InlineEnvelopeReplay(t *testing.T) {
	var okRuns, boomRuns, skipRuns int64
	var handlerEntries int64

	handler := func(ctx durable.Context, _ struct{}) (inlineEnvOut, error) {
		atomic.AddInt64(&handlerEntries, 1)

		res, err := durable.Dag(ctx, "orders", func(d *durable.DagBuilder) {
			durable.DagStep(d, "ok", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) {
				atomic.AddInt64(&okRuns, 1)
				return 7, nil
			})
			boom := durable.DagStep(d, "boom", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) {
				atomic.AddInt64(&boomRuns, 1)
				return 0, errors.New("card declined")
			}, durable.WithTaskRetry(func(_ error, _ int) durable.RetryDecision {
				return durable.RetryDecision{Retry: false}
			}))
			// AllSuccess (default) trigger: boom fails, so skipme is skipped
			// with TRIGGER_RULE and its body never runs.
			durable.DagStep(d, "skipme", []durable.AnyHandle{boom}, func(_ durable.Deps, _ durable.StepContext) (int, error) {
				atomic.AddInt64(&skipRuns, 1)
				return 1, nil
			})
		})
		if err != nil {
			return inlineEnvOut{}, err
		}

		// Suspend AFTER the DAG has completed so the next invocation replays
		// the terminal, inline container.
		if werr := durable.Wait(ctx, "settle", 2*time.Second); werr != nil {
			return inlineEnvOut{}, werr
		}

		// Everything below runs on the REPLAY, reading the inline envelope.
		okv, _ := durable.ResultByName[int](res, "ok")
		boomSt, _ := res.Status("boom")
		skipSt, _ := res.Status("skipme")
		return inlineEnvOut{
			OkResult:   okv,
			BoomStatus: string(boomSt),
			SkipStatus: string(skipSt),
			SkipReason: string(res.Results()["skipme"].SkipReason),
			Success:    res.SucceededCount(),
			Failure:    res.FailureCount(),
			Skipped:    res.SkippedCount(),
			Total:      res.TotalCount(),
			Reason:     string(res.CompletionReason()),
		}, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (%+v)", result.Status, result.Error)
	}
	o, err := durabletest.ResultAs[inlineEnvOut](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}

	// The container was actually replayed across a suspend.
	if got := atomic.LoadInt64(&handlerEntries); got < 2 {
		t.Fatalf("handler entered %d time(s); the inline container was not replayed", got)
	}

	// Aggregate faithfully reconstructed from the inline envelope.
	if o.OkResult != 7 {
		t.Fatalf("ok result after replay = %d, want 7", o.OkResult)
	}
	if o.BoomStatus != string(durable.StatusFailed) || o.SkipStatus != string(durable.StatusSkipped) {
		t.Fatalf("statuses after replay: boom=%q skip=%q", o.BoomStatus, o.SkipStatus)
	}
	if o.SkipReason != string(durable.SkipTriggerRule) {
		t.Fatalf("skip reason after replay = %q, want TRIGGER_RULE", o.SkipReason)
	}
	if o.Success != 1 || o.Failure != 1 || o.Skipped != 1 || o.Total != 3 {
		t.Fatalf("counts after replay: s=%d f=%d k=%d t=%d, want 1/1/1/3", o.Success, o.Failure, o.Skipped, o.Total)
	}
	if o.Reason != string(durable.CompletedWithFailures) {
		t.Fatalf("reason after replay = %q, want COMPLETED_WITH_FAILURES", o.Reason)
	}

	// Bodies ran exactly once across the replay; the skipped task never ran.
	if got := atomic.LoadInt64(&okRuns); got != 1 {
		t.Fatalf("ok body ran %d time(s) across replay, want 1", got)
	}
	if got := atomic.LoadInt64(&boomRuns); got != 1 {
		t.Fatalf("boom body ran %d time(s) across replay, want 1", got)
	}
	if got := atomic.LoadInt64(&skipRuns); got != 0 {
		t.Fatalf("skipped task body ran %d time(s), want 0", got)
	}

	// The container is checkpointed INLINE: full envelope with `tasks`, no
	// ReplayChildren (this is the observable proof the inline path was taken).
	scope := dagScopeOp(t, result, "orders")
	if scope.ContextDetails == nil {
		t.Fatal("orders scope op has no ContextDetails")
	}
	if scope.ContextDetails.ReplayChildren {
		t.Fatal("a small DAG must checkpoint inline, not with ReplayChildren")
	}
	if !strings.Contains(scope.ContextDetails.Result, `"tasks"`) {
		t.Fatalf("inline container must carry the per-task `tasks` array, got: %s", scope.ContextDetails.Result)
	}
}
