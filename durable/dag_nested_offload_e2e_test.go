// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// These are the local-runner guards for the nested-offload contract and
// conformance scenario 10-17 (DagNestedLargePayload): the untested
// intersection of nesting and large payloads.
//
// A nested DAG `inner` (six step tasks p1..p6, each returning a distinct
// letter repeated nlpReps times) has a ~307KB aggregate, over the 256KB
// checkpoint limit, so the inner container is offloaded (its `tasks` array is
// dropped and ReplayChildren is set, the per-task results living in the
// retained child step checkpoints). The outer DAG `outernested` reads the
// inner DagResult through a dependency in two step tasks straddling a wait:
// `digestBefore` (checkpointed, computed before the suspend) and
// `digestAfter` (computed on the resume from the RECONSTRUCTED inner result).
//
// Because Go's DAG model is flat, the outer's reconstruct path re-runs its
// scheduler on resume; the `inner` SubDag task re-enters Dag(), which restores
// the offloaded inner from its own envelope while each inner step fast-paths
// from its own child checkpoint (rule 2 — recursion into the inner
// container's children). digestBefore == digestAfter proves the inner
// per-task detail survived the offload of the inner container across replay.

var nlpInner = []string{"p1", "p2", "p3", "p4", "p5", "p6"}

const (
	// nlpReps is the per-inner-task repetition count. 6 * 51200 = 307200
	// bytes of inner aggregate, over the 256KB threshold; each ~50KB task
	// result stays under it.
	nlpReps = 51200
	// nlpExpectedDigest is the language-neutral fingerprint of the inner
	// aggregate: "<taskCount>:<totalLength>:<firstCharOfEachTaskInOrder>".
	nlpExpectedDigest = "6:307200:abcdef"
)

// nlpDigest recomputes the digest of the inner DagResult, reading per-task
// results in registration order.
func nlpDigest(inner *durable.DagResult) string {
	total := 0
	var firsts strings.Builder
	for _, n := range nlpInner {
		v, err := durable.ResultByName[string](inner, n)
		if err != nil {
			continue
		}
		total += len(v)
		if len(v) > 0 {
			firsts.WriteByte(v[0])
		}
	}
	return fmt.Sprintf("%d:%d:%s", len(nlpInner), total, firsts.String())
}

type nlpOut struct {
	Reason       string
	InnerReason  string
	InnerCounts  [4]int // [total, failed, skipped, succeeded]
	DigestBefore string
	DigestAfter  string
	Match        bool
}

// newNestedLargePayloadHandler builds the 10-17 graph. innerBodyRuns[i] is
// incremented each time inner task p<i+1>'s BODY runs; handlerEntries counts
// handler invocations. Both let the caller prove the offload/replay happened
// and that no inner body re-ran across it (nesting doubles the number of
// containers that replay).
func newNestedLargePayloadHandler(innerBodyRuns *[6]int64, handlerEntries *int64) func(durable.Context, struct{}) (nlpOut, error) {
	return func(ctx durable.Context, _ struct{}) (nlpOut, error) {
		atomic.AddInt64(handlerEntries, 1)

		res, err := durable.Dag(ctx, "outernested", func(d *durable.DagBuilder) {
			inner := durable.SubDag(d, "inner", nil, func(sub *durable.DagBuilder) {
				for i, name := range nlpInner {
					idx := i
					val := strings.Repeat(string(rune('a'+i)), nlpReps)
					durable.DagStep(sub, name, nil,
						func(_ durable.Deps, _ durable.StepContext) (string, error) {
							atomic.AddInt64(&innerBodyRuns[idx], 1)
							return val, nil
						})
				}
			}, durable.WithDagMaxConcurrency(1))

			before := durable.DagStep(d, "digestBefore", []durable.AnyHandle{inner},
				func(dp durable.Deps, _ durable.StepContext) (string, error) {
					ir, _ := durable.Get(dp, inner)
					return nlpDigest(ir), nil
				})

			settle := durable.DagWait(d, "settle", []durable.AnyHandle{before}, 2*time.Second)

			durable.DagStep(d, "digestAfter", []durable.AnyHandle{inner, settle},
				func(dp durable.Deps, _ durable.StepContext) (string, error) {
					ir, _ := durable.Get(dp, inner)
					return nlpDigest(ir), nil
				})
		}, durable.WithDagMaxConcurrency(1))
		if err != nil {
			return nlpOut{}, err
		}
		if e := res.ThrowIfError(); e != nil {
			return nlpOut{}, e
		}

		inner, err := durable.ResultByName[*durable.DagResult](res, "inner")
		if err != nil {
			return nlpOut{}, err
		}
		before, _ := durable.ResultByName[string](res, "digestBefore")
		after, _ := durable.ResultByName[string](res, "digestAfter")

		return nlpOut{
			Reason:      string(res.CompletionReason()),
			InnerReason: string(inner.CompletionReason()),
			InnerCounts: [4]int{
				inner.TotalCount(), inner.FailureCount(), inner.SkippedCount(), inner.SucceededCount(),
			},
			DigestBefore: before,
			DigestAfter:  after,
			Match:        before == after,
		}, nil
	}
}

// TestDagE2E_NestedLargePayloadReconstruct is the rule-2 guard: after the
// outer's reconstruct path runs across a suspend, the inner DagResult reports
// the correct counts and reason AND its per-task detail is present (proven by
// the digest recomputed after replay matching the one computed before it).
func TestDagE2E_NestedLargePayloadReconstruct(t *testing.T) {
	var innerBodyRuns [6]int64
	var handlerEntries int64

	runner := durabletest.NewLocalRunner(newNestedLargePayloadHandler(&innerBodyRuns, &handlerEntries))
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (%+v)", result.Status, result.Error)
	}
	o, err := durabletest.ResultAs[nlpOut](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}

	// The container was actually replayed across a suspend.
	if got := atomic.LoadInt64(&handlerEntries); got < 2 {
		t.Fatalf("handler entered %d time(s); no replay occurred across the suspend", got)
	}

	// Outer and inner outcomes.
	if o.Reason != string(durable.AllCompleted) {
		t.Fatalf("outer reason = %q, want ALL_COMPLETED", o.Reason)
	}
	if o.InnerReason != string(durable.AllCompleted) {
		t.Fatalf("inner reason = %q, want ALL_COMPLETED", o.InnerReason)
	}
	if o.InnerCounts != [4]int{6, 0, 0, 6} {
		t.Fatalf("inner counts = %v, want [6 0 0 6] (total,failed,skipped,succeeded)", o.InnerCounts)
	}

	// Rule 2: the inner per-task detail survived the offload of the inner
	// container across the outer's reconstruct replay.
	if o.DigestBefore != nlpExpectedDigest || o.DigestAfter != nlpExpectedDigest {
		t.Fatalf("digest mismatch: before=%q after=%q want %q", o.DigestBefore, o.DigestAfter, nlpExpectedDigest)
	}
	if !o.Match {
		t.Fatal("digestBefore != digestAfter: inner per-task detail did not survive the nested offload/replay")
	}

	// Sanity: the inner aggregate genuinely exceeds the 256KB threshold.
	if len(nlpInner)*nlpReps <= 256*1024 {
		t.Fatalf("test misconfigured: inner aggregate %d bytes is not over 256KB", len(nlpInner)*nlpReps)
	}
}

// TestDagE2E_NestedLargePayloadBodiesRunOnce asserts each inner task body runs
// exactly once across the offloaded replay. Nesting doubles the number of
// containers that replay (outer + inner both re-run), so a body that re-ran
// would double a customer side effect — the exact bug this guard catches.
func TestDagE2E_NestedLargePayloadBodiesRunOnce(t *testing.T) {
	var innerBodyRuns [6]int64
	var handlerEntries int64

	runner := durabletest.NewLocalRunner(newNestedLargePayloadHandler(&innerBodyRuns, &handlerEntries))
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (%+v)", result.Status, result.Error)
	}
	if got := atomic.LoadInt64(&handlerEntries); got < 2 {
		t.Fatalf("handler entered %d time(s); no replay occurred, so this guard would be vacuous", got)
	}
	for i := range innerBodyRuns {
		if got := atomic.LoadInt64(&innerBodyRuns[i]); got != 1 {
			t.Fatalf("inner task %s body ran %d time(s) across the nested offload + replay, want exactly 1",
				nlpInner[i], got)
		}
	}
}
