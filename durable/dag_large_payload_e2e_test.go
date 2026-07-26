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

// These are the local-runner guards for conformance scenario 10-15
// (DagLargePayload) and the large-payload offload/replay contract. A DAG
// whose AGGREGATE result exceeds the 256KB checkpoint limit is offloaded;
// Go re-executes the DAG child body via ReplayChildren on replay (no
// DagSummary envelope — that is the TypeScript-only strategy). The property
// under test is that the >256KB aggregate survives the offload and the
// subsequent container replay byte-for-byte, and that no task body is
// re-invoked across it.
//
// The graph mirrors the compliance handler exactly: eight independent root
// step tasks p1..p8, each returning lpReps repetitions of its own letter, so
// the aggregate is 8 * 51200 = 409600 bytes (~400KB), over the threshold,
// while each individual ~50KB result stays under it.

const (
	// lpReps is the per-task repetition count (matches the compliance
	// handler). 8 * 51200 = 409600 bytes of aggregate.
	lpReps = 51200
	// lpAggregate is the expected total aggregate length across all tasks.
	lpAggregate = 8 * lpReps
	// lpExpectedDigest is the language-neutral fingerprint of the aggregate:
	// "<taskCount>:<totalLength>:<firstCharOfEachTaskInOrder>".
	lpExpectedDigest = "8:409600:abcdefgh"
)

var lpTaskNames = []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"}

// lpExpectedValue is the full result string task p<index+1> must return:
// letter ('a'+index) repeated lpReps times.
func lpExpectedValue(index int) string {
	return strings.Repeat(string(rune('a'+index)), lpReps)
}

// lpDigest recomputes the language-neutral digest from a DagResult, reading
// per-task results in registration order.
func lpDigest(res *durable.DagResult) string {
	total := 0
	var firsts strings.Builder
	for _, name := range lpTaskNames {
		v, err := durable.ResultByName[string](res, name)
		if err != nil {
			continue
		}
		total += len(v)
		if len(v) > 0 {
			firsts.WriteByte(v[0])
		}
	}
	return fmt.Sprintf("%d:%d:%s", len(lpTaskNames), total, firsts.String())
}

// lpOut is the small projection returned by the large-payload test handler.
// It never carries the full aggregate; it carries the two digests, one full
// per-task value for a byte-level fidelity check, and the per-task lengths
// as reconstructed AFTER the replay.
type lpOut struct {
	Reason           string
	Success          int
	DigestBefore     string
	DigestAfter      string
	Match            bool
	FullP1           string
	Lengths          []int
	AllByteIdentical bool
}

// newLargePayloadHandler builds the 10-15 handler. bodyRuns[i] is incremented
// (atomically) each time task p<i+1>'s BODY actually executes; handlerEntries
// counts handler invocations. Both let the caller prove the offload/replay
// happened and that no body re-ran across it.
func newLargePayloadHandler(bodyRuns *[8]int64, handlerEntries *int64) func(durable.Context, struct{}) (lpOut, error) {
	return func(ctx durable.Context, _ struct{}) (lpOut, error) {
		atomic.AddInt64(handlerEntries, 1)

		res, err := durable.Dag(ctx, "bigdag", func(d *durable.DagBuilder) {
			for i, name := range lpTaskNames {
				idx := i
				val := lpExpectedValue(i)
				durable.DagStep(d, name, nil,
					func(_ durable.Deps, _ durable.StepContext) (string, error) {
						atomic.AddInt64(&bodyRuns[idx], 1)
						return val, nil
					})
			}
		}, durable.WithDagMaxConcurrency(1))
		if err != nil {
			return lpOut{}, err
		}
		if e := res.ThrowIfError(); e != nil {
			return lpOut{}, e
		}

		// digestBefore is checkpointed in a step so it survives the suspend.
		digestBefore, err := durable.Step(ctx, "digestBefore",
			func(_ durable.StepContext) (string, error) { return lpDigest(res), nil })
		if err != nil {
			return lpOut{}, err
		}

		// The 2s wait ends this invocation; the next one replays the
		// completed bigdag container via Go's ReplayChildren path.
		if err := durable.Wait(ctx, "settle", 2*time.Second); err != nil {
			return lpOut{}, err
		}

		// Everything below runs on the REPLAY, reading the reconstructed
		// aggregate.
		digestAfter := lpDigest(res)

		lengths := make([]int, len(lpTaskNames))
		allIdentical := true
		for i, name := range lpTaskNames {
			v, e := durable.ResultByName[string](res, name)
			if e != nil {
				allIdentical = false
				continue
			}
			lengths[i] = len(v)
			if v != lpExpectedValue(i) {
				allIdentical = false
			}
		}
		fullP1, _ := durable.ResultByName[string](res, "p1")

		return lpOut{
			Reason:           string(res.CompletionReason()),
			Success:          res.SucceededCount(),
			DigestBefore:     digestBefore,
			DigestAfter:      digestAfter,
			Match:            digestBefore == digestAfter,
			FullP1:           fullP1,
			Lengths:          lengths,
			AllByteIdentical: allIdentical,
		}, nil
	}
}

// TestDagE2E_LargePayloadAggregateFidelity is the shared "aggregate fidelity"
// guard: it runs the bigdag graph, forces a replay of the completed
// container (via the mid-handler 2s wait, which the local runner
// auto-advances), and asserts the >256KB aggregate came back byte-identical.
// It checks not just the digest but one FULL 51200-char value (p1) and every
// task's individual retrievability and length.
func TestDagE2E_LargePayloadAggregateFidelity(t *testing.T) {
	var bodyRuns [8]int64
	var handlerEntries int64

	runner := durabletest.NewLocalRunner(newLargePayloadHandler(&bodyRuns, &handlerEntries))
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (%+v)", result.Status, result.Error)
	}
	o, err := durabletest.ResultAs[lpOut](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}

	// The container was actually replayed: the handler was entered more than
	// once (it suspended on the wait and resumed).
	if got := atomic.LoadInt64(&handlerEntries); got < 2 {
		t.Fatalf("handler entered %d time(s); the container was not replayed across a suspend", got)
	}

	// Outcome.
	if o.Success != 8 || o.Reason != string(durable.AllCompleted) {
		t.Fatalf("unexpected aggregate: %+v", o)
	}

	// Digest fidelity across the offload + replay.
	if o.DigestBefore != lpExpectedDigest || o.DigestAfter != lpExpectedDigest {
		t.Fatalf("digest mismatch: before=%q after=%q want %q", o.DigestBefore, o.DigestAfter, lpExpectedDigest)
	}
	if !o.Match {
		t.Fatal("digestBefore != digestAfter: aggregate did not survive the offload/replay identically")
	}

	// Full-value byte fidelity — not just the digest. p1 must be exactly
	// "a" x 51200 after the replay.
	if len(o.FullP1) != lpReps {
		t.Fatalf("p1 length after replay = %d, want %d", len(o.FullP1), lpReps)
	}
	if o.FullP1 != lpExpectedValue(0) {
		t.Fatal("p1's full 51200-char value was not byte-identical after the replay")
	}

	// Every task individually retrievable and full-length.
	if !o.AllByteIdentical {
		t.Fatal("at least one task result was not byte-identical to its expected letter*51200 after replay")
	}
	if len(o.Lengths) != 8 {
		t.Fatalf("expected 8 per-task lengths, got %d", len(o.Lengths))
	}
	for i, n := range o.Lengths {
		if n != lpReps {
			t.Fatalf("task %s length after replay = %d, want %d", lpTaskNames[i], n, lpReps)
		}
	}

	// Sanity: the total genuinely exceeds the 256KB checkpoint threshold, so
	// this really is a large-payload offload and not a trivially-small DAG.
	if lpAggregate <= 256*1024 {
		t.Fatalf("test misconfigured: aggregate %d bytes is not over the 256KB threshold", lpAggregate)
	}
}

// TestDagE2E_LargePayloadTaskBodiesRunOnce is the shared "bodies are not
// re-invoked" guard. Under Go's ReplayChildren re-execution the scheduler
// re-runs on the container replay, but each task's operation must fast-path
// from its own checkpoint. If a body ran twice, a customer's side effect
// would happen twice — the exact bug this test exists to catch.
func TestDagE2E_LargePayloadTaskBodiesRunOnce(t *testing.T) {
	var bodyRuns [8]int64
	var handlerEntries int64

	runner := durabletest.NewLocalRunner(newLargePayloadHandler(&bodyRuns, &handlerEntries))
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (%+v)", result.Status, result.Error)
	}

	// The replay must have occurred, otherwise "runs once across the replay"
	// is vacuous.
	if got := atomic.LoadInt64(&handlerEntries); got < 2 {
		t.Fatalf("handler entered %d time(s); no container replay occurred, so this guard would be vacuous", got)
	}

	for i := range bodyRuns {
		if got := atomic.LoadInt64(&bodyRuns[i]); got != 1 {
			t.Fatalf("task %s body ran %d time(s) across the offload + replay, want exactly 1 "+
				"(a re-invoked body doubles a customer side effect)", lpTaskNames[i], got)
		}
	}
}

// TestDagE2E_LargePayloadReExecutionPath is the Go-specific "re-execution
// path" guard the contract asks each non-envelope SDK for. Go DOES expose a
// hook to observe which path was taken: the DAG container's checkpointed
// CONTEXT op carries ReplayChildren, surfaced by the local runner as
// TestContextDetails.ReplayChildren. This asserts the container replay goes
// through child-body re-execution (ReplayChildren=true) with NO aggregate or
// DagSummary envelope stored inline, and that fidelity still holds — the same
// guarantee the envelope SDK reaches by a different mechanism.
//
// Note on Go's offload semantics: dagFinishChild checkpoints every succeeded
// DAG container with ReplayChildren=true UNCONDITIONALLY — it is not gated on
// the 256KB threshold (the container aggregate is never stored inline in any
// case; only per-task results are checkpointed, each ~50KB here). This
// confirms DAG_SPEC_CROSS_LANGUAGE.md §2.B.6: Go re-executes the child body
// via ReplayChildren with no DagSummary envelope anywhere.
func TestDagE2E_LargePayloadReExecutionPath(t *testing.T) {
	var bodyRuns [8]int64
	var handlerEntries int64

	runner := durabletest.NewLocalRunner(newLargePayloadHandler(&bodyRuns, &handlerEntries))
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (%+v)", result.Status, result.Error)
	}
	o, err := durabletest.ResultAs[lpOut](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}

	// The bigdag container is offloaded via ReplayChildren: this is the
	// observable proof Go took the child-body re-execution path.
	scope := dagScopeOp(t, result, "bigdag")
	if scope.ContextDetails == nil {
		t.Fatal("bigdag scope op has no ContextDetails")
	}
	if !scope.ContextDetails.ReplayChildren {
		t.Fatal("bigdag container was NOT checkpointed with ReplayChildren=true; " +
			"Go must offload/re-execute the child body, not store an aggregate")
	}
	// No aggregate nor DagSummary envelope is stored inline on the container —
	// the aggregate lives only in the per-task checkpoints.
	if scope.ContextDetails.Result != "" {
		t.Fatalf("bigdag container stored a non-empty inline result (%d bytes); "+
			"Go must not store the aggregate or a DagSummary envelope", len(scope.ContextDetails.Result))
	}

	// Fidelity by the re-execution mechanism: same digest, byte-identical p1.
	if !o.Match || o.DigestAfter != lpExpectedDigest {
		t.Fatalf("re-execution path did not reconstruct the aggregate identically: %+v", o)
	}
	if o.FullP1 != lpExpectedValue(0) {
		t.Fatal("p1 not byte-identical after child-body re-execution")
	}

	// And re-execution did not re-run any body.
	for i := range bodyRuns {
		if got := atomic.LoadInt64(&bodyRuns[i]); got != 1 {
			t.Fatalf("task %s body ran %d time(s) under re-execution, want 1", lpTaskNames[i], got)
		}
	}
}
