// Command dag_nested_large_payload is a deploy-only DAG conformance handler
// (scenario 10-17, DagNestedLargePayload) covering the untested intersection
// of nesting and large payloads. It is modeled on 10-15 (dag_large_payload)
// but the large aggregate lives in a NESTED sub-DAG, so BOTH the inner and
// the outer container's per-task detail must survive the offload/replay
// boundary.
//
// A nested DAG whose own aggregate exceeds the 256KB checkpoint limit can be
// restored as an EMPTY result that falsely claims success; this scenario is
// the intersection that hits that fallback (NESTED_OFFLOAD_CONTRACT.md). It
// is missed by 10-9 DagNested (small, nothing offloads) and 10-15
// DagLargePayload (flat, no nesting).
//
// maxConcurrency is 1 everywhere for determinism. Graph and flow follow 10-15
// exactly: the outer DAG "outernested" contains a SINGLE nested SubDag task
// "inner" (maxConcurrency 1) of six step tasks p1..p6, each returning its own
// distinct letter (a..f) repeated payloadReps times. 6 * 51200 = 307200 bytes
// (~307KB), over the 256KB limit, so the inner aggregate is offloaded; each
// ~50KB task result stays under it. Because the outer embeds the inner tasks
// in full, the outer is over the limit too, so BOTH containers offload.
//
// Everything else runs at HANDLER level, OUTSIDE the DAG, so the DAG completes
// in the first invocation and the NEXT invocation replays BOTH already-
// succeeded containers — which is the whole point (the reconstruct-vs-
// re-execute divergence only fires when a completed container is replayed):
//
//  1. Dag(...) resolves with the offloaded outer/inner aggregate.
//  2. digestBefore — a STEP that reads the inner DagResult and computes a
//     compact digest "<taskCount>:<totalLength>:<firstCharOfEachTaskInOrder>"
//     = exactly "6:307200:abcdef". Being a step it is checkpointed and
//     survives the suspend, fast-pathing on replay.
//  3. Wait "settle" of 2 seconds forces the invocation to end so the next one
//     replays both completed containers (outer + inner).
//  4. digestAfter — recomputes the identical digest from the inner DagResult
//     reconstructed AFTER the replay.
//
// Returned summary: exactly {reason, innerReason, innerCounts, digestBefore,
// digestAfter, match}, where innerCounts is [total, failed, skipped,
// succeeded]. The decisive assertion is
// digestBefore == digestAfter == "6:307200:abcdef" with match: true,
// innerReason: ALL_COMPLETED, innerCounts: [6,0,0,6] — proving the inner
// per-task detail survived the offload of BOTH containers. Under the bug the
// inner comes back empty, so digestAfter differs while innerReason would
// still read ALL_COMPLETED from a fabricated result: the digest, not the
// reason, is the decisive check. Outcome is asserted; the container's
// ContextSucceeded payload legitimately differs across SDKs, so no execution
// history is pinned (following 10-15).
//
// The result is a scenario-specific struct rather than the shared
// dagsummary.Summary: that shared type serializes a mandatory (non-omitempty)
// "counts" key, which the 10-17 contract does not include, and the runner
// does strict full-result equality. A local struct emits exactly the six
// contract keys without touching dagsummary or the other fifteen DAG handlers.
package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// innerTaskNames are the six inner step tasks in registration order. Task pN
// returns its own letter (a..f) repeated payloadReps times.
var innerTaskNames = []string{"p1", "p2", "p3", "p4", "p5", "p6"}

// payloadReps is the per-task repetition count. 6 * 51200 = 307200 bytes of
// inner aggregate, over 256KB; each task's 51200 bytes is well under it.
const payloadReps = 51200

// summary is the 10-17-specific result. The contract fixes it to exactly
// {reason, innerReason, innerCounts, digestBefore, digestAfter, match}; unlike
// dagsummary.Summary it carries no "counts" key, so Go emits precisely the six
// keys JS, Python and Java emit and the strict full-result equality holds.
type summary struct {
	Reason       string `json:"reason"`
	InnerReason  string `json:"innerReason"`
	InnerCounts  [4]int `json:"innerCounts"`
	DigestBefore string `json:"digestBefore"`
	DigestAfter  string `json:"digestAfter"`
	Match        bool   `json:"match"`
}

// digest is the language-neutral fingerprint of the inner aggregate
// DagResult: "<taskCount>:<totalLength>:<firstCharOfEachTaskInOrder>".
// Reading the per-task results in registration order makes it deterministic
// and identical across SDKs and across the offload/replay boundary.
func digest(inner *durable.DagResult) string {
	total := 0
	var firsts strings.Builder
	for _, name := range innerTaskNames {
		v, err := durable.ResultByName[string](inner, name)
		if err != nil {
			continue
		}
		total += len(v)
		if len(v) > 0 {
			firsts.WriteByte(v[0])
		}
	}
	return fmt.Sprintf("%d:%d:%s", len(innerTaskNames), total, firsts.String())
}

func handler(ctx durable.Context, _ struct{}) (summary, error) {
	// The outer DAG contains exactly ONE task: the nested SubDag whose ~307KB
	// aggregate offloads both the inner and (embedding it) the outer container.
	res, err := durable.Dag(ctx, "outernested", func(d *durable.DagBuilder) {
		durable.SubDag(d, "inner", nil, func(sub *durable.DagBuilder) {
			for i, name := range innerTaskNames {
				payload := strings.Repeat(string(rune('a'+i)), payloadReps)
				durable.DagStep(sub, name, nil,
					func(_ durable.Deps, _ durable.StepContext) (string, error) {
						return payload, nil
					})
			}
		}, durable.WithDagMaxConcurrency(1))
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return summary{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return summary{}, err
	}

	// digestBefore is computed in a STEP (outside the DAG) so it is
	// checkpointed and survives the suspend unchanged; on replay this step
	// fast-paths and returns the value minted in the first invocation.
	digestBefore, err := durable.Step(ctx, "digestBefore",
		func(_ durable.StepContext) (string, error) {
			inner, e := durable.ResultByName[*durable.DagResult](res, "inner")
			if e != nil {
				return "", e
			}
			return digest(inner), nil
		})
	if err != nil {
		return summary{}, err
	}

	// The 2s wait ends this invocation; the next one replays BOTH the outer
	// and the offloaded inner container.
	if err := durable.Wait(ctx, "settle", 2*time.Second); err != nil {
		return summary{}, err
	}

	// After the resume, re-read the inner DagResult reconstructed by recursing
	// into the inner container's own child checkpoints, and recompute the
	// identical digest from it.
	inner, err := durable.ResultByName[*durable.DagResult](res, "inner")
	if err != nil {
		return summary{}, err
	}
	digestAfter := digest(inner)

	return summary{
		Reason:      string(res.CompletionReason()),
		InnerReason: string(inner.CompletionReason()),
		// innerCounts is [total, failed, skipped, succeeded] per the 10-17 contract.
		InnerCounts: [4]int{
			inner.TotalCount(), inner.FailureCount(), inner.SkippedCount(), inner.SucceededCount(),
		},
		DigestBefore: digestBefore,
		DigestAfter:  digestAfter,
		Match:        digestBefore == digestAfter,
	}, nil
}

func main() { durable.Start(handler) }
