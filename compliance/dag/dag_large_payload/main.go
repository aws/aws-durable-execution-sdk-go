// Command dag_large_payload is a deploy-only DAG conformance handler
// (scenario 10-15) exercising the large-payload offload/replay divergence —
// the last conformance gap and the largest untested behavioural difference
// in the DAG feature. A DAG whose AGGREGATE result exceeds the 256KB
// checkpoint limit is offloaded, and the four SDKs then deliberately diverge
// on replay (DAG_SPEC_CROSS_LANGUAGE.md §2.A.4 / §2.B.6): TypeScript writes
// an SDK-owned DagSummary envelope and reconstructs from it, whereas Python,
// Java and Go re-execute the DAG child body via ReplayChildren with no
// envelope. This handler asserts the single language-neutral property that
// holds under EITHER strategy.
//
// maxConcurrency is 1 for determinism. Graph (bigdag): eight independent
// root step tasks p1..p8, each returning 51200 repetitions of its own letter
// (p1 -> "a"x51200 .. p8 -> "h"x51200). Aggregate ~= 409600 bytes (~400KB),
// comfortably over the 256KB threshold, while every individual task result
// (~50KB) stays well under it, so only the aggregate is offloaded.
//
// Handler flow, in order:
//
//  1. Dag(...) resolves with the ~400KB aggregate.
//  2. A STEP (outside the DAG) computes a digest from the DagResult —
//     "<taskCount>:<totalLength>:<firstCharOfEachTaskInOrder>" = exactly
//     "8:409600:abcdefgh". Being a step, it is checkpointed and survives the
//     suspend as digestBefore.
//  3. A Wait of 2 seconds forces the invocation to end so the NEXT
//     invocation replays the already-succeeded DAG container. This is the
//     whole point: the reconstruct-vs-re-execute divergence only fires when
//     a succeeded container is replayed.
//  4. After the resume the same digest is recomputed from the REPLAYED
//     DagResult as digestAfter.
//
// Returned summary: {reason, counts[8,0,0,8], digestBefore, digestAfter,
// match}. digestBefore == digestAfter == "8:409600:abcdefgh" proves the
// aggregate survived the offload and came back identical through Go's
// child-body re-execution path. Outcome is asserted; the container's
// ContextSucceeded payload legitimately differs across SDKs, so no execution
// history is pinned. The summary stays small — it carries the digest, never
// the payload.
package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// taskNames are the eight step tasks in registration order. Task pN returns
// its own letter (a..h) repeated payloadReps times.
var taskNames = []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"}

// payloadReps is the per-task repetition count. 8 * 51200 = 409600 bytes of
// aggregate, over 256KB; each task's 51200 bytes is well under it.
const payloadReps = 51200

// digest is the language-neutral fingerprint of the aggregate DagResult:
// "<taskCount>:<totalLength>:<firstCharOfEachTaskInOrder>". Reading the
// per-task results in registration order makes it deterministic and
// identical across SDKs and across the offload/replay boundary.
func digest(res *durable.DagResult) string {
	total := 0
	var firsts strings.Builder
	for _, name := range taskNames {
		v, err := durable.ResultByName[string](res, name)
		if err != nil {
			continue
		}
		total += len(v)
		if len(v) > 0 {
			firsts.WriteByte(v[0])
		}
	}
	return fmt.Sprintf("%d:%d:%s", len(taskNames), total, firsts.String())
}

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "bigdag", func(d *durable.DagBuilder) {
		for i, name := range taskNames {
			payload := strings.Repeat(string(rune('a'+i)), payloadReps)
			durable.DagStep(d, name, nil,
				func(_ durable.Deps, _ durable.StepContext) (string, error) {
					return payload, nil
				})
		}
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return dagsummary.Summary{}, err
	}

	// digestBefore is computed in a STEP so it is checkpointed and survives
	// the suspend unchanged; on replay this step fast-paths and returns the
	// value minted in the first invocation.
	digestBefore, err := durable.Step(ctx, "digestBefore",
		func(_ durable.StepContext) (string, error) { return digest(res), nil })
	if err != nil {
		return dagsummary.Summary{}, err
	}

	// The 2s wait ends this invocation; the next one replays the completed
	// bigdag container, exercising Go's ReplayChildren re-execution path.
	if err := durable.Wait(ctx, "settle", 2*time.Second); err != nil {
		return dagsummary.Summary{}, err
	}

	// digestAfter is recomputed from the DagResult as reconstructed on the
	// replay after the resume.
	digestAfter := digest(res)

	sum := dagsummary.From(res)
	// The 10-15 contract fixes the returned summary to exactly
	// {reason, counts, digestBefore, digestAfter, match} and NEVER the
	// payload. From() populates a per-task Statuses map used by the other DAG
	// handlers; drop it here so (with omitempty) the key is omitted and the
	// result is byte-shaped to the language-neutral assertion.
	sum.Statuses = nil
	sum.DigestBefore = digestBefore
	sum.DigestAfter = digestAfter
	match := digestBefore == digestAfter
	sum.Match = &match
	return sum, nil
}

func main() { durable.Start(handler) }
