// Command parallel-custom-summary-generator demonstrates
// [durable.WithBatchSummary] on a [durable.Parallel] operation.
//
// The SDK checkpoints a batch result when it is at most 256 KiB
// serialized. A larger result is not stored: the checkpoint records that
// the branch operations are kept, plus a compact record of the completion,
// and replay rebuilds the result from the branch checkpoints. The summary
// function supplies a description of the result that is stored in that
// record under its "summary" key, so the execution history shows what the
// batch produced. The full results are still returned to the caller.
package main

import (
	"encoding/json"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// defaultBranchPayloadSize is the size of each branch's result. Three
// branches of this size push the serialized batch result past the 256 KiB
// checkpoint limit, which is the only path on which the summary function
// runs. Tests pass a small size to keep the result within one checkpoint.
const defaultBranchPayloadSize = 120_000

// customSummaryMarker is embedded in the summary so a test can prove the
// caller-supplied function produced the stored value.
const customSummaryMarker = "custom-parallel-summary"

// Event optionally overrides the per-branch payload size.
type Event struct {
	BranchPayloadSize int `json:"branchPayloadSize"`
}

// Output reports the batch outcome and the size of each branch result.
type Output struct {
	TotalCount    int   `json:"totalCount"`
	SuccessCount  int   `json:"successCount"`
	ResultLengths []int `json:"resultLengths"`
}

// Summary is the value the summary function serializes.
type Summary struct {
	Marker       string `json:"marker"`
	TotalCount   int    `json:"totalCount"`
	SuccessCount int    `json:"successCount"`
}

func handler(ctx durable.Context, event Event) (Output, error) {
	branchPayloadSize := event.BranchPayloadSize
	if branchPayloadSize <= 0 {
		branchPayloadSize = defaultBranchPayloadSize
	}

	branch := func(name, fill string) durable.Branch[string] {
		return durable.Branch[string]{Name: name, Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, name, func(durable.StepContext) (string, error) {
				return strings.Repeat(fill, branchPayloadSize), nil
			})
		}}
	}

	results, err := durable.Parallel(ctx, "parallel-large", []durable.Branch[string]{
		branch("branch-0", "a"),
		branch("branch-1", "b"),
		branch("branch-2", "c"),
	},
		durable.WithMaxConcurrency(3),
		durable.WithBatchSummary(func(r durable.BatchResult[string]) string {
			// The summary must be deterministic: it is stored once and
			// never recomputed on replay.
			b, err := json.Marshal(Summary{
				Marker:       customSummaryMarker,
				TotalCount:   r.TotalCount(),
				SuccessCount: r.SuccessCount(),
			})
			if err != nil {
				return customSummaryMarker
			}
			return string(b)
		}),
	)
	if err != nil {
		return Output{}, err
	}

	lengths := make([]int, 0, len(results.Results()))
	for _, r := range results.Results() {
		lengths = append(lengths, len(r))
	}
	return Output{
		TotalCount:    results.TotalCount(),
		SuccessCount:  results.SuccessCount(),
		ResultLengths: lengths,
	}, nil
}

func main() { durable.Start(handler) }
