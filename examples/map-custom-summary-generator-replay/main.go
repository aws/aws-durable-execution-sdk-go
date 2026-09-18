// Command map-custom-summary-generator-replay demonstrates
// [durable.WithBatchSummary] on a [durable.Map] whose aggregate result
// exceeds the 256 KiB checkpoint limit and that completes early through
// MinSuccessful while one item is still in flight.
//
// The oversized result is not checkpointed. The SDK stores a compact
// record of the completion instead, with the caller's summary under its
// "summary" key, and rebuilds the result from the item checkpoints on
// replay. The summary here is free-form text: it is advisory, so replay
// does not depend on anything it contains. After the map the handler
// suspends on a wait, so the second invocation replays the summarized map
// and must observe the same result the first invocation did.
package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// defaultItemPayloadSize is the size of each item's result. Two successes
// of this size push the aggregate past the 256 KiB checkpoint limit,
// which is the only path on which the summary function runs. Tests pass
// a small size to keep the result within a single checkpoint.
const defaultItemPayloadSize = 150 * 1024

// customSummaryPrefix starts the summary so a test can prove the
// caller-supplied function produced the stored value.
const customSummaryPrefix = "processed"

// Event optionally overrides the per-item payload size.
type Event struct {
	ItemPayloadSize int `json:"itemPayloadSize"`
}

// Output is the aggregate the handler observed.
type Output struct {
	TotalCount       int    `json:"totalCount"`
	SuccessCount     int    `json:"successCount"`
	StartedCount     int    `json:"startedCount"`
	CompletionReason string `json:"completionReason"`
	ItemIndexes      []int  `json:"itemIndexes"`
}

func handler(ctx durable.Context, event Event) (Output, error) {
	itemPayloadSize := event.ItemPayloadSize
	if itemPayloadSize <= 0 {
		itemPayloadSize = defaultItemPayloadSize
	}

	result, err := durable.Map(ctx, "summarized-map", []int{0, 1, 2, 3, 4},
		func(ctx durable.Context, _ int, index int) (string, error) {
			// With MaxConcurrency 2, items 0 and 1 start first. Item 1
			// blocks on a long wait, so item 0 finishes and frees a slot
			// for item 2, which also finishes. That reaches MinSuccessful
			// 2 while item 1 is still in flight, so the batch abandons
			// it and reports it started. The wait is never resumed: the
			// execution completes long before it would elapse.
			if index == 1 {
				if err := durable.Wait(ctx, "slow-item", time.Hour); err != nil {
					return "", err
				}
			}
			return strings.Repeat("x", itemPayloadSize), nil
		},
		durable.WithMaxConcurrency(2),
		durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 2}),
		durable.WithItemNamer(func(i int) string { return fmt.Sprintf("item-%d", i) }),
		// Free-form text with none of the fields replay needs. Replay
		// reads the SDK's own record, never this string.
		durable.WithBatchSummary(func(r durable.BatchResult[string]) string {
			return fmt.Sprintf("%s %d/%d items", customSummaryPrefix, r.SuccessCount(), r.TotalCount())
		}),
	)
	if err != nil {
		return Output{}, err
	}

	// Suspend, then resume: the second invocation replays the
	// summarized map from its item checkpoints.
	if err := durable.Wait(ctx, "after-map", time.Second); err != nil {
		return Output{}, err
	}

	indexes := make([]int, 0, len(result.Items))
	for _, item := range result.Items {
		indexes = append(indexes, item.Index)
	}
	return Output{
		TotalCount:       result.TotalCount(),
		SuccessCount:     result.SuccessCount(),
		StartedCount:     result.StartedCount(),
		CompletionReason: result.Reason.String(),
		ItemIndexes:      indexes,
	}, nil
}

func main() { durable.Start(handler) }
