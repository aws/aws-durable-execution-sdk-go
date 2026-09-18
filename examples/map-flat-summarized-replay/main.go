// Command map-flat-summarized-replay demonstrates a [durable.Map] with
// [durable.NestingFlat] whose aggregate result exceeds the 256 KiB
// checkpoint limit and is replayed across a suspension.
//
// An oversized batch result is not checkpointed. The SDK stores a compact
// record of which items finished and rebuilds the result from the item
// checkpoints on replay. A FLAT item runs in a virtual context that is
// never checkpointed itself, so nothing beneath it identifies a finished
// item whose body created no durable operation; the record is what makes
// such an item reconstructible. The handler records the item count the
// first invocation observed in a step, suspends on a wait, and records the
// count the replayed batch reports, so a test can compare the two.
package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

const (
	// itemCount and itemPayloadBytes together push the aggregate result
	// past the 256 KiB checkpoint limit while each item stays well under
	// it, so only the parent batch takes the summarized path.
	itemCount        = 8
	itemPayloadBytes = 40 * 1024
)

// Event optionally overrides the per-item payload size and nesting, and
// selects a mapper that performs no durable operation.
type Event struct {
	ItemPayloadSize int `json:"itemPayloadSize"`
	// Nesting is "FLAT" (default) or "NORMAL".
	Nesting string `json:"nesting"`
	// NoDurableOperation makes each item return its value directly
	// instead of from a step, so a FLAT item leaves no checkpoint of its
	// own beneath the batch.
	NoDurableOperation bool `json:"noDurableOperation"`
}

// ItemResult is one item's value.
type ItemResult struct {
	Item    int    `json:"item"`
	Payload string `json:"payload"`
}

// Output compares the live and replayed views of the batch.
type Output struct {
	LiveResultCount     int   `json:"liveResultCount"`
	ReplayedResultCount int   `json:"replayedResultCount"`
	ReplayedItems       []int `json:"replayedItems"`
}

func handler(ctx durable.Context, event Event) (Output, error) {
	itemPayloadSize := event.ItemPayloadSize
	if itemPayloadSize <= 0 {
		itemPayloadSize = itemPayloadBytes
	}
	nesting := durable.NestingFlat
	if event.Nesting == "NORMAL" {
		nesting = durable.NestingNormal
	}

	items := make([]int, itemCount)
	for i := range items {
		items[i] = i
	}

	batch, err := durable.Map(ctx, "resolve-pages", items,
		func(ctx durable.Context, item int, index int) (ItemResult, error) {
			value := ItemResult{Item: item, Payload: strings.Repeat("x", itemPayloadSize)}
			if event.NoDurableOperation {
				return value, nil
			}
			return durable.Step(ctx, fmt.Sprintf("resolve-%d", index),
				func(durable.StepContext) (ItemResult, error) { return value, nil })
		},
		durable.WithNesting(nesting),
	)
	if err != nil {
		return Output{}, err
	}

	// Recorded on the first invocation; replay returns the stored value,
	// so the live observation survives to be compared with the replayed
	// one.
	liveResultCount, err := durable.Step(ctx, "record-live-count",
		func(durable.StepContext) (int, error) { return len(batch.Results()), nil })
	if err != nil {
		return Output{}, err
	}

	// Suspension point: everything below runs in a new invocation that
	// replays the summarized map from its item checkpoints.
	if err := durable.Wait(ctx, "suspend", 5*time.Second); err != nil {
		return Output{}, err
	}

	// Runs for the first time after the resumption, so it observes the
	// rebuilt batch rather than the live one.
	replayedResultCount, err := durable.Step(ctx, "record-replayed-count",
		func(durable.StepContext) (int, error) { return len(batch.Results()), nil })
	if err != nil {
		return Output{}, err
	}

	replayedItems := make([]int, 0, len(batch.Results()))
	for _, r := range batch.Results() {
		replayedItems = append(replayedItems, r.Item)
	}
	return Output{
		LiveResultCount:     liveResultCount,
		ReplayedResultCount: replayedResultCount,
		ReplayedItems:       replayedItems,
	}, nil
}

func main() { durable.Start(handler) }
