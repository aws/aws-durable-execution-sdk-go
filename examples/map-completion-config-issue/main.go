// Command map-completion-config-issue reproduces a scenario where map with
// minSuccessful combined with toleratedFailurePercentage completes early.
// Once the MinSuccessful threshold is met the map stops awaiting the items
// still in flight: those are abandoned (reported as started, not counted as
// successes or failures), and items that never started are omitted. The
// output distinguishes succeeded, failed, and started-but-abandoned items.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Item struct {
	ID         int  `json:"id"`
	ShouldFail bool `json:"shouldFail"`
}

type SuccessItem struct {
	Index  int `json:"index"`
	ItemID int `json:"itemId"`
}

type FailItem struct {
	Index  int    `json:"index"`
	ItemID int    `json:"itemId"`
	Error  string `json:"error"`
}

type Output struct {
	TotalItems      int           `json:"totalItems"`
	SuccessfulCount int           `json:"successfulCount"`
	FailedCount     int           `json:"failedCount"`
	StartedCount    int           `json:"startedCount"`
	HasFailures     bool          `json:"hasFailures"`
	BatchStatus     string        `json:"batchStatus"`
	CompletionNote  string        `json:"completionReason"`
	SuccessItems    []SuccessItem `json:"successfulItems"`
	FailItems       []FailItem    `json:"failedItems"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	items := []Item{
		{ID: 1, ShouldFail: false},
		{ID: 2, ShouldFail: true},
		{ID: 3, ShouldFail: false},
		{ID: 4, ShouldFail: true},
		{ID: 5, ShouldFail: false},
	}

	results, err := durable.Map(ctx, "completion-config-items", items,
		func(ctx durable.Context, item Item, index int) (any, error) {
			return durable.Step(ctx, fmt.Sprintf("process-item-%d", index),
				func(_ durable.StepContext) (any, error) {
					if item.ShouldFail {
						return nil, fmt.Errorf("processing failed for item %d", item.ID)
					}
					return map[string]any{
						"itemId":    item.ID,
						"processed": true,
						"result":    fmt.Sprintf("Item %d processed successfully", item.ID),
					}, nil
				},
				durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
					MaxAttempts:  2,
					InitialDelay: 1 * time.Second,
				})),
			)
		},
		durable.WithCompletion(durable.CompletionConfig{
			MinSuccessful:              2,
			ToleratedFailurePercentage: 50,
		}),
		durable.WithMaxConcurrency(3),
	)
	if err != nil {
		return Output{}, err
	}

	var successItems []SuccessItem
	for _, item := range results.Succeeded() {
		successItems = append(successItems, SuccessItem{
			Index:  item.Index,
			ItemID: items[item.Index].ID,
		})
	}

	var failItems []FailItem
	for _, item := range results.Failed() {
		failItems = append(failItems, FailItem{
			Index:  item.Index,
			ItemID: items[item.Index].ID,
			Error:  item.Err.Error(),
		})
	}

	startedCount := 0
	for _, item := range results.Items {
		if item.Status == durable.BatchItemStarted {
			startedCount++
		}
	}

	return Output{
		TotalItems:      results.TotalCount(),
		SuccessfulCount: results.SuccessCount(),
		FailedCount:     results.FailureCount(),
		StartedCount:    startedCount,
		HasFailures:     results.HasFailure(),
		BatchStatus:     results.Status().String(),
		CompletionNote:  results.Reason.String(),
		SuccessItems:    successItems,
		FailItems:       failItems,
	}, nil
}

func main() { durable.Start(handler) }
