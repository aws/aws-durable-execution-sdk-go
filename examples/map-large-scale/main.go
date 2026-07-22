// Command map-large-scale demonstrates a Map operation with 50 items,
// each returning a large payload (~100KB), bounded to 10 concurrency.
package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type ItemResult struct {
	ItemID    int    `json:"itemId"`
	Index     int    `json:"index"`
	DataSize  int    `json:"dataSize"`
	Processed bool   `json:"processed"`
	Data      string `json:"data"`
}

type Summary struct {
	ItemsProcessed    int  `json:"itemsProcessed"`
	TotalDataSizeMB   int  `json:"totalDataSizeMB"`
	TotalDataBytes    int  `json:"totalDataSizeBytes"`
	MaxConcurrency    int  `json:"maxConcurrency"`
	AverageItemSize   int  `json:"averageItemSize"`
	AllItemsProcessed bool `json:"allItemsProcessed"`
}

type Output struct {
	Success bool    `json:"success"`
	Message string  `json:"message"`
	Summary Summary `json:"summary"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	items := make([]int, 50)
	for i := range items {
		items[i] = i + 1
	}

	results, err := durable.Map(ctx, "large-scale-map", items,
		func(ctx durable.Context, item int, index int) (ItemResult, error) {
			return durable.Step(ctx, fmt.Sprintf("process-item-%d", item),
				func(_ durable.StepContext) (ItemResult, error) {
					data := strings.Repeat("B", 100*1024) // ~100KB
					return ItemResult{
						ItemID:    item,
						Index:     index,
						DataSize:  len(data),
						Data:      data,
						Processed: true,
					}, nil
				})
		},
		durable.WithMaxConcurrency(10),
	)
	if err != nil {
		return Output{}, err
	}

	_ = durable.Wait(ctx, "wait1", 1*time.Second)

	succeeded := results.Results()
	totalDataSize := 0
	allProcessed := true
	for _, r := range succeeded {
		totalDataSize += r.DataSize
		if !r.Processed {
			allProcessed = false
		}
	}

	_ = durable.Wait(ctx, "wait2", 1*time.Second)

	return Output{
		Success: true,
		Message: "Successfully processed 50 items with substantial data using map",
		Summary: Summary{
			ItemsProcessed:    results.SuccessCount(),
			TotalDataSizeMB:   totalDataSize / (1024 * 1024),
			TotalDataBytes:    totalDataSize,
			MaxConcurrency:    10,
			AverageItemSize:   totalDataSize / max(results.SuccessCount(), 1),
			AllItemsProcessed: allProcessed,
		},
	}, nil
}

func main() { durable.Start(handler) }
