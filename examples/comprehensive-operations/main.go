// Command comprehensive-operations demonstrates all major durable
// operations in a single handler: Step, Wait, Map, and Parallel.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result combines outputs from all operation types.
type Result struct {
	StepResult      string   `json:"stepResult"`
	WaitCompleted   bool     `json:"waitCompleted"`
	MapResults      []int    `json:"mapResults"`
	ParallelResults []string `json:"parallelResults"`
	TotalOperations int      `json:"totalOperations"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// Step: simple checkpoint returning a string.
	stepResult, err := durable.Step(ctx, "step1", func(_ durable.StepContext) (string, error) {
		return "Step 1 completed successfully", nil
	})
	if err != nil {
		return Result{}, err
	}

	// Wait: pause for 1 second.
	_ = durable.Wait(ctx, "pause", 1*time.Second)

	// Map: process items 1-5 concurrently.
	items := []int{1, 2, 3, 4, 5}
	mapBatch, err := durable.Map(ctx, "map-numbers", items,
		func(ctx durable.Context, item int, index int) (int, error) {
			return durable.Step(ctx, fmt.Sprintf("map-step-%d", index),
				func(_ durable.StepContext) (int, error) {
					return item * 2, nil
				})
		},
		durable.WithMaxConcurrency(3),
	)
	if err != nil {
		return Result{}, err
	}

	// Parallel: three branches each returning a fruit name.
	parallelBatch, err := durable.Parallel(ctx, "parallel-fruits", []durable.Branch[string]{
		{Name: "fruit-1", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
				return "apple", nil
			})
		}},
		{Name: "fruit-2", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
				return "banana", nil
			})
		}},
		{Name: "fruit-3", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
				return "orange", nil
			})
		}},
	}, durable.WithMaxConcurrency(3))
	if err != nil {
		return Result{}, err
	}

	return Result{
		StepResult:      stepResult,
		WaitCompleted:   true,
		MapResults:      mapBatch.Results(),
		ParallelResults: parallelBatch.Results(),
		TotalOperations: 4,
	}, nil
}

func main() { durable.Start(handler) }
