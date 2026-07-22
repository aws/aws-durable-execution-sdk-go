// Command chained-invoke demonstrates chaining multiple [durable.Invoke]
// calls in sequence: validate → process → confirm. Each step invokes the
// invoke-simple-target function with different payloads, and the next step
// uses the previous result.
package main

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input carries the target function name and the order to process.
type Input struct {
	TargetFunction string `json:"targetFunction"`
	OrderID        string `json:"orderId"`
}

// StageRequest is sent to the target function at each stage.
type StageRequest struct {
	OrderID string `json:"orderId"`
	Stage   string `json:"stage"`
}

// StageResult is returned by the target function.
type StageResult struct {
	OrderID string `json:"orderId"`
	Stage   string `json:"stage"`
	Status  string `json:"status"`
}

// Result summarizes the chain outcome.
type Result struct {
	OrderID  string        `json:"orderId"`
	Stages   []StageResult `json:"stages"`
	Complete bool          `json:"complete"`
}

func handler(ctx durable.Context, event Input) (Result, error) {
	stages := []string{"validate", "process", "confirm"}
	var results []StageResult

	for _, stage := range stages {
		payload := StageRequest{OrderID: event.OrderID, Stage: stage}

		raw, err := durable.Invoke[json.RawMessage](ctx,
			fmt.Sprintf("chain-%s", stage), event.TargetFunction, payload)
		if err != nil {
			return Result{OrderID: event.OrderID, Stages: results}, err
		}

		var sr StageResult
		if err := json.Unmarshal(raw, &sr); err != nil {
			return Result{OrderID: event.OrderID, Stages: results},
				fmt.Errorf("chain-%s: unmarshal result: %w", stage, err)
		}
		results = append(results, sr)
	}

	return Result{OrderID: event.OrderID, Stages: results, Complete: true}, nil
}

func main() { durable.Start(handler) }
