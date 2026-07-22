// Command invoke-simple-target is the target function for invoke-simple
// and chained-invoke. It echoes its input with added metadata, simulating
// a downstream service.
package main

import (
	"encoding/json"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// GenericInput accepts any JSON payload.
type GenericInput struct {
	OrderID string `json:"orderId,omitempty"`
	Stage   string `json:"stage,omitempty"`
	Message string `json:"message,omitempty"`
}

// GenericResult returns a status and the received input.
type GenericResult struct {
	OrderID string          `json:"orderId,omitempty"`
	Stage   string          `json:"stage,omitempty"`
	Status  string          `json:"status"`
	Input   json.RawMessage `json:"input"`
}

func handler(ctx durable.Context, event GenericInput) (GenericResult, error) {
	inputBytes, _ := json.Marshal(event)
	return GenericResult{
		OrderID: event.OrderID,
		Stage:   event.Stage,
		Status:  "completed",
		Input:   inputBytes,
	}, nil
}

func main() { durable.Start(handler) }
