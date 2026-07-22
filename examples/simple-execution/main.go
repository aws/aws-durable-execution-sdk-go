// Command simple-execution demonstrates a durable handler that executes
// without performing any durable operations — it processes the event and
// returns a structured result immediately.
package main

import (
	"encoding/json"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result is the structured output of this handler.
type Result struct {
	Received  string `json:"received"`
	Timestamp int64  `json:"timestamp"`
	Message   string `json:"message"`
}

func handler(_ durable.Context, event json.RawMessage) (Result, error) {
	return Result{
		Received:  string(event),
		Timestamp: time.Now().UnixMilli(),
		Message:   "Handler completed successfully",
	}, nil
}

func main() { durable.Start(handler) }
