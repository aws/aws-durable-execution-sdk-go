// Command invoke-simple demonstrates [durable.Invoke]: durably calling
// another Lambda function and awaiting its result. The target function
// name and payload are supplied in the event.
package main

import (
	"encoding/json"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input carries the target function name and payload.
type Input struct {
	FunctionName string          `json:"functionName"`
	Payload      json.RawMessage `json:"payload"`
}

func handler(ctx durable.Context, event Input) (json.RawMessage, error) {
	return durable.Invoke[json.RawMessage](ctx, "invoke", event.FunctionName, event.Payload)
}

func main() { durable.Start(handler) }
