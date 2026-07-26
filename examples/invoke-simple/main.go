// Command invoke-simple demonstrates [durable.Invoke]: durably calling
// another Lambda function and awaiting its result. The target function
// name and payload are supplied in the event, falling back to the
// FUNCTION_NAME_PREFIX environment variable for cloud deployments.
package main

import (
	"encoding/json"
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input carries the target function name and payload.
type Input struct {
	FunctionName string          `json:"functionName"`
	Payload      json.RawMessage `json:"payload"`
}

func handler(ctx durable.Context, event Input) (json.RawMessage, error) {
	functionName := event.FunctionName
	if functionName == "" {
		prefix := os.Getenv("FUNCTION_NAME_PREFIX")
		if prefix == "" {
			prefix = "v2-"
		}
		functionName = prefix + "go-invoke-simple-target:$LATEST"
	}
	return durable.Invoke[json.RawMessage](ctx, "invoke", functionName, event.Payload)
}

func main() { durable.Start(handler) }
