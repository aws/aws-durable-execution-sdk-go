// Command invoke_with_name implements conformance requirement 5-2: invoke
// with an explicit operation name taken from the input.
package main

import (
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type input struct {
	Name    string `json:"name"`
	Payload any    `json:"payload"`
}

func handler(ctx durable.Context, event input) (any, error) {
	return durable.Invoke[any](ctx, event.Name, os.Getenv("TARGET_FUNCTION_NAME"), event.Payload)
}

func main() {
	durable.Start(handler)
}
