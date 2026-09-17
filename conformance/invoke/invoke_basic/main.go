// Command invoke_basic implements conformance requirement 5-1: invoke an
// echo target function with the event input and return its result.
package main

import (
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event any) (any, error) {
	return durable.Invoke[any](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), event)
}

func main() {
	durable.Start(handler)
}
