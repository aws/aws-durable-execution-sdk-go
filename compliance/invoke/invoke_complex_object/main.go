// Command invoke_complex_object implements conformance requirement 5-3:
// invoke returning a nested JSON object.
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
