// Command invoke_null_result implements conformance requirement 5-4:
// invoke with a null payload; the echo target returns null.
package main

import (
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (any, error) {
	return durable.Invoke[any, any](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), nil)
}

func main() {
	durable.Start(handler)
}
