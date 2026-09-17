// Command invoke_target_fails implements conformance requirement 5-5: the
// invoked function throws, the caller gets an InvokeError, and the
// execution fails.
package main

import (
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event any) (string, error) {
	return durable.Invoke[string](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), event)
}

func main() {
	durable.Start(handler)
}
