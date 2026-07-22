// Command invoke_in_child_context implements conformance requirement 5-13:
// an invoke inside a child context; the child returns the invoke result.
package main

import (
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.RunInChildContext(ctx, "", func(child durable.Context) (string, error) {
		return durable.Invoke[string, any](child, "", os.Getenv("TARGET_FUNCTION_NAME"), nil)
	})
}

func main() {
	durable.Start(handler)
}
