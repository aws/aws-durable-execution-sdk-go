// Command child_error_no_step implements conformance requirement 3-15: a
// child context where the child function throws an error directly without
// calling any durable operation.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.RunInChildContext(ctx, "direct-error", func(_ durable.Context) (string, error) {
		return "", errors.New("direct error")
	})
}

func main() {
	durable.Start(handler)
}
