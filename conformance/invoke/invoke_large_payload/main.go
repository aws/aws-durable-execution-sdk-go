// Command invoke_large_payload implements conformance requirement 5-7:
// invoke with a payload near the size limit.
package main

import (
	"os"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type payload struct {
	Data string `json:"data"`
}

func handler(ctx durable.Context, _ any) (any, error) {
	large := payload{Data: strings.Repeat("x", 200000)}
	return durable.Invoke[any](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), large)
}

func main() {
	durable.Start(handler)
}
