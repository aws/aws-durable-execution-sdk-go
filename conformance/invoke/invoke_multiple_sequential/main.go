// Command invoke_multiple_sequential implements conformance requirement
// 5-14: two sequential invokes, the second consuming the first's result.
package main

import (
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event any) (any, error) {
	first, err := durable.Invoke[any](ctx, "", os.Getenv("TARGET_FUNCTION_NAME_1"), event)
	if err != nil {
		return nil, err
	}
	return durable.Invoke[any](ctx, "", os.Getenv("TARGET_FUNCTION_NAME_2"), first)
}

func main() {
	durable.Start(handler)
}
