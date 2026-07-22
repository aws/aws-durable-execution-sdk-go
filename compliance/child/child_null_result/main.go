// Command child_null_result implements conformance requirement 3-16: a
// child context where the child function returns null without calling any
// durable operation.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (any, error) {
	return durable.RunInChildContext(ctx, "null-child", func(_ durable.Context) (any, error) {
		return nil, nil
	})
}

func main() {
	durable.Start(handler)
}
