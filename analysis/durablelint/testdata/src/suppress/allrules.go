//durable:ignore-file
package suppress

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func everyRule(ctx durable.Context) {
	go func() {
		durable.Step(ctx, "a", work)
	}()
}
