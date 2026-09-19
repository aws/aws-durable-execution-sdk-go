package suppress

//durable:ignore-file durablegoroutine -- this file is exempt from the goroutine rule

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func fileDirective(ctx durable.Context) {
	go func() {
		durable.Step(ctx, "a", work)
	}()
}
