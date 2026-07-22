// Command wait_with_name implements conformance requirement 2-2: a wait
// operation with an explicit name parameter.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (any, error) {
	if err := durable.Wait(ctx, "custom_wait_name", 2*time.Second); err != nil {
		return nil, err
	}
	return nil, nil
}

func main() {
	durable.Start(handler)
}
