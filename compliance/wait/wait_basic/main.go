// Command wait_basic implements conformance requirement 2-1: a single
// wait operation with a specified duration.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (any, error) {
	if err := durable.Wait(ctx, "", 2*time.Second); err != nil {
		return nil, err
	}
	return nil, nil
}

func main() {
	durable.Start(handler)
}
