// Command wait_long_duration implements conformance requirement 2-5: a
// wait operation with a 1-hour duration, verifying conversion from hours
// to seconds.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (any, error) {
	if err := durable.Wait(ctx, "", time.Hour); err != nil {
		return nil, err
	}
	return nil, nil
}

func main() {
	durable.Start(handler)
}
