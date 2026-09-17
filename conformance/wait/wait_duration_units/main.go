// Command wait_duration_units implements conformance requirement 2-4: a
// wait operation expressed in minutes, verifying conversion to seconds.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (any, error) {
	if err := durable.Wait(ctx, "", time.Minute); err != nil {
		return nil, err
	}
	return nil, nil
}

func main() {
	durable.Start(handler)
}
