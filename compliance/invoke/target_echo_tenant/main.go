// Command target_echo_tenant is the tenancy-enabled deployment of the echo
// target: identical behavior to target_echo, deployed with a PER_TENANT
// tenancy configuration for the tenant-isolation conformance test.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event any) (any, error) {
	if err := durable.Wait(ctx, "", time.Second); err != nil {
		return nil, err
	}
	return event, nil
}

func main() {
	durable.Start(handler)
}
