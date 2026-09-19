// Command target_slow is the durable target function for the invoke
// timeout scenarios: it waits longer than its configured execution timeout,
// so the calling execution observes a timed-out chained invoke.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	if err := durable.Wait(ctx, "", 600*time.Second); err != nil {
		return "", err
	}
	return "should_not_reach", nil
}

func main() {
	durable.Start(handler)
}
