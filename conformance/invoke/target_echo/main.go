// Command target_echo is the durable target function for invoke
// conformance tests: it waits briefly (so the caller suspends) and echoes
// its input.
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
