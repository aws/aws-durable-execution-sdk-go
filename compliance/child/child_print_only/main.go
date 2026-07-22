// Command child_print_only implements conformance requirement 3-17: a
// child context that only prints to stdout and returns (no durable
// operations inside), followed by a wait that causes a replay.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	result, err := durable.RunInChildContext(ctx, "print-child", func(_ durable.Context) (string, error) {
		fmt.Println(event)
		return event, nil
	})
	if err != nil {
		return "", err
	}

	if err := durable.Wait(ctx, "", 1*time.Second); err != nil {
		return "", err
	}

	return result, nil
}

func main() {
	durable.Start(handler)
}
