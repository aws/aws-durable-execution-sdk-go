// Conformance 3-17: Child context prints once, verifies no re-execution on replay.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	return durable.RunInChildContext(ctx, "print-child", func(child durable.Context) (string, error) {
		fmt.Println(event)
		return event, nil
	})
}

func main() {
	durable.Start(handler)
}
