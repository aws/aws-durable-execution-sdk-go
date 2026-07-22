// Command wait-named demonstrates [durable.Wait] with a custom name,
// improving observability and debuggability of execution traces.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	// Named waits appear in execution traces with their label,
	// making it easier to distinguish between multiple waits.
	if err := durable.Wait(ctx, "wait-2-seconds", 2*time.Second); err != nil {
		return "", err
	}
	return "wait finished", nil
}

func main() { durable.Start(handler) }
