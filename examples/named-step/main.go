// Command named-step demonstrates naming a step for observability. Named
// steps appear with their label in checkpoint traces, making it easier to
// follow execution progress in the console.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input is the event payload for this example.
type Input struct {
	Data string `json:"data"`
}

func handler(ctx durable.Context, event Input) (string, error) {
	data := event.Data
	if data == "" {
		data = "default"
	}
	return durable.Step(ctx, "process-data", func(_ durable.StepContext) (string, error) {
		return fmt.Sprintf("processed: %s", data), nil
	})
}

func main() { durable.Start(handler) }
