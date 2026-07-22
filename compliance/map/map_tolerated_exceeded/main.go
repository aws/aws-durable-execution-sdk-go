// Command map_tolerated_exceeded implements conformance requirement 9-9: Map
// stops early once the failure count exceeds tolerated-failure-count.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (map[string]any, error) {
	items := []string{"fail0", "fail1", "never"}
	result, err := durable.Map(ctx, "tolerated-exceeded", items, func(_ durable.Context, item string, _ int) (string, error) {
		if item != "never" {
			return "", errors.New("item failed")
		}
		return item, nil
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.WithToleratedFailureCount(1)))
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"completionReason": result.Reason.String(),
		"successCount":     result.SuccessCount(),
		"failureCount":     result.FailureCount(),
		"totalCount":       result.TotalCount(),
	}, nil
}

func main() {
	durable.Start(handler)
}
