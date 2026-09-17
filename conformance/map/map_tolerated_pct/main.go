// Command map_tolerated_pct implements conformance requirement 9-10: Map stops
// early once the failure percentage exceeds tolerated-failure-percentage.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (map[string]any, error) {
	items := []string{"fail0", "fail1", "ok2", "ok3"}
	result, err := durable.Map(ctx, "tolerated-pct", items, func(_ durable.Context, item string, _ int) (string, error) {
		if item == "fail0" || item == "fail1" {
			return "", errors.New("item failed")
		}
		return item, nil
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{ToleratedFailurePercentage: 25}))
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
