// Command map_tolerated_pct implements conformance requirement 9-10: Map stops
// early once the failure percentage exceeds tolerated-failure-percentage.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func handler(ctx durable.Context, _ any) (map[string]any, error) {
	items := []string{"fail0", "fail1", "ok2", "ok3"}
	result, err := durable.Map(ctx, "tolerated-pct", items, func(_ durable.Context, item string, _ int) (string, error) {
		if item == "fail0" || item == "fail1" {
			return "", errors.New("item failed")
		}
		return item, nil
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{ToleratedFailurePercentage: aws.Int(25)}))
	// Failed items are reported as a *durable.BatchError alongside the
	// populated result; this handler reports the result. Any other error is
	// an SDK failure and propagates.
	var berr *durable.BatchError
	if err != nil && !errors.As(err, &berr) {
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
