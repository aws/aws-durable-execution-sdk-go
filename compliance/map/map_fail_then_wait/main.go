// Command map_fail_then_wait implements conformance requirement 9-18:
// Suspension after a map that completed with a failure; on replay the
// completed map (including the failed iteration) is skipped.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func handler(ctx durable.Context, _ any) (map[string]any, error) {
	items := []string{"ok", "fail"}
	result, err := durable.Map(ctx, "fail-then-wait", items, func(_ durable.Context, item string, _ int) (string, error) {
		if item == "fail" {
			return "", errors.New("item failed")
		}
		return item, nil
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(1)}))
	if err != nil {
		return nil, err
	}
	if err := durable.Wait(ctx, "", 1*time.Second); err != nil {
		return nil, err
	}
	return map[string]any{
		"completionReason": result.Reason.String(),
		"status":           result.Status().String(),
		"successCount":     result.SuccessCount(),
		"failureCount":     result.FailureCount(),
		"totalCount":       result.TotalCount(),
	}, nil
}

func main() {
	durable.Start(handler)
}
