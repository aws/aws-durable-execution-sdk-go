// Command map_tolerated_within implements conformance requirement 9-8: Map with
// tolerated-failure-count=1 tolerating one failure, all items complete.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func handler(ctx durable.Context, _ any) (map[string]any, error) {
	items := []string{"ok", "fail", "ok2"}
	result, err := durable.Map(ctx, "tolerated", items, func(_ durable.Context, item string, _ int) (string, error) {
		if item == "fail" {
			return "", errors.New("item failed")
		}
		return item, nil
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(1)}))
	if err != nil {
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
