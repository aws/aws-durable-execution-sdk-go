// Command map_min_successful implements conformance requirement 9-7: Map with
// a min-successful completion config stops early once enough items succeed.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (map[string]any, error) {
	items := []string{"a", "b", "c", "d"}
	result, err := durable.Map(ctx, "min-successful", items, func(_ durable.Context, item string, _ int) (string, error) {
		return item, nil
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 2}))
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"completionReason": result.Reason.String(),
		"successCount":     result.SuccessCount(),
		"totalCount":       result.TotalCount(),
	}, nil
}

func main() {
	durable.Start(handler)
}
