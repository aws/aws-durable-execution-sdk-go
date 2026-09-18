// Command map_throw_if_error implements conformance requirement 9-6: Map where
// the handler returns the batch error, propagating an item failure.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func handler(ctx durable.Context, _ any) ([]string, error) {
	items := []string{"fail", "never"}
	result, err := durable.Map(ctx, "throwing", items, func(_ durable.Context, item string, _ int) (string, error) {
		if item == "fail" {
			return "", errors.New("item failed")
		}
		return item, nil
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(0)}))
	// The item failure is returned as a *durable.BatchError; returning it
	// fails the execution.
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler)
}
