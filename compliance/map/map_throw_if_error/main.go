// Command map_throw_if_error implements conformance requirement 9-6: Map where
// the handler asks the batch result to rethrow, propagating an item failure.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) ([]string, error) {
	items := []string{"fail", "never"}
	result, err := durable.Map(ctx, "throwing", items, func(_ durable.Context, item string, _ int) (string, error) {
		if item == "fail" {
			return "", errors.New("item failed")
		}
		return item, nil
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.WithToleratedFailureCount(0)))
	if err != nil {
		return nil, err
	}
	if throwErr := result.ThrowIfError(); throwErr != nil {
		return nil, throwErr
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler)
}
