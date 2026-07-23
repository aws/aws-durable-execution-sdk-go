// Requirement 9-6: Map throw-if-error propagates an item failure to the
// execution.
//
// From test-requirements/map/9-6.yaml:
//
//	description: Map where the handler asks the batch result to
//	  rethrow, propagating an item failure so the execution fails
//	handler: |
//	  Handler invokes the map operation over two items with a fail-fast
//	  completion config (tolerated-failure-count=0). Max-concurrency is
//	  1: item 0 raises an error and fails, so fail-fast stops the
//	  operation and item 1 is never started. The map operation returns a
//	  batch result; the handler then asks the batch result to rethrow,
//	  which rethrows the first item failure. The error is not caught, so
//	  the execution fails.
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//
// # Update: now uses the real BatchResult.ThrowIfError() method directly
//
// operations.Map now always returns a valid BatchResult with a nil
// error, even when the completion policy is unmet (see batch.go's own
// completion-policy-contract fix doc) - this handler now calls the
// batch result's own ThrowIfError() method directly, exactly matching
// the reference SDKs' own "catch the batch result, call rethrow(), let
// it propagate uncaught" idiom this requirement's YAML describes.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("9-6", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_6Handler, config(client))
	})
}

func map9_6Handler(event any, dc types.DurableContext) (*string, error) {
	tolerated := 0
	items := []int{0, 1}

	batch, err := operations.Map(dc, "throwing", items,
		func(child types.DurableContext, item int, index int) (string, error) {
			if item == 0 {
				return "", errMapItemFailed
			}
			return "unreached", nil
		},
		operations.WithMapMaxConcurrency[int, string](1),
		operations.WithMapCompletionConfig[int, string](types.CompletionConfig{ToleratedFailureCount: &tolerated}),
	)
	if err != nil {
		return nil, err
	}
	if throwErr := batch.ThrowIfError(); throwErr != nil {
		return nil, throwErr
	}
	return nil, nil
}
