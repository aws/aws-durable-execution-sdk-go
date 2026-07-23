// Requirement 4-16: Callback with custom serdes (numeric).
//
// From test-requirements/callback/4-16.yaml:
//
//	description: Custom serdes converts callback string into a number
//	handler: |
//	  Handler creates a callback using the input as the callback name
//	  with a custom deserializer that parses the payload into a number.
//	  The handler returns a structured output containing the
//	  deserialized count and its doubled value.
//	invocations: |
//	  - Handler creates a callback with a custom deserializer. The SDK
//	    checkpoints CallbackStarted. The invocation completes and
//	    execution suspends.
//	  - Replay 1: Received callback success. The SDK invokes the custom
//	    deserializer which converts the payload into a number. The
//	    handler returns the numeric result, and execution succeeds.
//	CallbackActions:
//	  - Payload: 42
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: {count: 42, doubled: 84}
//
// callback416Serdes.Deserialize parses the raw payload string (the
// external system's Payload: 42 arrives as the literal JSON text "42",
// not a quoted string) directly into an int64 via json.Unmarshal -
// simpler than 4-15's struct case, but the same types.Serdes shape.
package handlers

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// callback416Result is this handler's own returned output shape, matching
// ExpectedResult.Result's {count, doubled} envelope.
type callback416Result struct {
	Count   int64 `json:"count"`
	Doubled int64 `json:"doubled"`
}

type callback416Serdes struct{}

func (callback416Serdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("callback416Serdes: serialize: %w", err)
	}
	return string(b), nil
}

func (callback416Serdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	var n int64
	if err := json.Unmarshal([]byte(pointer), &n); err != nil {
		return nil, fmt.Errorf("callback416Serdes: deserialize: %w", err)
	}
	return n, nil
}

func init() {
	Register("4-16", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_16Handler, config(client))
	})
}

func callback4_16Handler(event string, dc types.DurableContext) (callback416Result, error) {
	resultCh, _, err := operations.CreateCallback[int64](dc, event, operations.WithCallbackSerdes[int64](callback416Serdes{}))
	if err != nil {
		return callback416Result{}, err
	}
	result := operations.AwaitCallback(dc, resultCh)
	if result.Err != nil {
		return callback416Result{}, result.Err
	}
	return callback416Result{Count: result.Value, Doubled: result.Value * 2}, nil
}
