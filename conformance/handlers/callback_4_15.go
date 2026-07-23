// Requirement 4-15: Callback with custom serdes (happy path).
//
// From test-requirements/callback/4-15.yaml:
//
//	description: Custom serdes deserializes a JSON callback payload into
//	  a typed object with a Date field
//	handler: |
//	  Handler creates a callback using the input as the callback name
//	  with a custom deserializer that parses the raw JSON string into a
//	  typed object and converts the timestamp string into a second
//	  timestamp. The handler returns the deserialized result as a
//	  structured output.
//	invocations: |
//	  - Handler creates a callback with a custom deserializer. The SDK
//	    checkpoints CallbackStarted. The invocation completes and
//	    execution suspends.
//	  - Replay 1: Received callback success. The SDK invokes the custom
//	    deserializer which converts the payload into a typed object. The
//	    handler returns the deserialized result, and execution succeeds.
//	CallbackActions:
//	  - Payload: {id: ${CB_PAYLOAD_ID}, message: hello, timestamp:
//	      '2026-01-01T00:00:00.000Z'}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    received: {id: ${CB_PAYLOAD_ID}, message: hello, timestamp:
//	      1767225600}
//
// callbackPayload415 is the raw shape the external system's Payload JSON
// deserializes into via encoding/json (id/message/timestamp all strings
// on the wire). callback415Serdes.Deserialize parses that raw JSON
// string, then converts the RFC3339 timestamp into Unix seconds -
// exactly the "converts the timestamp string into a second timestamp"
// scenario the YAML describes - producing a callback415Received value
// (timestamp as an int64 Unix-seconds field) that this handler wraps in
// the ExpectedResult's own {received: {...}} envelope shape.
//
// Only Deserialize is exercised by this requirement (the external system
// produces the payload, not this handler), but Serialize is still
// implemented to fully satisfy types.Serdes - see callback.go's
// CallbackOption[T]/WithCallbackSerdes, which requires a serdes value,
// not merely a deserialize func.
package handlers

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// callbackPayload415 is the wire shape of the external system's raw JSON
// callback payload for this requirement.
type callbackPayload415 struct {
	ID        string `json:"id"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

// callback415Received is the typed value callback415Serdes.Deserialize
// produces - message/id passed through verbatim, Timestamp converted
// from an RFC3339 string into Unix seconds.
type callback415Received struct {
	ID        string `json:"id"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

// callback415Result is this handler's own returned output shape, wrapping
// the deserialized value under a "received" key to match
// ExpectedResult.Result's own {received: {...}} envelope.
type callback415Result struct {
	Received callback415Received `json:"received"`
}

type callback415Serdes struct{}

func (callback415Serdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("callback415Serdes: serialize: %w", err)
	}
	return string(b), nil
}

func (callback415Serdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	var raw callbackPayload415
	if err := json.Unmarshal([]byte(pointer), &raw); err != nil {
		return nil, fmt.Errorf("callback415Serdes: deserialize: %w", err)
	}
	ts, err := time.Parse(time.RFC3339, raw.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("callback415Serdes: parsing timestamp %q: %w", raw.Timestamp, err)
	}
	return callback415Received{ID: raw.ID, Message: raw.Message, Timestamp: ts.Unix()}, nil
}

func init() {
	Register("4-15", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_15Handler, config(client))
	})
}

func callback4_15Handler(event string, dc types.DurableContext) (callback415Result, error) {
	resultCh, _, err := operations.CreateCallback[callback415Received](dc, event, operations.WithCallbackSerdes[callback415Received](callback415Serdes{}))
	if err != nil {
		return callback415Result{}, err
	}
	result := operations.AwaitCallback(dc, resultCh)
	if result.Err != nil {
		return callback415Result{}, result.Err
	}
	return callback415Result{Received: result.Value}, nil
}
