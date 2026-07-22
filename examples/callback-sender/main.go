// Command callback-sender is a companion Lambda that completes callbacks
// externally. It receives a callback ID and action (success, failure, or
// heartbeat) and calls the corresponding Lambda API. Used by callback
// examples that need external completion.
package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// Input configures what callback action to perform.
type Input struct {
	// CallbackID is the callback identifier from the durable execution.
	CallbackID string `json:"callbackId"`
	// Action is one of "success", "failure", "heartbeat".
	Action string `json:"action"`
	// Result is the JSON payload for success responses.
	Result json.RawMessage `json:"result,omitempty"`
	// ErrorMessage is used for failure responses.
	ErrorMessage string `json:"errorMessage,omitempty"`
	// ErrorType is used for failure responses.
	ErrorType string `json:"errorType,omitempty"`
}

// Output reports the outcome.
type Output struct {
	Action     string `json:"action"`
	CallbackID string `json:"callbackId"`
	Success    bool   `json:"success"`
}

func handler(ctx context.Context, event Input) (Output, error) {
	if event.CallbackID == "" {
		return Output{}, fmt.Errorf("callbackId is required")
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return Output{}, fmt.Errorf("load AWS config: %w", err)
	}
	client := lambdasvc.NewFromConfig(cfg)

	switch event.Action {
	case "success":
		result := event.Result
		if result == nil {
			result = []byte(`"completed"`)
		}
		_, err = client.SendDurableExecutionCallbackSuccess(ctx, &lambdasvc.SendDurableExecutionCallbackSuccessInput{
			CallbackId: &event.CallbackID,
			Result:     result,
		})
	case "failure":
		errMsg := event.ErrorMessage
		if errMsg == "" {
			errMsg = "external failure"
		}
		errType := event.ErrorType
		if errType == "" {
			errType = "CallbackError"
		}
		_, err = client.SendDurableExecutionCallbackFailure(ctx, &lambdasvc.SendDurableExecutionCallbackFailureInput{
			CallbackId: &event.CallbackID,
			Error: &types.ErrorObject{
				ErrorMessage: &errMsg,
				ErrorType:    &errType,
			},
		})
	case "heartbeat":
		_, err = client.SendDurableExecutionCallbackHeartbeat(ctx, &lambdasvc.SendDurableExecutionCallbackHeartbeatInput{
			CallbackId: &event.CallbackID,
		})
	default:
		return Output{}, fmt.Errorf("unknown action: %s (must be success, failure, or heartbeat)", event.Action)
	}

	if err != nil {
		return Output{}, fmt.Errorf("callback %s for %s: %w", event.Action, event.CallbackID, err)
	}

	return Output{Action: event.Action, CallbackID: event.CallbackID, Success: true}, nil
}

func main() {
	lambda.Start(handler)
}
