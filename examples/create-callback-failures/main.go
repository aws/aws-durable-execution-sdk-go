// Command create-callback-failures demonstrates callback failure
// scenarios where the callback is failed externally. A durable Step sends
// the failure for idempotent replay. The error is caught and inspected.
package main

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Void is a placeholder for steps returning no value.
type Void = struct{}

type Result struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	cb, err := durable.CreateCallback[string](ctx, "failing-operation",
		durable.WithCallbackTimeout(30*time.Second))
	if err != nil {
		return Result{}, err
	}

	// Send failure in a durable Step for idempotent replay.
	callbackID := cb.ID()
	_, err = durable.Step[Void](ctx, "send-failure", func(_ durable.StepContext) (Void, error) {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return Void{}, err
		}
		client := lambdasvc.NewFromConfig(cfg)
		errMsg := "external system failed"
		errType := "CallbackError"
		_, err = client.SendDurableExecutionCallbackFailure(context.Background(),
			&lambdasvc.SendDurableExecutionCallbackFailureInput{
				CallbackId: &callbackID,
				Error: &types.ErrorObject{
					ErrorMessage: &errMsg,
					ErrorType:    &errType,
				},
			})
		return Void{}, err
	})
	if err != nil {
		return Result{}, err
	}

	_, err = cb.Result()
	if err != nil {
		var cbErr *durable.CallbackError
		if errors.As(err, &cbErr) {
			return Result{Success: false, Error: cbErr.Error()}, nil
		}
		return Result{}, err
	}
	return Result{Success: true}, nil
}

func main() { durable.Start(handler) }
