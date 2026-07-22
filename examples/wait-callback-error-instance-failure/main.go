// Command wait-callback-error-instance-failure verifies that
// CallbackError from an external failure is correctly propagated
// through replay cycles.
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Result struct {
	IsCallbackError bool   `json:"isCallbackError"`
	ErrorMessage    string `json:"errorMessage,omitempty"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	var cbErr error

	_, err := durable.WaitForCallback[string](ctx, "failure-test",
		func(_ durable.StepContext, callbackID string) error {
			cfg, err := config.LoadDefaultConfig(context.Background())
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			client := lambdasvc.NewFromConfig(cfg)
			errMsg := "external failure"
			errType := "CallbackError"
			_, err = client.SendDurableExecutionCallbackFailure(
				context.Background(),
				&lambdasvc.SendDurableExecutionCallbackFailureInput{
					CallbackId: &callbackID,
					Error: &types.ErrorObject{
						ErrorMessage: &errMsg,
						ErrorType:    &errType,
					},
				})
			return err
		},
		durable.WithCallbackTimeout(30*time.Second),
	)
	if err != nil {
		cbErr = err
	}

	// Wait to force replay.
	if waitErr := durable.Wait(ctx, "post-failure-wait", 1*time.Second); waitErr != nil {
		return Result{}, waitErr
	}

	// Verify error type persists through replay.
	result, err := durable.Step[Result](ctx, "check-error-type",
		func(_ durable.StepContext) (Result, error) {
			var callbackErr *durable.CallbackError
			isCbErr := errors.As(cbErr, &callbackErr)
			msg := ""
			if cbErr != nil {
				msg = cbErr.Error()
			}
			return Result{IsCallbackError: isCbErr, ErrorMessage: msg}, nil
		})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func main() { durable.Start(handler) }
