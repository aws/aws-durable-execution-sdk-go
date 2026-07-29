// Command wait-callback-failures demonstrates handling of callback failure
// scenarios where the submitter succeeds but sends a failure response via
// SendDurableExecutionCallbackFailure. The error is caught and inspected.
package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Result struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	_, err := durable.WaitForCallback[string](ctx, "failure-callback",
		func(sctx durable.StepContext, callbackID string) error {
			sctx.Logger().Info("Submitter sending failure", "callbackId", callbackID)

			cfg, err := config.LoadDefaultConfig(sctx)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			client := lambdasvc.NewFromConfig(cfg)

			errMsg := "external system failed"
			errType := "CallbackError"
			_, err = client.SendDurableExecutionCallbackFailure(
				sctx,
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
		var cbErr *durable.CallbackError
		if errors.As(err, &cbErr) {
			return Result{Success: false, Error: cbErr.Error()}, nil
		}
		return Result{}, err
	}
	return Result{Success: true}, nil
}

func main() { durable.Start(handler) }
