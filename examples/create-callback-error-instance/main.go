// Command create-callback-error-instance verifies that callback errors
// throw correct error instances (timeout and failure) through replay.
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

// Void is a placeholder for steps returning no value.
type Void = struct{}

type ErrorInfo struct {
	IsCallbackError bool   `json:"isCallbackError"`
	IsTimedOut      bool   `json:"isTimedOut,omitempty"`
	ErrorMessage    string `json:"errorMessage,omitempty"`
}

type Result struct {
	TimeoutError ErrorInfo `json:"timeoutError"`
	FailureError ErrorInfo `json:"failureError"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// Test 1: Callback timeout.
	cb1, err := durable.CreateCallback[string](ctx, "timeout-test",
		durable.WithCallbackTimeout(3*time.Second))
	if err != nil {
		return Result{}, err
	}
	_, err1 := cb1.Result()

	// Test 2: Callback failure.
	cb2, err := durable.CreateCallback[string](ctx, "failure-test",
		durable.WithCallbackTimeout(30*time.Second))
	if err != nil {
		return Result{}, err
	}

	// Send failure in a durable Step.
	callbackID := cb2.ID()
	_, err = durable.Step[Void](ctx, "send-failure", func(sctx durable.StepContext) (Void, error) {
		cfg, err := config.LoadDefaultConfig(sctx)
		if err != nil {
			return Void{}, fmt.Errorf("load config: %w", err)
		}
		client := lambdasvc.NewFromConfig(cfg)
		errMsg := "test failure"
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
		return Void{}, err
	})
	if err != nil {
		return Result{}, err
	}
	_, err2 := cb2.Result()

	// Wait to force replay.
	if err := durable.Wait(ctx, "post-errors-wait", 1*time.Second); err != nil {
		return Result{}, err
	}

	// Verify error types in a step.
	result, err := durable.Step[Result](ctx, "check-error-types",
		func(sctx durable.StepContext) (Result, error) {
			var cbErr1 *durable.CallbackError
			isCb1 := errors.As(err1, &cbErr1)
			isTimeout := isCb1 && errors.Is(cbErr1.Err, durable.ErrCallbackTimedOut)

			var cbErr2 *durable.CallbackError
			isCb2 := errors.As(err2, &cbErr2)

			return Result{
				TimeoutError: ErrorInfo{
					IsCallbackError: isCb1,
					IsTimedOut:      isTimeout,
					ErrorMessage:    errStr(err1),
				},
				FailureError: ErrorInfo{
					IsCallbackError: isCb2,
					ErrorMessage:    errStr(err2),
				},
			}, nil
		})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func main() { durable.Start(handler) }
