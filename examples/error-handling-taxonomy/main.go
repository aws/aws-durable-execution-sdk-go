// Command error-handling-taxonomy demonstrates the SDK's typed error
// hierarchy. Each durable operation that can fail produces a specific error
// type (StepError, InvokeError, CallbackError) that is matchable with
// errors.As and participates in the OperationError super-type.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Void is a placeholder for steps returning no value.
type Void = struct{}

// Output records the taxonomy of each error the handler encountered.
type Output struct {
	StepErrorInfo     ErrorInfo `json:"stepErrorInfo"`
	InvokeErrorInfo   ErrorInfo `json:"invokeErrorInfo"`
	CallbackErrorInfo ErrorInfo `json:"callbackErrorInfo"`
}

// ErrorInfo captures the key fields extracted from a typed error via errors.As.
type ErrorInfo struct {
	Matched       bool   `json:"matched"`
	TypeName      string `json:"typeName"`
	OperationName string `json:"operationName"`
	Attempts      int    `json:"attempts,omitempty"`
	IsOpError     bool   `json:"isOpError"`
	OpErrorName   string `json:"opErrorName,omitempty"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	var out Output

	// 1. Provoke a StepError: a step that always fails with no retry.
	_, err := durable.Step(ctx, "failing-step",
		func(_ durable.StepContext) (any, error) {
			return nil, fmt.Errorf("deliberate step failure")
		},
		durable.WithRetry(durable.NoRetry()),
	)
	if err != nil {
		var stepErr *durable.StepError
		if errors.As(err, &stepErr) {
			var opErr *durable.OperationError
			isOp := errors.As(err, &opErr)
			out.StepErrorInfo = ErrorInfo{
				Matched:       true,
				TypeName:      "StepError",
				OperationName: stepErr.Name,
				Attempts:      stepErr.Attempts,
				IsOpError:     isOp,
			}
			if isOp {
				out.StepErrorInfo.OpErrorName = opErr.Name
			}
		}
	}

	// 2. Provoke an InvokeError: invoke the retry-invoke-target function
	// with an attempt below its failure threshold so it fails
	// deliberately. The target function name is resolved the same way as
	// in the retry-invoke example. In local testing the invoke suspends
	// and the test fails it externally via runner.FailChainedInvoke.
	targetFunction := os.Getenv("FUNCTION_NAME_PREFIX") + "go-retry-invoke-target:$LATEST"
	_, err = durable.Invoke[any](ctx, "failing-invoke", targetFunction,
		struct {
			Attempt          int `json:"attempt"`
			FailUntilAttempt int `json:"failUntilAttempt"`
		}{Attempt: 1, FailUntilAttempt: 2},
	)
	var invokeErr *durable.InvokeError
	switch {
	case err == nil:
		// Unexpected success; leave the info zeroed.
	case errors.As(err, &invokeErr):
		var opErr *durable.OperationError
		isOp := errors.As(err, &opErr)
		out.InvokeErrorInfo = ErrorInfo{
			Matched:       true,
			TypeName:      "InvokeError",
			OperationName: invokeErr.Name,
			IsOpError:     isOp,
		}
		if isOp {
			out.InvokeErrorInfo.OpErrorName = opErr.Name
		}
	default:
		// Not the handled terminal type: propagate unchanged so
		// suspension signals reach the SDK.
		return Output{}, err
	}

	// 3. Provoke a CallbackError: create a callback then send an external
	// failure via the Lambda API. The failure resolves the callback with a
	// typed CallbackError, distinct from the StepError a submitter failure
	// would produce.
	cb, err := durable.CreateCallback[string](ctx, "failing-callback",
		durable.WithCallbackTimeout(30*time.Second))
	if err != nil {
		return Output{}, err
	}

	// Send the failure in a step. In cloud the API resolves the callback;
	// in local testing the step fails (no endpoint) and the test resolves
	// the callback externally via runner.SendCallbackFailure.
	callbackID := cb.ID()
	_, err = durable.Step[Void](ctx, "send-callback-failure", func(_ durable.StepContext) (Void, error) {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return Void{}, fmt.Errorf("load config: %w", err)
		}
		client := lambdasvc.NewFromConfig(cfg)
		errMsg := "deliberate callback failure"
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
		return Void{}, err
	}, durable.WithRetry(durable.NoRetry()))
	_ = err // Step failure is expected locally; cloud succeeds and resolves the callback.

	_, err = cb.Result()
	var cbErr *durable.CallbackError
	switch {
	case err == nil:
		// Unexpected success; leave the info zeroed.
	case errors.As(err, &cbErr):
		var opErr *durable.OperationError
		isOp := errors.As(err, &opErr)
		out.CallbackErrorInfo = ErrorInfo{
			Matched:       true,
			TypeName:      "CallbackError",
			OperationName: cbErr.Name,
			IsOpError:     isOp,
		}
		if isOp {
			out.CallbackErrorInfo.OpErrorName = opErr.Name
		}
	default:
		// Not the handled terminal type: propagate unchanged so
		// suspension signals reach the SDK.
		return Output{}, err
	}

	return out, nil
}

func main() { durable.Start(handler) }
