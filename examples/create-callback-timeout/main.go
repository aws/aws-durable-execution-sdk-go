// Command create-callback-timeout demonstrates CreateCallback with a
// short timeout. No external system completes the callback, so it times
// out and the error is caught and returned.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Result struct {
	TimedOut bool   `json:"timedOut"`
	Error    string `json:"error,omitempty"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	cb, err := durable.CreateCallback[string](ctx, "timeout-callback",
		durable.WithCallbackTimeout(3*time.Second))
	if err != nil {
		return Result{}, err
	}

	_, err = cb.Result()
	if err != nil {
		var cbErr *durable.CallbackError
		if errors.As(err, &cbErr) && errors.Is(cbErr.Err, durable.ErrCallbackTimedOut) {
			return Result{TimedOut: true, Error: cbErr.Error()}, nil
		}
		return Result{}, err
	}
	return Result{TimedOut: false}, nil
}

func main() { durable.Start(handler) }
