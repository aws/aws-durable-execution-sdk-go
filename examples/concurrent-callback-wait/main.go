// Command concurrent-callback-wait demonstrates durable.All combining a
// WaitAsync future with a WaitForCallback future — both run concurrently
// and the workflow completes only when both settle.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result reports the elapsed wall-clock time showing concurrent resolution.
type Result struct {
	ElapsedMs int64 `json:"elapsedMs"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	before, err := durable.Step(ctx, "before", func(sctx durable.StepContext) (int64, error) {
		return time.Now().UnixMilli(), nil
	})
	if err != nil {
		return Result{}, err
	}

	// Wrap WaitForCallback in a Go child context so it becomes a Future[Void].
	cbFuture := durable.Go(ctx, "cb-wrap", func(childCtx durable.Context) (durable.Void, error) {
		_, err := durable.WaitForCallback[string](childCtx, "callback",
			func(sctx durable.StepContext, callbackID string) error {
				return completeCallback(sctx, callbackID, "callback-done")
			},
			durable.WithCallbackTimeout(30*time.Second),
		)
		return durable.Void{}, err
	})

	waitFuture := durable.WaitAsync(ctx, "wait-1s", 1*time.Second)

	// All waits for both: callback and wait timer.
	_, err = durable.All(ctx, "all", []*durable.Future[durable.Void]{waitFuture, cbFuture})
	if err != nil {
		return Result{}, err
	}

	after, err := durable.Step(ctx, "after", func(sctx durable.StepContext) (int64, error) {
		return time.Now().UnixMilli(), nil
	})
	if err != nil {
		return Result{}, err
	}

	return Result{ElapsedMs: after - before}, nil
}

func completeCallback(ctx context.Context, callbackID, value string) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	client := lambdasvc.NewFromConfig(cfg)
	result, _ := json.Marshal(value)
	_, err = client.SendDurableExecutionCallbackSuccess(ctx,
		&lambdasvc.SendDurableExecutionCallbackSuccessInput{
			CallbackId: &callbackID,
			Result:     result,
		},
	)
	return err
}

func main() { durable.Start(handler) }
