// Command concurrent-callback-submitter demonstrates WaitForCallback inside
// concurrent Go child contexts. Multiple callbacks run in parallel, each
// with its own self-completing submitter that tolerates idempotent retries.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result reports the parallel callback outcomes.
type Result struct {
	Results      []string `json:"results"`
	AllCompleted bool     `json:"allCompleted"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	f1 := durable.Go(ctx, "cb-1", func(childCtx durable.Context) (string, error) {
		return durable.WaitForCallback[string](childCtx, "wait-for-callback-1",
			func(sctx durable.StepContext, callbackID string) error {
				return completeCallback(sctx, callbackID, "callback-1-done")
			},
			durable.WithCallbackTimeout(30*time.Second),
		)
	})

	f2 := durable.Go(ctx, "cb-2", func(childCtx durable.Context) (string, error) {
		return durable.WaitForCallback[string](childCtx, "wait-for-callback-2",
			func(sctx durable.StepContext, callbackID string) error {
				return completeCallback(sctx, callbackID, "callback-2-done")
			},
			durable.WithCallbackTimeout(30*time.Second),
		)
	})

	results, err := durable.All(ctx, "all-callbacks", []*durable.Future[string]{f1, f2})
	if err != nil {
		return Result{}, err
	}

	return Result{Results: results, AllCompleted: true}, nil
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
	// Tolerate "already completed" on idempotent retry — the callback was
	// completed on a prior invocation but the step checkpoint didn't land
	// before suspension.
	if err != nil && strings.Contains(err.Error(), "CallbackTimeoutException") {
		return nil
	}
	return err
}

func main() { durable.Start(handler) }
