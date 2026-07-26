// Command parallel-min-successful-callback demonstrates a Parallel
// operation with MinSuccessful completion config where branches include
// both Step operations and WaitForCallback operations. The batch completes
// early once enough branches succeed, regardless of whether those are
// step-based or callback-based branches.
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

type Output struct {
	SuccessCount     int    `json:"successCount"`
	TotalCount       int    `json:"totalCount"`
	CompletionReason string `json:"completionReason"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	results, err := durable.Parallel(ctx, "min-successful-callback-branches", []durable.Branch[string]{
		{Name: "step-1", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "step-1", func(_ durable.StepContext) (string, error) {
				return "step-1 done", nil
			})
		}},
		{Name: "callback-1", Func: func(ctx durable.Context) (string, error) {
			return durable.WaitForCallback[string](ctx, "callback-1",
				func(_ durable.StepContext, callbackID string) error {
					cfg, err := config.LoadDefaultConfig(context.Background())
					if err != nil {
						return fmt.Errorf("load config: %w", err)
					}
					client := lambdasvc.NewFromConfig(cfg)
					payload, _ := json.Marshal("callback-1 result")
					_, err = client.SendDurableExecutionCallbackSuccess(
						context.Background(),
						&lambdasvc.SendDurableExecutionCallbackSuccessInput{
							CallbackId: &callbackID,
							Result:     payload,
						})
					return err
				},
				durable.WithCallbackTimeout(30*time.Second))
		}},
		{Name: "step-2", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "step-2", func(_ durable.StepContext) (string, error) {
				return "step-2 done", nil
			})
		}},
		{Name: "callback-2", Func: func(ctx durable.Context) (string, error) {
			return durable.WaitForCallback[string](ctx, "callback-2",
				func(_ durable.StepContext, callbackID string) error {
					cfg, err := config.LoadDefaultConfig(context.Background())
					if err != nil {
						return fmt.Errorf("load config: %w", err)
					}
					client := lambdasvc.NewFromConfig(cfg)
					payload, _ := json.Marshal("callback-2 result")
					_, err = client.SendDurableExecutionCallbackSuccess(
						context.Background(),
						&lambdasvc.SendDurableExecutionCallbackSuccessInput{
							CallbackId: &callbackID,
							Result:     payload,
						})
					return err
				},
				durable.WithCallbackTimeout(30*time.Second))
		}},
		{Name: "step-3", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "step-3", func(_ durable.StepContext) (string, error) {
				return "step-3 done", nil
			})
		}},
	}, durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 3}))
	if err != nil {
		return Output{}, err
	}

	return Output{
		SuccessCount:     results.SuccessCount(),
		TotalCount:       results.TotalCount(),
		CompletionReason: results.Reason.String(),
	}, nil
}

func main() { durable.Start(handler) }
