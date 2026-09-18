// Command force-checkpoint-callback demonstrates force-checkpoint polling
// when a long-running step in one parallel branch blocks invocation
// termination while another branch performs sequential callbacks. The
// runtime's force-checkpoint mechanism ensures callbacks are checkpointed
// even though the long-running branch hasn't yielded.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	results, err := durable.Parallel(ctx, "force-cp-block", []durable.Branch[any]{
		// Branch 1: Long-running step that blocks invocation termination.
		{Name: "long-running", Func: func(branchCtx durable.Context) (any, error) {
			return durable.Step(branchCtx, "long-running-step", func(sctx durable.StepContext) (string, error) {
				time.Sleep(10 * time.Second)
				return "long-complete", nil
			})
		}},
		// Branch 2: Sequential callbacks that need force checkpoint.
		{Name: "callbacks", Func: func(branchCtx durable.Context) (any, error) {
			_, err := durable.WaitForCallback[string](branchCtx, "callback-1",
				func(sctx durable.StepContext, callbackID string) error {
					return completeCallback(sctx, callbackID, "cb1-done")
				},
				durable.WithCallbackTimeout(30*time.Second),
			)
			if err != nil {
				return nil, err
			}

			_, err = durable.WaitForCallback[string](branchCtx, "callback-2",
				func(sctx durable.StepContext, callbackID string) error {
					return completeCallback(sctx, callbackID, "cb2-done")
				},
				durable.WithCallbackTimeout(30*time.Second),
			)
			if err != nil {
				return nil, err
			}

			_, err = durable.WaitForCallback[string](branchCtx, "callback-3",
				func(sctx durable.StepContext, callbackID string) error {
					return completeCallback(sctx, callbackID, "cb3-done")
				},
				durable.WithCallbackTimeout(30*time.Second),
			)
			if err != nil {
				return nil, err
			}

			return "callbacks-complete", nil
		}},
	},
		// Tolerate a failed branch so the long-running branch still runs to
		// completion; the default completion policy would stop the batch at
		// the first failure.
		durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(1)}))
	// A failed branch is reported as a *durable.BatchError alongside the
	// populated result; this handler reports the result. Any other error is
	// an SDK failure and propagates.
	var berr *durable.BatchError
	if err != nil && !errors.As(err, &berr) {
		return "", err
	}

	b, _ := json.Marshal(results)
	return string(b), nil
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
		})
	return err
}

func main() { durable.Start(handler) }
