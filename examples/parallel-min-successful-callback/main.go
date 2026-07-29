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
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

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
			return durable.Step(ctx, "step-1", func(sctx durable.StepContext) (string, error) {
				return "step-1 done", nil
			})
		}},
		{Name: "callback-1", Func: func(ctx durable.Context) (string, error) {
			return durable.WaitForCallback[string](ctx, "callback-1",
				func(sctx durable.StepContext, callbackID string) error {
					return sendCallback(sctx, callbackID, "callback-1 result")
				},
				durable.WithCallbackTimeout(30*time.Second))
		}},
		{Name: "step-2", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "step-2", func(sctx durable.StepContext) (string, error) {
				return "step-2 done", nil
			})
		}},
		{Name: "callback-2", Func: func(ctx durable.Context) (string, error) {
			return durable.WaitForCallback[string](ctx, "callback-2",
				func(sctx durable.StepContext, callbackID string) error {
					return sendCallback(sctx, callbackID, "callback-2 result")
				},
				durable.WithCallbackTimeout(30*time.Second))
		}},
		{Name: "step-3", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "step-3", func(sctx durable.StepContext) (string, error) {
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

// sendCallback delegates callback completion to the companion
// callback-sender Lambda using fire-and-forget (Event) invocation. The
// submitter step therefore completes as soon as the invoke is accepted,
// independently of when the callback itself resolves. Sending the callback
// inline instead lets it resolve before the step checkpoint lands, and the
// service rejects that checkpoint once batch completion is under way.
//
// An Event invocation returns once accepted, so a retried submitter simply
// asks the sender to resolve an already-resolved callback, which the sender
// absorbs.
func sendCallback(ctx context.Context, callbackID, value string) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	client := lambda.NewFromConfig(cfg)

	senderName := callbackSenderName()
	payload, _ := json.Marshal(map[string]any{
		"callbackId": callbackID,
		"action":     "success",
		"result":     json.RawMessage(mustMarshal(value)),
	})

	_, err = client.Invoke(ctx, &lambda.InvokeInput{
		FunctionName:   &senderName,
		InvocationType: types.InvocationTypeEvent,
		Payload:        payload,
	})
	return err
}

// callbackSenderName derives the callback-sender function name from the
// current function's name. Both follow the pattern <prefix>go-<example>.
func callbackSenderName() string {
	name := os.Getenv("AWS_LAMBDA_FUNCTION_NAME")
	if idx := strings.LastIndex(name, "go-"); idx >= 0 {
		return name[:idx] + "go-callback-sender"
	}
	return "go-callback-sender"
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func main() { durable.Start(handler) }
