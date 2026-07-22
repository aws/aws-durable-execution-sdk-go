// Command wait-callback-nested demonstrates nested WaitForCallback
// operations across multiple child context levels.
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

type InnerResult struct {
	InnerCallback string `json:"innerCallback"`
	DeepLevel     string `json:"deepLevel"`
}

type NestedResult struct {
	InnerCallback string      `json:"innerCallback"`
	DeepNested    InnerResult `json:"deepNested"`
	Level         string      `json:"level"`
}

type Result struct {
	OuterCallback string       `json:"outerCallback"`
	NestedResults NestedResult `json:"nestedResults"`
}

func selfComplete(callbackID, value string) error {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return err
	}
	client := lambdasvc.NewFromConfig(cfg)
	result, _ := json.Marshal(value)
	_, err = client.SendDurableExecutionCallbackSuccess(
		context.Background(),
		&lambdasvc.SendDurableExecutionCallbackSuccessInput{
			CallbackId: &callbackID,
			Result:     result,
		})
	return err
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// Outer callback.
	outerResult, err := durable.WaitForCallback[string](ctx, "outer-callback-op",
		func(_ durable.StepContext, callbackID string) error {
			return selfComplete(callbackID, "outer-value")
		},
		durable.WithCallbackTimeout(30*time.Second),
	)
	if err != nil {
		return Result{}, err
	}

	// Nested child context with inner callback.
	nestedResult, err := durable.RunInChildContext[NestedResult](ctx, "outer-child-context",
		func(childCtx durable.Context) (NestedResult, error) {
			innerResult, err := durable.WaitForCallback[string](childCtx, "inner-callback-op",
				func(_ durable.StepContext, callbackID string) error {
					return selfComplete(callbackID, "inner-value")
				},
				durable.WithCallbackTimeout(30*time.Second),
			)
			if err != nil {
				return NestedResult{}, err
			}

			// Deep nested child context with another callback.
			deepResult, err := durable.RunInChildContext[InnerResult](childCtx, "inner-child-context",
				func(deepCtx durable.Context) (InnerResult, error) {
					if err := durable.Wait(deepCtx, "deep-wait", 2*time.Second); err != nil {
						return InnerResult{}, err
					}
					deepCb, err := durable.WaitForCallback[string](deepCtx, "nested-callback-op",
						func(_ durable.StepContext, callbackID string) error {
							return selfComplete(callbackID, "deep-value")
						},
						durable.WithCallbackTimeout(30*time.Second),
					)
					if err != nil {
						return InnerResult{}, err
					}
					return InnerResult{InnerCallback: deepCb, DeepLevel: "inner-child"}, nil
				})
			if err != nil {
				return NestedResult{}, fmt.Errorf("deep nested: %w", err)
			}

			return NestedResult{
				InnerCallback: innerResult,
				DeepNested:    deepResult,
				Level:         "outer-child",
			}, nil
		})
	if err != nil {
		return Result{}, err
	}

	return Result{OuterCallback: outerResult, NestedResults: nestedResult}, nil
}

func main() { durable.Start(handler) }
