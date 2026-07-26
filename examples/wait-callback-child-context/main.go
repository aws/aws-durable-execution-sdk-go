// Command wait-callback-child-context demonstrates WaitForCallback
// operations within child contexts. A parent-level callback is followed
// by a child context that contains its own callback.
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

type ParentData struct {
	ParentData string `json:"parentData"`
}

type ChildData struct {
	ChildData int `json:"childData"`
}

type ChildResult struct {
	ChildResult    ChildData `json:"childResult"`
	ChildProcessed bool      `json:"childProcessed"`
}

type Result struct {
	ParentResult   ParentData  `json:"parentResult"`
	ChildCtxResult ChildResult `json:"childContextResult"`
}

func selfComplete[T any](callbackID string, data T) error {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return err
	}
	client := lambdasvc.NewFromConfig(cfg)
	result, _ := json.Marshal(data)
	_, err = client.SendDurableExecutionCallbackSuccess(
		context.Background(),
		&lambdasvc.SendDurableExecutionCallbackSuccessInput{
			CallbackId: &callbackID,
			Result:     result,
		})
	return err
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// Parent-level callback.
	parentResult, err := durable.WaitForCallback[ParentData](ctx, "parent-callback-op",
		func(_ durable.StepContext, callbackID string) error {
			return selfComplete(callbackID, ParentData{ParentData: "parent-value"})
		},
		durable.WithCallbackTimeout(30*time.Second),
	)
	if err != nil {
		return Result{}, err
	}

	// Child context with its own callback.
	childResult, err := durable.RunInChildContext[ChildResult](ctx, "child-context-with-callback",
		func(childCtx durable.Context) (ChildResult, error) {
			if err := durable.Wait(childCtx, "child-wait", 1*time.Second); err != nil {
				return ChildResult{}, err
			}

			childCb, err := durable.WaitForCallback[ChildData](childCtx, "child-callback-op",
				func(_ durable.StepContext, callbackID string) error {
					return selfComplete(callbackID, ChildData{ChildData: 42})
				},
				durable.WithCallbackTimeout(30*time.Second),
			)
			if err != nil {
				return ChildResult{}, fmt.Errorf("child callback: %w", err)
			}

			return ChildResult{ChildResult: childCb, ChildProcessed: true}, nil
		})
	if err != nil {
		return Result{}, err
	}

	return Result{ParentResult: parentResult, ChildCtxResult: childResult}, nil
}

func main() { durable.Start(handler) }
