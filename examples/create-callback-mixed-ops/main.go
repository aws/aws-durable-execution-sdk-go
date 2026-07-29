// Command create-callback-mixed-ops demonstrates CreateCallback mixed
// with Step and Wait operations in a single workflow.
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Void is a placeholder for steps returning no value.
type Void = struct{}

type UserData struct {
	UserID int    `json:"userId"`
	Name   string `json:"name"`
}

type Result struct {
	StepResult     UserData `json:"stepResult"`
	CallbackResult string   `json:"callbackResult"`
	Completed      bool     `json:"completed"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// Step before callback.
	stepResult, err := durable.Step[UserData](ctx, "fetch-data",
		func(sctx durable.StepContext) (UserData, error) {
			return UserData{UserID: 123, Name: "John Doe"}, nil
		})
	if err != nil {
		return Result{}, err
	}

	// CreateCallback.
	cb, err := durable.CreateCallback[string](ctx, "process-user",
		durable.WithCallbackTimeout(30*time.Second))
	if err != nil {
		return Result{}, err
	}

	// Wait between create and completion.
	if err := durable.Wait(ctx, "initial-wait", 1*time.Second); err != nil {
		return Result{}, err
	}

	// Complete callback in a Step.
	callbackID := cb.ID()
	_, err = durable.Step[Void](ctx, "send-callback", func(sctx durable.StepContext) (Void, error) {
		cfg, err := config.LoadDefaultConfig(sctx)
		if err != nil {
			return Void{}, fmt.Errorf("load config: %w", err)
		}
		client := lambdasvc.NewFromConfig(cfg)
		result, _ := json.Marshal("processed")
		_, err = client.SendDurableExecutionCallbackSuccess(
			sctx,
			&lambdasvc.SendDurableExecutionCallbackSuccessInput{
				CallbackId: &callbackID,
				Result:     result,
			})
		return Void{}, err
	})
	if err != nil {
		return Result{}, err
	}

	callbackResult, err := cb.Result()
	if err != nil {
		return Result{}, err
	}

	return Result{
		StepResult:     stepResult,
		CallbackResult: callbackResult,
		Completed:      true,
	}, nil
}

func main() { durable.Start(handler) }
