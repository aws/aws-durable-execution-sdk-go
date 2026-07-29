// Command wait-callback-multiple-invocations demonstrates multiple
// WaitForCallback operations interspersed with waits and steps across
// different invocation cycles.
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

type StepData struct {
	Processed bool `json:"processed"`
	Step      int  `json:"step"`
}

type CallbackData struct {
	Step int `json:"step"`
}

type Result struct {
	FirstCallback  CallbackData `json:"firstCallback"`
	SecondCallback CallbackData `json:"secondCallback"`
	StepResult     StepData     `json:"stepResult"`
	Invocations    string       `json:"invocationCount"`
}

func selfComplete(ctx context.Context, callbackID string, data CallbackData) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	client := lambdasvc.NewFromConfig(cfg)
	result, _ := json.Marshal(data)
	_, err = client.SendDurableExecutionCallbackSuccess(
		ctx,
		&lambdasvc.SendDurableExecutionCallbackSuccessInput{
			CallbackId: &callbackID,
			Result:     result,
		})
	return err
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// First wait.
	if err := durable.Wait(ctx, "wait-invocation-1", 1*time.Second); err != nil {
		return Result{}, err
	}

	// First callback.
	cb1Result, err := durable.WaitForCallback[CallbackData](ctx, "first-callback",
		func(sctx durable.StepContext, callbackID string) error {
			return selfComplete(sctx, callbackID, CallbackData{Step: 1})
		},
		durable.WithCallbackTimeout(30*time.Second),
	)
	if err != nil {
		return Result{}, fmt.Errorf("first callback: %w", err)
	}

	// Step between callbacks.
	stepResult, err := durable.Step[StepData](ctx, "process-callback-data",
		func(sctx durable.StepContext) (StepData, error) {
			return StepData{Processed: true, Step: 1}, nil
		})
	if err != nil {
		return Result{}, err
	}

	// Second wait.
	if err := durable.Wait(ctx, "wait-invocation-2", 1*time.Second); err != nil {
		return Result{}, err
	}

	// Second callback.
	cb2Result, err := durable.WaitForCallback[CallbackData](ctx, "second-callback",
		func(sctx durable.StepContext, callbackID string) error {
			return selfComplete(sctx, callbackID, CallbackData{Step: 2})
		},
		durable.WithCallbackTimeout(30*time.Second),
	)
	if err != nil {
		return Result{}, fmt.Errorf("second callback: %w", err)
	}

	return Result{
		FirstCallback:  cb1Result,
		SecondCallback: cb2Result,
		StepResult:     stepResult,
		Invocations:    "multiple",
	}, nil
}

func main() { durable.Start(handler) }
