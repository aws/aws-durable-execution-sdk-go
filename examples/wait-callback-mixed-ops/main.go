// Command wait-callback-mixed-ops demonstrates WaitForCallback combined
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

type UserData struct {
	UserID int    `json:"userId"`
	Name   string `json:"name"`
}

type FinalStep struct {
	Status    string `json:"status"`
	Timestamp int64  `json:"timestamp"`
}

type Result struct {
	StepResult       UserData  `json:"stepResult"`
	CallbackResult   string    `json:"callbackResult"`
	FinalStep        FinalStep `json:"finalStep"`
	WorkflowComplete bool      `json:"workflowCompleted"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// Initial wait.
	if err := durable.Wait(ctx, "initial-wait", 1*time.Second); err != nil {
		return Result{}, err
	}

	// Step to fetch data.
	stepResult, err := durable.Step[UserData](ctx, "fetch-user-data",
		func(sctx durable.StepContext) (UserData, error) {
			return UserData{UserID: 123, Name: "John Doe"}, nil
		})
	if err != nil {
		return Result{}, err
	}

	// WaitForCallback with self-completing submitter.
	callbackResult, err := durable.WaitForCallback[string](ctx, "wait-for-callback",
		func(sctx durable.StepContext, callbackID string) error {
			cfg, err := config.LoadDefaultConfig(sctx)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			client := lambdasvc.NewFromConfig(cfg)
			result, _ := json.Marshal("callback-data")
			_, err = client.SendDurableExecutionCallbackSuccess(
				sctx,
				&lambdasvc.SendDurableExecutionCallbackSuccessInput{
					CallbackId: &callbackID,
					Result:     result,
				})
			return err
		},
		durable.WithCallbackTimeout(30*time.Second),
	)
	if err != nil {
		return Result{}, err
	}

	// Final wait.
	if err := durable.Wait(ctx, "final-wait", 2*time.Second); err != nil {
		return Result{}, err
	}

	// Final step.
	finalStep, err := durable.Step[FinalStep](ctx, "finalize-processing",
		func(sctx durable.StepContext) (FinalStep, error) {
			return FinalStep{Status: "completed", Timestamp: 1234567890}, nil
		})
	if err != nil {
		return Result{}, err
	}

	return Result{
		StepResult:       stepResult,
		CallbackResult:   callbackResult,
		FinalStep:        finalStep,
		WorkflowComplete: true,
	}, nil
}

func main() { durable.Start(handler) }
