// Command logger-after-callback demonstrates replay-aware logging across a
// callback suspend/resume boundary. The log line before WaitForCallback
// appears only during the first invocation; on the resume invocation
// (triggered by the callback response), replayed operations suppress log
// output so "before-callback" does NOT appear again.
//
// The callback's outcome is new to the resume invocation, so the code after
// WaitForCallback is live there and its lines are written.
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

type result struct {
	Message    string `json:"message"`
	CallbackID string `json:"callbackId"`
	Result     string `json:"result"`
}

func handler(ctx durable.Context, _ any) (result, error) {
	ctx.Logger().Info("before-callback", "phase", "live")

	value, err := durable.WaitForCallback[string](ctx, "my-callback",
		func(sctx durable.StepContext, callbackID string) error {
			sctx.Logger().Info("Submitter sending callback success",
				"callbackId", callbackID)
			return completeCallback(sctx, callbackID, "callback-resolved")
		},
		durable.WithCallbackTimeout(30*time.Second),
		durable.WithCallbackSerdes(durable.JSONSerdes),
	)
	if err != nil {
		return result{}, err
	}

	// The step runs live on the resume invocation.
	_, err = durable.Step(ctx, "post-callback-step", func(sc durable.StepContext) (string, error) {
		sc.Logger().Info("inside-post-callback-step", "phase", "live")
		return "ok", nil
	})
	if err != nil {
		return result{}, err
	}

	ctx.Logger().Info("after-callback", "phase", "resumed", "value", value)

	return result{
		Message:    "done",
		CallbackID: "self-resolved",
		Result:     value,
	}, nil
}

func completeCallback(ctx context.Context, callbackID, value string) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	client := lambdasvc.NewFromConfig(cfg)
	resultPayload, _ := json.Marshal(value)
	_, err = client.SendDurableExecutionCallbackSuccess(ctx,
		&lambdasvc.SendDurableExecutionCallbackSuccessInput{
			CallbackId: &callbackID,
			Result:     resultPayload,
		})
	return err
}

func main() { durable.Start(handler) }
