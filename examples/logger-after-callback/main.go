// Command logger-after-callback demonstrates replay-aware logging across a
// callback suspend/resume boundary. The log line before createCallback
// appears only during the first invocation; on the resume invocation
// (triggered by the callback response), replayed operations suppress log
// output so "before-callback" does NOT appear again.
//
// The replay→live transition happens when the next durable operation is
// claimed and has no checkpoint. A step after the callback demonstrates that
// live-mode logging resumes correctly after replay.
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type result struct {
	Message    string `json:"message"`
	CallbackID string `json:"callbackId"`
	Result     string `json:"result"`
}

func handler(ctx durable.Context, _ any) (result, error) {
	ctx.Logger().Info("before-callback", "phase", "live")

	cb, err := durable.CreateCallback[string](ctx, "my-callback")
	if err != nil {
		return result{}, err
	}

	value, err := cb.Result()
	if err != nil {
		return result{}, err
	}

	// This step triggers the replay→live transition on the resume
	// invocation.
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
		CallbackID: cb.ID(),
		Result:     value,
	}, nil
}

func main() { durable.Start(handler) }
