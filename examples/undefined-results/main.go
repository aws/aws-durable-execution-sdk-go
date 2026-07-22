// Command undefined-results demonstrates that operations returning nil
// (the Go equivalent of JavaScript's undefined) replay correctly. Each
// operation type — Step, RunInChildContext, and Wait — produces a nil or
// zero-value result that the checkpoint system stores and restores without
// error.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	// Step returning nil result.
	_, err := durable.Step(ctx, "fetch-user", func(_ durable.StepContext) (*string, error) {
		return nil, nil
	})
	if err != nil {
		return "", err
	}

	// RunInChildContext returning nil result.
	_, err = durable.RunInChildContext(ctx, "parent", func(_ durable.Context) (*string, error) {
		return nil, nil
	})
	if err != nil {
		return "", err
	}

	// Wait produces no meaningful result (Void).
	_ = durable.Wait(ctx, "pause", 1*time.Second)

	return "result", nil
}

func main() { durable.Start(handler) }
