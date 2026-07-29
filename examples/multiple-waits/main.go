// Command multiple-waits demonstrates sequential [durable.Wait]
// operations, each creating a separate suspension and invocation cycle.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result is the output after both waits complete.
type Result struct {
	CompletedWaits int    `json:"completedWaits"`
	FinalStep      string `json:"finalStep"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// Two sequential waits — each suspends for 2 seconds (kept short
	// for faster smoke runs).
	if err := durable.Wait(ctx, "wait-1", 2*time.Second); err != nil {
		return Result{}, err
	}
	if err := durable.Wait(ctx, "wait-2", 2*time.Second); err != nil {
		return Result{}, err
	}
	return Result{CompletedWaits: 2, FinalStep: "done"}, nil
}

func main() { durable.Start(handler) }
