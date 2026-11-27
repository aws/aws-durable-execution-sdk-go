// Command step-default-semantics sets [durable.AtLeastOnceAsync] as the
// default step semantics with [durable.WithDefaultStepSemantics], and keeps
// [durable.AtLeastOnce] for a step whose result is an idempotency key.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// options are the handler options of the example. The test runs the
// handler with the same options.
var options = []durable.HandlerOption{
	durable.WithDefaultStepSemantics(durable.AtLeastOnceAsync),
}

func handler(ctx durable.Context, _ any) (string, error) {
	// Uses the execution default, AtLeastOnceAsync. The step returns
	// without waiting for its result to be saved.
	quantity, err := durable.Step(ctx, "count-items", func(durable.StepContext) (int, error) {
		return 3, nil
	})
	if err != nil {
		return "", err
	}

	// The order ID is an idempotency key for later steps. So it must be
	// saved before they run, and the step sets AtLeastOnce itself.
	orderID, err := durable.Step(ctx, "create-order-id", func(durable.StepContext) (string, error) {
		return fmt.Sprintf("order-%d", quantity), nil
	}, durable.WithSemantics(durable.AtLeastOnce))
	if err != nil {
		return "", err
	}

	// The wait ends the invocation. The next invocation replays the saved
	// results.
	if err := durable.Wait(ctx, "wait-for-warehouse", time.Second); err != nil {
		return "", err
	}

	return durable.Step(ctx, "confirm-order", func(durable.StepContext) (string, error) {
		return orderID + " confirmed", nil
	})
}

func main() { durable.Start(handler, options...) }
