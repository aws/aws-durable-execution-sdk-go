// Command child-ops-invalid-depth demonstrates that unhandled errors in
// a child context propagate as the execution's final error, causing it
// to reach FAILED terminal state. Since the SDK handles ReplayChildren
// automatically, this demonstrates that a programming error (panic in
// child) correctly fails the execution rather than being silently
// swallowed.
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	// An unhandled error in RunInChildContext propagates and fails the
	// execution (FAILED terminal state).
	_, err := durable.RunInChildContext(ctx, "invalid-child",
		func(child durable.Context) (string, error) {
			return durable.Step(child, "boom",
				func(_ durable.StepContext) (string, error) {
					panic("simulated programming error: invalid depth config")
				},
				durable.WithRetry(durable.NoRetry()))
		})
	if err != nil {
		return "", err
	}
	return "should not reach here", nil
}

func main() { durable.Start(handler) }
