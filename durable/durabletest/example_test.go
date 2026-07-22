// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// This example demonstrates the canonical usage of the local testing
// runner: create a runner, run a handler to completion, and inspect the
// typed result.
func Example() {
	// Define a durable handler that processes an order through multiple steps.
	handler := func(ctx durable.Context, event string) (string, error) {
		validated, err := durable.Step[string](ctx, "validate", func(_ durable.StepContext) (string, error) {
			return "valid:" + event, nil
		})
		if err != nil {
			return "", err
		}

		if err := durable.Wait(ctx, "cooldown", 5*time.Second); err != nil {
			return "", err
		}

		confirmed, err := durable.Step[string](ctx, "confirm", func(_ durable.StepContext) (string, error) {
			return "confirmed:" + validated, nil
		})
		if err != nil {
			return "", err
		}

		return confirmed, nil
	}

	// Create a local runner and run the handler to completion.
	// RunUntilComplete automatically advances the wait timer between
	// invocations.
	t := &testing.T{} // in real tests, use the *testing.T passed to your test function
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "order-42")

	fmt.Println("Status:", result.Status)

	output, err := durabletest.ResultAs[string](result)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Result:", output)

	// Inspect individual operations.
	validateOp := result.Operation("validate")
	fmt.Println("Validate status:", validateOp.Status)

	// Output:
	// Status: SUCCEEDED
	// Result: confirmed:valid:order-42
	// Validate status: SUCCEEDED
}
