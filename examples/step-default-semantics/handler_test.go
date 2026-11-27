// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := extest.New(t, handler, options...)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	output, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output != "order-3 confirmed" {
		t.Errorf("expected %q, got %q", "order-3 confirmed", output)
	}

	// The wait suspends the first invocation, so the execution needs two.
	if got := len(result.Invocations); got < 2 {
		t.Errorf("expected at least 2 invocations, got %d", got)
	}

	want := map[string]string{"count-items": "3", "create-order-id": `"order-3"`, "confirm-order": `"order-3 confirmed"`}
	for name, res := range want {
		op := result.Operation(name)
		if op == nil {
			t.Fatalf("operation %q not found", name)
		}
		if op.Type != "STEP" || op.Status != "SUCCEEDED" {
			t.Errorf("%s: got %s %s, want STEP SUCCEEDED", name, op.Type, op.Status)
		}
		if op.StepDetails == nil || op.StepDetails.Result != res {
			t.Errorf("%s: result = %+v, want %s", name, op.StepDetails, res)
		}
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
