// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s (%v)", result.Status, result.Error)
	}
	// The wait suspends the first invocation; the second replays the
	// failed child and computes the result from the re-mapped error.
	if got := len(result.Invocations); got != 2 {
		t.Fatalf("expected 2 invocations, got %d", got)
	}

	out, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if want := (Result{Status: "declined", DeclineCode: "insufficient_funds"}); out != want {
		t.Errorf("result = %+v, want %+v", out, want)
	}

	// The checkpoint records the failure that escaped the child body, the
	// mapper's input, not the PaymentDeclinedError it returned.
	op := result.Operation("payment")
	if op == nil || op.ContextDetails == nil {
		t.Fatal("payment context operation not found")
	}
	if op.Status != "FAILED" || op.ContextDetails.ErrorType != "StepError" {
		t.Errorf("payment = %s with ErrorType %q, want FAILED with StepError", op.Status, op.ContextDetails.ErrorType)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
