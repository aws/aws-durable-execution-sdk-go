// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)

	// First run: Step and Wait complete locally; Invoke suspends.
	result, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invoke), got %s", result.Status)
	}

	// Complete the chained invoke with a string result.
	if err := runner.CompleteChainedInvoke("child-function", json.RawMessage(`"invoked: child returned"`)); err != nil {
		t.Fatalf("CompleteChainedInvoke: %v", err)
	}

	// Second run: all branches resolved.
	result, err = runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	want := Output{
		Compute:          Computation{Expression: "7*6", Value: 42},
		WaitedSeconds:    1,
		Invoked:          "invoked: child returned",
		CompletionReason: "ALL_COMPLETED",
	}
	if output != want {
		t.Errorf("output = %+v, want %+v", output, want)
	}

	// The branches run concurrently and checkpoint in scheduling-dependent
	// order, so the signature is compared as a set.
	extest.AssertSignature(t, result, extest.Unordered)
}
