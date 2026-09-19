// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output != "result" {
		t.Errorf("expected %q, got %q", "result", output)
	}

	// The "complete" step must always be present.
	// The "background-wait" (fired via WaitAsync) may or may not appear
	// depending on goroutine timing — it is fire-and-forget.
	durabletest.AssertSignatureContains(t, result, []durabletest.OperationSignature{
		{Type: "STEP", SubType: "Step", Name: "complete", Status: "SUCCEEDED"},
	})

	// Verify that if background-wait appears, it has the expected shape.
	sig := durabletest.EventSignature(result)
	for _, op := range sig {
		if op.Name == "background-wait" {
			if op.Type != "WAIT" {
				t.Errorf("expected background-wait type WAIT, got %s", op.Type)
			}
			if op.Status != "STARTED" && op.Status != "SUCCEEDED" {
				t.Errorf("expected background-wait status STARTED or SUCCEEDED, got %s", op.Status)
			}
		}
	}
}
