// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	// Reset the package-level recorder so repeated test runs (-count=5)
	// start with a clean slate.
	rec = &recorder{}

	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(plugin))
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	if output.Message != "plugin lifecycle complete" {
		t.Errorf("expected message %q, got %q", "plugin lifecycle complete", output.Message)
	}

	// The full lifecycle sequence including OnInvocationEnd, which fires
	// after the handler returns.
	fullSequence := rec.snapshot()

	expectedHooks := []hookEvent{
		{Hook: "OnInvocationStart"},
		{Hook: "OnOperationStart", OperationName: "compute"},
		{Hook: "OnOperationAttemptStart", OperationName: "compute", Attempt: 1},
		{Hook: "OnOperationAttemptEnd", OperationName: "compute", Attempt: 1},
		{Hook: "OnOperationEnd", OperationName: "compute"},
		{Hook: "OnInvocationEnd"},
	}

	if len(fullSequence) != len(expectedHooks) {
		t.Fatalf("expected %d hook events, got %d: %+v", len(expectedHooks), len(fullSequence), fullSequence)
	}

	for i, want := range expectedHooks {
		got := fullSequence[i]
		if got.Hook != want.Hook {
			t.Errorf("event[%d]: expected hook %q, got %q", i, want.Hook, got.Hook)
		}
		if want.OperationName != "" && got.OperationName != want.OperationName {
			t.Errorf("event[%d]: expected operationName %q, got %q", i, want.OperationName, got.OperationName)
		}
		if want.Attempt != 0 && got.Attempt != want.Attempt {
			t.Errorf("event[%d]: expected attempt %d, got %d", i, want.Attempt, got.Attempt)
		}
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
