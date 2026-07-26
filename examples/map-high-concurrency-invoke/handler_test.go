// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)

	const itemCount = 15

	// Run until all invokes are resolved. Map items with concurrency 5
	// may not all register their invokes in a single invocation cycle,
	// so iterate: resolve any registered invokes, re-invoke, repeat.
	var result *durabletest.TestResult
	for attempt := 0; attempt < 20; attempt++ {
		result = runner.RunUntilComplete(t, nil)
		if result.Status == durabletest.Succeeded {
			break
		}
		if result.Status != durabletest.Pending {
			t.Fatalf("expected Pending or Succeeded, got %s", result.Status)
		}

		// Complete all invokes that have been registered so far.
		// CompleteChainedInvoke returns specific errors for expected
		// conditions during convergence:
		// - "not found": invoke not yet checkpointed (memory_client.go:454)
		// - "is in SUCCEEDED status, expected STARTED": already completed
		//   in a prior iteration (memory_client.go:462)
		// Any other error indicates a real problem (e.g., wrong type).
		for i := range itemCount {
			name := fmt.Sprintf("invoke-item-%d", i)
			payload := fmt.Sprintf("result-%d", i)
			err := runner.CompleteChainedInvoke(name, json.RawMessage(fmt.Sprintf("%q", payload)))
			if err != nil &&
				!strings.Contains(err.Error(), "not found") &&
				!strings.Contains(err.Error(), "expected STARTED") {
				t.Fatalf("CompleteChainedInvoke(%q): unexpected error: %v", name, err)
			}
		}
	}

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded after resolving all invokes, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	// Verify all invoke results are collected in input order.
	if len(output.Results) != itemCount {
		t.Fatalf("expected %d results, got %d", itemCount, len(output.Results))
	}
	for i, got := range output.Results {
		want := fmt.Sprintf("result-%d", i)
		if got != want {
			t.Errorf("result[%d] = %q, want %q", i, got, want)
		}
	}

	// Verify the map context and chained invoke operations are present in
	// the operation signature. Use unordered assertion since map item
	// completion order is nondeterministic.
	durabletest.AssertSignatureContains(t, result, []durabletest.OperationSignature{
		{Type: "CONTEXT", SubType: "Map", Name: "process-items", Status: "SUCCEEDED"},
	})
}
