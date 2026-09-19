// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if len(output.Outcomes) != 3 {
		t.Fatalf("expected 3 outcomes, got %d", len(output.Outcomes))
	}
	// Verify each outcome contains the expected prefix.
	if !strings.HasPrefix(output.Outcomes[0], "fulfilled:") {
		t.Errorf("outcome[0] should be fulfilled, got %q", output.Outcomes[0])
	}
	if !strings.HasPrefix(output.Outcomes[1], "rejected:") {
		t.Errorf("outcome[1] should be rejected, got %q", output.Outcomes[1])
	}
	if !strings.HasPrefix(output.Outcomes[2], "fulfilled:") {
		t.Errorf("outcome[2] should be fulfilled, got %q", output.Outcomes[2])
	}

	// Concurrent branches checkpoint in scheduling-dependent order, so the
	// signature is compared as a set.
	extest.AssertSignature(t, result, extest.Unordered)
}
