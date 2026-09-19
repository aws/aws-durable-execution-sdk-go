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
	result := runner.RunUntilComplete(t, nil)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output.Winner != "fallback" {
		t.Errorf("expected Winner=%q, got %q", "fallback", output.Winner)
	}
	if output.Quote != 104.50*1.02 {
		t.Errorf("expected Quote=%v, got %v", 104.50*1.02, output.Quote)
	}

	// Select returns when the fallback settles, so the slow primary step
	// may or may not have reached a checkpoint. The golden lists the
	// operations every run produces.
	extest.AssertSignature(t, result, extest.Subset)
}
