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

	// WaitAsync records the start of background-wait before it returns,
	// so the wait is always recorded. The handler returns before the
	// wait elapses, so the wait stays STARTED.
	extest.AssertSignature(t, result, extest.Ordered)
}
