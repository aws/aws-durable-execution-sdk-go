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

	output, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	// The uppercaseSerdes uppercases on marshal, so the stored/returned
	// value should be uppercase when read back through the virtual child.
	if output != "HELLO FROM VIRTUAL" {
		t.Errorf("expected 'HELLO FROM VIRTUAL' (uppercased by custom serdes), got %q", output)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
