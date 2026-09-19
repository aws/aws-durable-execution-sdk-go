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

	// The wait strategy requires state == attempt on every cycle, so the
	// handler succeeds only if the counter checkpointed on each RETRY is
	// read back by the next attempt. It stops once the counter reaches 3.
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s (error: %v)", result.Status, result.Error)
	}

	output, err := durabletest.ResultAs[int](result)
	if err != nil {
		t.Fatalf("ResultAs error: %v", err)
	}
	if output != 3 {
		t.Errorf("result = %d, want 3", output)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
