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
	if output.SuccessCount != 0 {
		t.Errorf("expected SuccessCount=0, got %d", output.SuccessCount)
	}
	if output.FailureCount != 0 {
		t.Errorf("expected FailureCount=0, got %d", output.FailureCount)
	}
	if output.TotalCount != 0 {
		t.Errorf("expected TotalCount=0, got %d", output.TotalCount)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
