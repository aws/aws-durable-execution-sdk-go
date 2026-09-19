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
	result := runner.RunUntilComplete(t, Input{})

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Result](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	if runner.Cloud() {
		// The deployed function has a Lambda timeout shorter than the
		// step, so the step is interrupted and the handler reports the
		// StepInterruptedError. The local runner has no function timeout,
		// so locally the step runs to completion.
		if output.Status != "failed" {
			t.Errorf("expected status %q, got %q", "failed", output.Status)
		}
		if output.CauseName != "StepInterruptedError" {
			t.Errorf("expected causeName %q, got %q", "StepInterruptedError", output.CauseName)
		}
		// The interrupted step is recorded as FAILED, so the deployed run
		// has its own golden.
		extest.AssertSignatureFile(t, result, extest.Ordered, extest.CloudGoldenPath)
		return
	}

	if output.Status != "succeeded" {
		t.Errorf("expected status %q, got %q", "succeeded", output.Status)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}
