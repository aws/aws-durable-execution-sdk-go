// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

func TestHandler(t *testing.T) {
	// Test success case: at least one future succeeds.
	t.Run("success", func(t *testing.T) {
		runner := durabletest.NewLocalRunner(handler)
		result := runner.RunUntilComplete(t, Input{ShouldFail: false})

		if result.Status != durabletest.Succeeded {
			t.Fatalf("expected Succeeded, got %s", result.Status)
		}

		output, err := durabletest.ResultAs[Result](result)
		if err != nil {
			t.Fatalf("deserialize result: %v", err)
		}
		if output.Status != "succeeded" {
			t.Errorf("expected status %q, got %q", "succeeded", output.Status)
		}
		if output.Value == "" {
			t.Error("expected non-empty value on success")
		}

		// The golden records the default (success) path, which is the one
		// the deployed example runs. Any returns on the first success, so
		// which of the other futures reach a checkpoint, and in what state,
		// depends on scheduling. The golden lists the operations every run
		// produces.
		extest.AssertSignature(t, result, extest.Subset)
	})

	// Test all-fail case: CombinatorError wraps all individual errors.
	t.Run("all-fail", func(t *testing.T) {
		runner := durabletest.NewLocalRunner(handler)
		result := runner.RunUntilComplete(t, Input{ShouldFail: true})

		if result.Status != durabletest.Succeeded {
			t.Fatalf("expected Succeeded (handler catches error), got %s", result.Status)
		}

		output, err := durabletest.ResultAs[Result](result)
		if err != nil {
			t.Fatalf("deserialize result: %v", err)
		}
		if output.Status != "all-failed" {
			t.Errorf("expected status %q, got %q", "all-failed", output.Status)
		}
		if output.Error == "" {
			t.Error("expected non-empty error message on all-fail")
		}

		// Every future fails before Any returns, so all of them reach a
		// checkpoint, in scheduling-dependent order.
		extest.AssertSignatureFile(t, result, extest.Unordered, "testdata/signature.all-fail.golden")
	})
}
