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

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}

	// With MinSuccessful=2 and MaxConcurrency=3 over 5 items (2 of which
	// always fail after MaxAttempts=2), the batch completes early with
	// reason MIN_SUCCESSFUL_REACHED as soon as 2 items succeed. The items
	// still in flight at that point are abandoned (reported STARTED, not
	// counted), and items that never started are omitted. Which items land
	// as failed versus abandoned is not deterministic, so only the
	// guarantees that always hold are asserted:
	//   - the min-successful guarantee (>= 2 successes),
	//   - the completion reason,
	//   - the accounting identity Total == Success + Failed + Started,
	//   - Total never exceeds the 5 input items.
	if output.CompletionNote != "MIN_SUCCESSFUL_REACHED" {
		t.Errorf("expected CompletionNote == MIN_SUCCESSFUL_REACHED, got %s", output.CompletionNote)
	}
	if output.SuccessfulCount < 2 {
		t.Errorf("expected SuccessfulCount >= 2 (MinSuccessful guarantee), got %d", output.SuccessfulCount)
	}
	if output.TotalItems != output.SuccessfulCount+output.FailedCount+output.StartedCount {
		t.Errorf("expected TotalItems == SuccessfulCount + FailedCount + StartedCount, got %d != %d + %d + %d",
			output.TotalItems, output.SuccessfulCount, output.FailedCount, output.StartedCount)
	}
	if output.TotalItems > 5 || output.TotalItems < output.SuccessfulCount {
		t.Errorf("expected SuccessfulCount <= TotalItems <= 5, got Total=%d Success=%d", output.TotalItems, output.SuccessfulCount)
	}
	// BatchStatus reflects whether any counted item failed; under early
	// completion a failing item may be abandoned before it fails, so
	// HasFailures is consistent with the counted failures either way.
	if output.HasFailures != (output.FailedCount > 0) {
		t.Errorf("HasFailures=%v inconsistent with FailedCount=%d", output.HasFailures, output.FailedCount)
	}

	// The Map completes once MinSuccessful is reached, so an in-flight item
	// may or may not have checkpointed its step. The golden lists the
	// operations every run produces.
	extest.AssertSignature(t, result, extest.Subset)
}
