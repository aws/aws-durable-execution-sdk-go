// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	t.Setenv("FUNCTION_NAME_PREFIX", "v2-")

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, Input{})

	// Execution suspends because invokes need external resolution.
	if result.Status != durabletest.Pending {
		t.Fatalf("expected Pending (awaiting invokes), got %s", result.Status)
	}

	// Complete all chained invokes — they run in parallel branches so may
	// need to be resolved iteratively as each branch suspends.
	for attempt := 0; attempt < 5; attempt++ {
		for i := range 3 {
			name := fmt.Sprintf("invoke-%d", i)
			// Ignore errors — some invokes may not yet be registered.
			_ = runner.CompleteChainedInvoke(name, json.RawMessage(`{"ok":true}`))
		}
		result = runner.RunUntilComplete(t, Input{})
		if result.Status == durabletest.Succeeded {
			break
		}
		if result.Status != durabletest.Pending {
			t.Fatalf("expected Pending or Succeeded, got %s", result.Status)
		}
	}

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded after resolving all invokes, got %s", result.Status)
	}

	output, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if output.SuccessCount != 3 {
		t.Errorf("expected SuccessCount=3, got %d", output.SuccessCount)
	}

	// NOTE: Golden signature assertion is skipped for this example because
	// Parallel invoke operations produce non-deterministic ordering.
}
