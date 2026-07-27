// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
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
	// Two of the four branches fail immediately, so the failure percentage
	// reaches (2*100)/4 = 50, which strictly exceeds the 25 tolerance and
	// completes the batch with FAILURE_TOLERANCE_EXCEEDED. Both failures
	// are always counted (they are what trip the threshold); the two
	// succeeding branches may be counted or, if still in flight when the
	// threshold is exceeded, abandoned. So only the guarantees that always
	// hold are asserted.
	if output.CompletionReason != "FAILURE_TOLERANCE_EXCEEDED" {
		t.Errorf("expected CompletionReason=FAILURE_TOLERANCE_EXCEEDED, got %s", output.CompletionReason)
	}
	if output.FailureCount != 2 {
		t.Errorf("expected FailureCount=2, got %d", output.FailureCount)
	}
	if output.TotalCount != 4 {
		t.Errorf("expected TotalCount=4, got %d", output.TotalCount)
	}
	if !output.HasFailure {
		t.Error("expected HasFailure=true")
	}
	if output.SuccessCount < 0 || output.SuccessCount > 2 {
		t.Errorf("expected 0 <= SuccessCount <= 2, got %d", output.SuccessCount)
	}
	if output.SuccessCount+output.FailureCount > output.TotalCount {
		t.Errorf("counted items %d exceed TotalCount %d", output.SuccessCount+output.FailureCount, output.TotalCount)
	}

	// Any preserved results must come from the two succeeding branches.
	if len(output.SuccessResults) != output.SuccessCount {
		t.Errorf("SuccessResults length %d != SuccessCount %d", len(output.SuccessResults), output.SuccessCount)
	}
	allowed := map[string]bool{"result-1": true, "result-3": true}
	for _, r := range output.SuccessResults {
		if !allowed[r] {
			t.Errorf("unexpected success result %q", r)
		}
	}

	// The two failing branches always run to completion (they trip the
	// threshold), so their step failures are always checkpointed. The
	// succeeding branches may be abandoned before their step is recorded,
	// so they are not asserted here.
	durabletest.AssertSignatureContains(t, result, []durabletest.OperationSignature{
		{Type: "STEP", SubType: "Step", Name: "branch-2", Status: "FAILED"},
		{Type: "STEP", SubType: "Step", Name: "branch-4", Status: "FAILED"},
	})
}
