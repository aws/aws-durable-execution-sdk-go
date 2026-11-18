// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// TestHandler runs the step until the strategy pauses the execution after
// attempt 2, resumes it, and checks that attempt 4 succeeds.
//
// The test is local only. The example function role has no
// lambda:PauseDurableExecution permission, and the cloud runner cannot
// resume an execution.
func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)

	paused, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if paused.Status != durabletest.Pending {
		t.Fatalf("after pause: status = %s, want PENDING", paused.Status)
	}
	isPaused, reason := runner.Paused()
	if !isPaused {
		t.Fatal("after pause: Paused() = false, want true")
	}
	if want := `Step "flaky-operation" failed attempt 2`; reason != want {
		t.Errorf("pause reason = %q, want %q", reason, want)
	}

	if err := runner.ResumeExecution(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	done, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != durabletest.Succeeded {
		t.Fatalf("after resume: status = %s, want SUCCEEDED", done.Status)
	}
	got, err := durabletest.ResultAs[string](done)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	if want := "succeeded on attempt 4"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}

	extest.AssertSignature(t, done, extest.Ordered)
}
