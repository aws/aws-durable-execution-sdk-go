// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// pollDelays returns the delay, in seconds, scheduled before each next
// check. A check after which the strategy keeps polling records its state
// as a step result together with the delay before the next check; the
// final check records none.
//
// The delays are the only evidence of the backoff: the operation signature
// holds neither check counts nor durations.
func pollDelays(result *durabletest.TestResult) []int32 {
	var delays []int32
	for _, ev := range result.Events {
		if ev.EventType != types.EventTypeStepSucceeded || ev.StepSucceededDetails == nil {
			continue
		}
		retry := ev.StepSucceededDetails.RetryDetails
		if retry == nil || retry.NextAttemptDelaySeconds == nil {
			continue
		}
		delays = append(delays, aws.ToInt32(retry.NextAttemptDelaySeconds))
	}
	return delays
}

// TestHandler runs the default scenario on defaultStrategy, the strategy
// built with MustNewWaitStrategy at package initialization.
func TestHandler(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, Input{})

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s (error: %v)", result.Status, result.Error)
	}
	out, err := durabletest.ResultAs[Job](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	// ShouldContinue stops polling on the fourth check, which found the
	// job ready.
	if want := (Job{Checks: 4, Ready: true}); out != want {
		t.Errorf("result = %+v, want %+v", out, want)
	}

	// Three delays separate exponential backoff from a linear one: both
	// can start 1 s, 2 s, but only exponential continues with 4 s.
	if got := pollDelays(result); !slices.Equal(got, []int32{1, 2, 4}) {
		t.Errorf("poll delays = %v, want [1 2 4]", got)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// TestHandlerExhausted overrides MaxAttempts, so the strategy is built at
// run time with NewWaitStrategy. The job is not ready by the last allowed
// check, so the operation fails.
func TestHandlerExhausted(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, Input{MaxAttempts: 2})

	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected non-nil error on Failed result")
	}
	if result.Error.Type != "WaitForConditionError" {
		t.Errorf("error type = %q, want WaitForConditionError", result.Error.Type)
	}
	if !strings.Contains(result.Error.Message, "exceeded maximum attempts (2)") {
		t.Errorf("expected the cap of 2 checks in the error, got %q", result.Error.Message)
	}
	// One delay between the two checks.
	if got := pollDelays(result); !slices.Equal(got, []int32{1}) {
		t.Errorf("poll delays = %v, want [1]", got)
	}

	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.exhausted.golden")
}

// TestHandlerInvalidMaxAttempts shows the difference between the two
// constructors on the same invalid value. NewWaitStrategy returns the
// validation error, so the handler returns it before the first check and
// the execution fails with nothing checkpointed. MustNewWaitStrategy
// panics on the same config.
func TestHandlerInvalidMaxAttempts(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, Input{MaxAttempts: -1})

	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected non-nil error on Failed result")
	}
	if !strings.Contains(result.Error.Message, "MaxAttempts must not be negative") {
		t.Errorf("expected MaxAttempts validation error, got %q", result.Error.Message)
	}
	if len(result.Operations) != 0 {
		t.Errorf("expected no operations, got %d", len(result.Operations))
	}
	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.invalid.golden")

	// The same configuration through MustNewWaitStrategy is a startup
	// panic, which is why the hard-coded defaultStrategy uses it and the
	// input-driven path does not.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("MustNewWaitStrategy did not panic on a negative MaxAttempts")
		}
		err, ok := r.(error)
		if !ok || !strings.Contains(err.Error(), "MaxAttempts must not be negative") {
			t.Errorf("unexpected panic value %v", r)
		}
	}()
	durable.MustNewWaitStrategy(pollConfig(-1))
}
