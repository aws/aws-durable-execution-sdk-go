// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// retryDelays returns the delay, in seconds, scheduled before each retry.
// Every failed attempt that the strategy retries records a StepFailed event
// with the next attempt's delay; the final failure of an exhausted step
// records none.
//
// The delays are the only evidence of the strategy: the operation signature
// holds neither attempt counts nor durations, so an exponential strategy
// that retried the same number of times would produce the same golden.
func retryDelays(result *durabletest.TestResult) []int32 {
	var delays []int32
	for _, ev := range result.Events {
		if ev.EventType != types.EventTypeStepFailed || ev.StepFailedDetails == nil {
			continue
		}
		retry := ev.StepFailedDetails.RetryDetails
		if retry == nil || retry.NextAttemptDelaySeconds == nil {
			continue
		}
		delays = append(delays, aws.ToInt32(retry.NextAttemptDelaySeconds))
	}
	return delays
}

// TestHandler runs the default scenario on defaultStrategy, the strategy
// built with MustLinearBackoff at package initialization.
func TestHandler(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, Input{})

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	// Attempts 1, 2 and 3 fail; attempt 4 succeeds.
	if out.Attempts != 4 || out.Message != "request confirmed on attempt 4" {
		t.Errorf("unexpected output %+v", out)
	}

	// Three delays are needed to identify the strategy: exponential backoff
	// from the same 1 s initial delay also produces 1 s then 2 s. Only the
	// third delay separates them, linear 3 s against exponential 4 s.
	if got := retryDelays(result); !slices.Equal(got, []int32{1, 2, 3}) {
		t.Errorf("retry delays = %v, want [1 2 3]", got)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// TestHandlerExhausted overrides MaxDelay, so the strategy is built at run
// time with LinearBackoff.
func TestHandlerExhausted(t *testing.T) {
	runner := extest.New(t, handler)
	// Above maxAttempts, so every attempt fails and the strategy stops
	// retrying. MaxDelay is lowered to 3 s so the fourth delay is capped:
	// the linear formula alone would give 4 s.
	result := runner.RunUntilComplete(t, Input{SucceedOnAttempt: 9, MaxDelaySeconds: 3})

	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected non-nil error on Failed result")
	}
	// The error of the last attempt propagates, not the first.
	if !strings.Contains(result.Error.Message, "upstream temporarily unavailable (attempt 5)") {
		t.Errorf("expected error from attempt 5, got %q", result.Error.Message)
	}

	// Four delays for five attempts, the last one clamped by MaxDelay.
	if got := retryDelays(result); !slices.Equal(got, []int32{1, 2, 3, 3}) {
		t.Errorf("retry delays = %v, want [1 2 3 3]", got)
	}

	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.exhausted.golden")
}

// TestHandlerInvalidMaxDelay shows the difference between the two
// constructors on the same invalid value. MaxDelay must be at least one
// second when set. LinearBackoff returns the validation error, so the
// handler returns it before the step starts and the execution fails with
// nothing checkpointed. MustLinearBackoff panics on the same config.
func TestHandlerInvalidMaxDelay(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, Input{MaxDelaySeconds: -1})

	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected non-nil error on Failed result")
	}
	if !strings.Contains(result.Error.Message, "MaxDelay must be at least 1 second") {
		t.Errorf("expected MaxDelay validation error, got %q", result.Error.Message)
	}
	if len(result.Operations) != 0 {
		t.Errorf("expected no operations, got %d", len(result.Operations))
	}
	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.invalid.golden")

	// The same configuration through MustLinearBackoff is a startup panic,
	// which is why the hard-coded defaultStrategy uses it and the
	// input-driven path does not.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("MustLinearBackoff did not panic on an invalid MaxDelay")
		}
		err, ok := r.(error)
		if !ok || !strings.Contains(err.Error(), "MaxDelay must be at least 1 second") {
			t.Errorf("unexpected panic value %v", r)
		}
	}()
	durable.MustLinearBackoff(durable.LinearRetryConfig{MaxDelay: -1 * time.Second})
}
