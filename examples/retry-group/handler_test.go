// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// backoffDelays returns the duration, in seconds, of each backoff wait
// Retry scheduled between attempts. A backoff is a Wait, so it records a
// WaitStarted event with its duration.
func backoffDelays(result *durabletest.TestResult) []int32 {
	var delays []int32
	for _, ev := range result.Events {
		if ev.EventType != types.EventTypeWaitStarted || ev.WaitStartedDetails == nil {
			continue
		}
		delays = append(delays, aws.ToInt32(ev.WaitStartedDetails.Duration))
	}
	return delays
}

// TestHandler runs the default scenario: the supplier answers busy,
// rate-limited, and 503 before quoting. Each answer is retryable only
// through its own matcher (ErrorTypeIs, ErrorContains, ErrorMatches), so
// reaching the fourth attempt shows all three matched the error rebuilt
// from the attempt's child context.
func TestHandler(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, Input{})

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s (error: %v)", result.Status, result.Error)
	}
	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	want := Output{Status: "confirmed", PriceCents: 4250, Attempts: 4}
	if out != want {
		t.Errorf("output = %+v, want %+v", out, want)
	}

	// Exponential backoff from 1 s at rate 2, without jitter.
	if got := backoffDelays(result); !slices.Equal(got, []int32{1, 2, 4}) {
		t.Errorf("backoff delays = %v, want [1 2 4]", got)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// TestHandlerRejected stops on the second attempt: no matcher selects an
// OrderRejectedError. The error mapper rebuilds it as the handler's own
// type, which the handler turns into a result.
func TestHandlerRejected(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, Input{Responses: []string{"busy", "rejected"}})

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s (error: %v)", result.Status, result.Error)
	}
	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	want := Output{Status: "rejected", Reason: "item discontinued", Attempts: 2}
	if out != want {
		t.Errorf("output = %+v, want %+v", out, want)
	}
	if got := backoffDelays(result); !slices.Equal(got, []int32{1}) {
		t.Errorf("backoff delays = %v, want [1]", got)
	}

	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.rejected.golden")
}

// TestHandlerExhausted keeps the supplier busy until MaxAttempts is
// reached. Retry then returns a RetryError naming the last attempt's
// error, and the execution fails with it.
func TestHandlerExhausted(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, Input{Responses: []string{"busy", "busy", "busy"}, MaxAttempts: 3})

	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected non-nil error on Failed result")
	}
	if result.Error.Type != "RetryError" {
		t.Errorf("error type = %q, want RetryError", result.Error.Type)
	}
	if msg := result.Error.Message; !strings.Contains(msg, "failed after 3 attempts") || !strings.Contains(msg, "supplier acme is busy") {
		t.Errorf("error message = %q, want 3 attempts ending on the busy supplier", msg)
	}
	if got := backoffDelays(result); !slices.Equal(got, []int32{1, 2}) {
		t.Errorf("backoff delays = %v, want [1 2]", got)
	}

	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.exhausted.golden")
}

// TestHandlerInvalidMaxAttempts shows NewRetryStrategy rejecting a value
// from the event. The handler returns the validation error before Retry
// starts, so nothing is checkpointed.
func TestHandlerInvalidMaxAttempts(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, Input{MaxAttempts: -1})

	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil || !strings.Contains(result.Error.Message, "MaxAttempts must not be negative") {
		t.Fatalf("expected MaxAttempts validation error, got %+v", result.Error)
	}
	if len(result.Operations) != 0 {
		t.Errorf("expected no operations, got %d", len(result.Operations))
	}

	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.invalid.golden")
}
