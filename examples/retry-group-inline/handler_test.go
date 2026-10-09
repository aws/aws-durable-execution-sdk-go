// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// backoffDelays returns the duration, in seconds, of each backoff wait
// Retry scheduled between attempts.
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

// TestHandler runs the default scenario: busy, then rate-limited, then a
// quote. The first failure is retryable only through ErrorAs and the
// second only through ErrorIs, and both match only because the attempts
// pass the strategy their live error. Reaching the third attempt shows
// both matched.
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
	if want := (Output{PriceCents: 4250, Attempts: 3}); out != want {
		t.Errorf("output = %+v, want %+v", out, want)
	}

	// JitterHalf draws each delay between half the computed delay and the
	// full delay, rounded to whole seconds: 1-2 s, then 2-4 s. The values
	// vary between runs, so only the bounds are asserted.
	delays := backoffDelays(result)
	if len(delays) != 2 {
		t.Fatalf("backoff delays = %v, want 2 delays", delays)
	}
	for i, bounds := range [][2]int32{{1, 2}, {2, 4}} {
		if delays[i] < bounds[0] || delays[i] > bounds[1] {
			t.Errorf("backoff delay %d = %d s, want %d-%d s", i+1, delays[i], bounds[0], bounds[1])
		}
	}

	// Without per-attempt child contexts, each attempt's step is a
	// top-level operation next to the backoff waits.
	extest.AssertSignature(t, result, extest.Ordered)
}

// TestHandlerNotRetryable stops on the first attempt: an error that
// neither matcher selects is not retried. Retry returns a RetryError
// carrying the attempt's own error, and the execution fails with it.
func TestHandlerNotRetryable(t *testing.T) {
	runner := extest.New(t, handler)
	result := runner.RunUntilComplete(t, Input{Responses: []string{"rejected"}})

	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected non-nil error on Failed result")
	}
	if result.Error.Type != "RetryError" {
		t.Errorf("error type = %q, want RetryError", result.Error.Type)
	}
	if msg := result.Error.Message; !strings.Contains(msg, "failed after 1 attempts") || !strings.Contains(msg, "item discontinued") {
		t.Errorf("error message = %q, want one attempt ending on the rejection", msg)
	}
	if delays := backoffDelays(result); len(delays) != 0 {
		t.Errorf("backoff delays = %v, want none", delays)
	}

	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.not-retryable.golden")
}

// TestStrategyJitterHalf samples the strategy's second delay, whose
// computed value is 4 s. JitterHalf draws it between 2 s and 4 s, so a
// sample never falls below 2 s, as full jitter can, and the samples
// differ, as they would not without jitter. A single execution records one
// draw, too few to tell the strategies apart.
func TestStrategyJitterHalf(t *testing.T) {
	lowest, highest := time.Hour, time.Duration(0)
	for range 200 {
		d := strategy(durable.RetryAttempt{Err: &SupplierBusyError{Supplier: "acme"}, Attempt: 2})
		if !d.Retry {
			t.Fatal("expected the busy supplier to be retried")
		}
		lowest, highest = min(lowest, d.Delay), max(highest, d.Delay)
	}
	if lowest < 2*time.Second || highest > 4*time.Second {
		t.Errorf("delays ranged %v-%v, want within 2s-4s", lowest, highest)
	}
	if lowest == highest {
		t.Errorf("every delay was %v; want jittered delays", lowest)
	}
}
