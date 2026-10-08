// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// Case 1. The service rejects a STEP output over 262144 bytes. The local
// client rejects a step result of 300000 bytes the same way, so the
// execution fails.
func TestDurabletestRejectsOversizedStepResult(t *testing.T) {
	h := func(ctx durable.Context, _ struct{}) (int, error) {
		s, err := durable.Step(ctx, "big", func(_ durable.StepContext) (string, error) {
			return strings.Repeat("x", 300000), nil
		})
		if err != nil {
			return 0, err
		}
		return len(s), nil
	}
	res, err := durabletest.NewLocalRunner(h).RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED (the service rejects the step result)", res.Status)
	}
	if res.Error == nil || !strings.Contains(res.Error.Message, stepPayloadMessage) {
		t.Fatalf("error = %+v, want the service's step payload message", res.Error)
	}
}

// Case 2. A wait of zero seconds checkpoints WaitSeconds 0. The service
// rejects WaitSeconds 0, and so does the local client.
func TestDurabletestRejectsZeroSecondWait(t *testing.T) {
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		if err := durable.Wait(ctx, "w", 0); err != nil {
			return "", err
		}
		return "ok", nil
	}
	res, err := durabletest.NewLocalRunner(h).RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED (the service rejects a 0-second wait)", res.Status)
	}
	if res.Error == nil || !strings.Contains(res.Error.Message, zeroWaitMessage) {
		t.Fatalf("error = %+v, want the service's waitSeconds message", res.Error)
	}
}

// Case 3. The handler starts a 1-second wait, runs a step, then awaits the
// wait. The wait elapses while the step runs, and the response to the
// step's checkpoint reports it. So the handler finishes in one Run.
func TestDurabletestCompletesWaitDuringInvocation(t *testing.T) {
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		pause := durable.WaitAsync(ctx, "pause", time.Second)
		time.Sleep(50 * time.Millisecond) // the wait START reaches the client first
		if _, err := durable.Step(ctx, "work", func(_ durable.StepContext) (string, error) {
			return "worked", nil
		}); err != nil {
			return "", err
		}
		if _, err := pause.Result(ctx); err != nil {
			return "", err
		}
		return "done", nil
	}
	r := durabletest.NewLocalRunner(h)
	res, err := r.Run(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != durabletest.Succeeded {
		t.Fatalf("status after one Run = %s, want SUCCEEDED", res.Status)
	}
	op := res.Operation("pause")
	if op == nil {
		t.Fatal("no pause operation recorded after one Run")
	}
	if op.Status != "SUCCEEDED" {
		t.Fatalf("pause status after one Run = %s, want SUCCEEDED (the wait was completed within the invocation)", op.Status)
	}
	if r.CompletePendingTimers() {
		t.Fatal("CompletePendingTimers completed a timer; the wait should already have completed during the invocation")
	}
}
