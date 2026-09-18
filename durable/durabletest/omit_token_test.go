// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"sync/atomic"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestOmitTokenOnCheckpointEndsInvocationPending(t *testing.T) {
	// The very first checkpoint call, the first step's START, gets a
	// response without a token. The invocation ends PENDING without an
	// error, the first step is recorded as STARTED, and neither step body
	// has run. The next invocation resumes from the recorded state and
	// completes.
	var firstRuns, secondRuns atomic.Int32
	handler := func(ctx durable.Context, event string) (string, error) {
		a, err := durable.Step(ctx, "first", func(_ durable.StepContext) (string, error) {
			firstRuns.Add(1)
			return "A", nil
		})
		if err != nil {
			return "", err
		}
		b, err := durable.Step(ctx, "second", func(_ durable.StepContext) (string, error) {
			secondRuns.Add(1)
			return "B", nil
		})
		if err != nil {
			return "", err
		}
		return a + b + event, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	runner.OmitTokenOnCheckpoint(1)

	result := runner.RunUntilComplete(t, "!")
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", result.Status)
	}
	if result.Error != nil {
		t.Errorf("error = %+v, want none", result.Error)
	}
	if op := result.Operation("first"); op == nil || op.Status != "STARTED" {
		t.Errorf("first = %+v, want STARTED", op)
	}
	if op := result.Operation("second"); op != nil {
		t.Errorf("second = %+v, want not recorded", op)
	}
	if got := firstRuns.Load(); got != 0 {
		t.Errorf("first step ran %d times before resumption, want 0 (its START was refused)", got)
	}
	if got := secondRuns.Load(); got != 0 {
		t.Errorf("second step ran %d times before resumption, want 0", got)
	}

	result = runner.RunUntilComplete(t, "!")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	out, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatal(err)
	}
	if out != "AB!" {
		t.Errorf("result = %q, want %q", out, "AB!")
	}
	if got := firstRuns.Load(); got != 1 {
		t.Errorf("first step ran %d times, want 1", got)
	}
	if got := secondRuns.Load(); got != 1 {
		t.Errorf("second step ran %d times, want 1", got)
	}
}

func TestOmitTokenOnCheckpointTargetsNthCall(t *testing.T) {
	// A step checkpoints twice: START, then SUCCEED. Scheduling the
	// omission on the second call lets the first step's START through and
	// withholds the token on its SUCCEED. The SUCCEED is still recorded,
	// so the first step does not run again, and the second step has not
	// started.
	var firstRuns atomic.Int32
	handler := func(ctx durable.Context, _ string) (string, error) {
		if _, err := durable.Step(ctx, "first", func(_ durable.StepContext) (string, error) {
			firstRuns.Add(1)
			return "A", nil
		}); err != nil {
			return "", err
		}
		return durable.Step(ctx, "second", func(_ durable.StepContext) (string, error) {
			return "B", nil
		})
	}

	runner := durabletest.NewLocalRunner(handler)
	runner.OmitTokenOnCheckpoint(2)

	result := runner.Run(t, "")
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", result.Status)
	}
	if op := result.Operation("first"); op == nil || op.Status != "SUCCEEDED" {
		t.Errorf("first = %+v, want SUCCEEDED (its SUCCEED was in the tokenless response)", op)
	}
	if op := result.Operation("second"); op != nil {
		t.Errorf("second = %+v, want not recorded", op)
	}

	result = runner.RunUntilComplete(t, "")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	if got := firstRuns.Load(); got != 1 {
		t.Errorf("first step ran %d times, want 1 (its result was recorded)", got)
	}
}

func TestOmitTokenOnCheckpointRejectsZero(t *testing.T) {
	runner := durabletest.NewLocalRunner(func(_ durable.Context, _ string) (string, error) {
		return "", nil
	})
	defer func() {
		if recover() == nil {
			t.Error("OmitTokenOnCheckpoint(0) did not panic")
		}
	}()
	runner.OmitTokenOnCheckpoint(0)
}
