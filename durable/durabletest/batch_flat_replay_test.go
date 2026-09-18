// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestFlatSequentialSideEffectReexecutes asserts that a side-effecting step
// placed after a sequential FLAT batch runs exactly once across the
// invocations of one execution. The batch consumes one operation ID on the
// live invocation; replay of the terminal batch must consume the same
// number, or every operation after it shifts to a new ID and re-executes.
func TestFlatSequentialSideEffectReexecutes(t *testing.T) {
	var sideRuns int32
	h := func(ctx durable.Context, _ any) (int32, error) {
		if _, err := durable.Map(ctx, "m", []int{1, 2}, func(c durable.Context, item int, _ int) (int, error) {
			return durable.Step(c, "s", func(durable.StepContext) (int, error) { return item, nil })
		}, durable.WithMaxConcurrency(1), durable.WithNesting(durable.NestingFlat)); err != nil {
			return 0, err
		}
		if _, err := durable.Step(ctx, "side", func(durable.StepContext) (int, error) {
			return int(atomic.AddInt32(&sideRuns, 1)), nil
		}); err != nil {
			return 0, err
		}
		if err := durable.Wait(ctx, "w", time.Second); err != nil {
			return 0, err
		}
		return atomic.LoadInt32(&sideRuns), nil
	}
	res := durabletest.NewLocalRunner(h).RunUntilComplete(t, nil)
	if res.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", res.Status)
	}
	if sideRuns != 1 {
		t.Errorf("side step body executed %d times across invocations; want 1", sideRuns)
	}
}

// TestFlatSequentialReplayIDDrift observes the same defect as a duplicated
// WAIT operation: the wait after the batch must be checkpointed once.
func TestFlatSequentialReplayIDDrift(t *testing.T) {
	var afterRuns int32
	h := func(ctx durable.Context, _ any) (string, error) {
		_, err := durable.Map(ctx, "m", []int{1, 2}, func(c durable.Context, item int, _ int) (int, error) {
			return durable.Step(c, "s", func(durable.StepContext) (int, error) { return item, nil })
		}, durable.WithMaxConcurrency(1), durable.WithNesting(durable.NestingFlat))
		if err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "w", time.Second); err != nil {
			return "", err
		}
		if _, err := durable.Step(ctx, "after", func(durable.StepContext) (int, error) {
			atomic.AddInt32(&afterRuns, 1)
			return 0, nil
		}); err != nil {
			return "", err
		}
		return "ok", nil
	}
	res := durabletest.NewLocalRunner(h).RunUntilComplete(t, nil)
	if res.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", res.Status)
	}
	waits := 0
	for _, op := range res.Operations {
		if op.Type == "WAIT" {
			waits++
		}
	}
	if waits != 1 {
		t.Errorf("expected exactly 1 WAIT operation, got %d", waits)
	}
	if afterRuns != 1 {
		t.Errorf("after-step ran %d times", afterRuns)
	}
}
