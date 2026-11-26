// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// The deprecated names keep the values of the names that replace them.
func TestDeprecatedStepSemanticsNames(t *testing.T) {
	if durable.AtLeastOncePerRetry != durable.AtLeastOnce { //nolint:staticcheck // checks the deprecated name
		t.Error("AtLeastOncePerRetry != AtLeastOnce")
	}
	if durable.AtMostOncePerRetry != durable.AtMostOnce { //nolint:staticcheck // checks the deprecated name
		t.Error("AtMostOncePerRetry != AtMostOnce")
	}
}

// returnsWhileSucceedHeld runs op with opts and reports whether op
// returned while the call that carries the first step SUCCEED was held.
func returnsWhileSucceedHeld(t *testing.T, op func(durable.Context) (string, error), opts ...durable.HandlerOption) bool {
	t.Helper()
	c := newHoldClient(isStepAction(durable.OperationActionSucceed))
	var released, returnedWhileHeld atomic.Bool
	done := invokeWith(t, func(ctx durable.Context, _ struct{}) (string, error) {
		v, err := op(ctx)
		returnedWhileHeld.Store(!released.Load())
		return v, err
	}, c, opts...)

	waitHeld(t, c)
	// Give an op that does not wait for its SUCCEED the time to return.
	time.Sleep(50 * time.Millisecond)
	released.Store(true)
	close(c.release)
	if s := <-done; s != "SUCCEEDED" {
		t.Fatalf("status = %s, want SUCCEEDED", s)
	}
	assertStartFirst(t, c.updates())
	return returnedWhileHeld.Load()
}

func okStep(ctx durable.Context, opts ...durable.StepOption) (string, error) {
	return durable.Step(ctx, "s", func(durable.StepContext) (string, error) { return "ok", nil }, opts...)
}

// A step that sets no semantics uses WithDefaultStepSemantics.
func TestDefaultStepSemanticsAppliesToStep(t *testing.T) {
	if !returnsWhileSucceedHeld(t, func(ctx durable.Context) (string, error) {
		return okStep(ctx)
	}, durable.WithDefaultStepSemantics(durable.AtLeastOnceAsync)) {
		t.Error("step did not use the default AtLeastOnceAsync")
	}
}

// Without WithDefaultStepSemantics a step waits for its SUCCEED.
func TestDefaultStepSemanticsIsAtLeastOnce(t *testing.T) {
	if returnsWhileSucceedHeld(t, func(ctx durable.Context) (string, error) {
		return okStep(ctx)
	}) {
		t.Error("step returned before its SUCCEED was recorded")
	}
}

// A step's own WithSemantics wins over WithDefaultStepSemantics.
func TestStepSemanticsOverrideDefault(t *testing.T) {
	if returnsWhileSucceedHeld(t, func(ctx durable.Context) (string, error) {
		return okStep(ctx, durable.WithSemantics(durable.AtLeastOnce))
	}, durable.WithDefaultStepSemantics(durable.AtLeastOnceAsync)) {
		t.Error("step's own AtLeastOnce did not win over the default")
	}
}

// stepReturnsWhileSucceedHeld runs run with WithDefaultStepSemantics set
// to AtLeastOnceAsync. run runs a step in a nested context and calls
// returned once the step returns. The test fails unless the step returns
// while the call that carries its SUCCEED is held.
func stepReturnsWhileSucceedHeld(t *testing.T, run func(ctx durable.Context, returned func()) error) {
	t.Helper()
	c := newHoldClient(isStepAction(durable.OperationActionSucceed))
	stepReturned := make(chan struct{})
	done := invokeWith(t, func(ctx durable.Context, _ struct{}) (string, error) {
		return "", run(ctx, func() { close(stepReturned) })
	}, c, durable.WithDefaultStepSemantics(durable.AtLeastOnceAsync))

	waitHeld(t, c)
	select {
	case <-stepReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("step did not return while its SUCCEED was in flight")
	}
	close(c.release)
	if s := <-done; s != "SUCCEEDED" {
		t.Fatalf("status = %s, want SUCCEEDED", s)
	}
}

// WithDefaultStepSemantics reaches the steps of a child context.
func TestDefaultStepSemanticsReachesChildContext(t *testing.T) {
	stepReturnsWhileSucceedHeld(t, func(ctx durable.Context, returned func()) error {
		_, err := durable.RunInChildContext(ctx, "child", func(cctx durable.Context) (string, error) {
			v, err := okStep(cctx)
			returned()
			return v, err
		})
		return err
	})
}

// WithDefaultStepSemantics reaches the steps of Map items.
func TestDefaultStepSemanticsReachesMapItems(t *testing.T) {
	stepReturnsWhileSucceedHeld(t, func(ctx durable.Context, returned func()) error {
		_, err := durable.Map(ctx, "m", []int{1}, func(ictx durable.Context, _ int, _ int) (string, error) {
			v, err := okStep(ictx)
			returned()
			return v, err
		})
		return err
	})
}
