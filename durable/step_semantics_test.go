// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
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

// assertReturnWhileSucceedHeld runs op with opts while the call that
// carries the first step SUCCEED is held. When wantReturned is true, the
// test fails unless op returns while the call is held. When it is false,
// the test fails if op returned before the call is released. msg
// describes the failure.
func assertReturnWhileSucceedHeld(t *testing.T, wantReturned bool, msg string, op func(durable.Context) (string, error), opts ...durable.HandlerOption) {
	t.Helper()
	c := newHoldClient(isStepAction(durable.OperationActionSucceed))
	returned := make(chan struct{})
	done := invokeWith(t, func(ctx durable.Context, _ struct{}) (string, error) {
		v, err := op(ctx)
		close(returned)
		return v, err
	}, c, opts...)

	waitHeld(t, c)
	if wantReturned {
		// The timeout only guards against a hang.
		select {
		case <-returned:
		case <-time.After(10 * time.Second):
			t.Fatal(msg)
		}
	} else {
		select {
		case <-returned:
			t.Fatal(msg)
		default:
		}
	}
	close(c.release)
	if s := <-done; s != "SUCCEEDED" {
		t.Fatalf("status = %s, want SUCCEEDED", s)
	}
	assertStartFirst(t, c.updates())
}

func okStep(ctx durable.Context, opts ...durable.StepOption) (string, error) {
	return durable.Step(ctx, "s", func(durable.StepContext) (string, error) { return "ok", nil }, opts...)
}

// A step that sets no semantics uses WithDefaultStepSemantics.
func TestDefaultStepSemanticsAppliesToStep(t *testing.T) {
	assertReturnWhileSucceedHeld(t, true, "step did not use the default AtLeastOnceAsync", func(ctx durable.Context) (string, error) {
		return okStep(ctx)
	}, durable.WithDefaultStepSemantics(durable.AtLeastOnceAsync))
}

// Without WithDefaultStepSemantics a step waits for its SUCCEED.
func TestDefaultStepSemanticsIsAtLeastOnce(t *testing.T) {
	assertReturnWhileSucceedHeld(t, false, "step returned before its SUCCEED was recorded", func(ctx durable.Context) (string, error) {
		return okStep(ctx)
	})
}

// A step's own WithSemantics wins over WithDefaultStepSemantics.
func TestStepSemanticsOverrideDefault(t *testing.T) {
	assertReturnWhileSucceedHeld(t, false, "step's own AtLeastOnce did not win over the default", func(ctx durable.Context) (string, error) {
		return okStep(ctx, durable.WithSemantics(durable.AtLeastOnce))
	}, durable.WithDefaultStepSemantics(durable.AtLeastOnceAsync))
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
