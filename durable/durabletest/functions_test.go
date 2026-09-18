// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

type priceRequest struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type priceQuote struct {
	SKU   string `json:"sku"`
	Total int    `json:"total"`
}

const pricingFn = "pricing-function"

// callerHandler invokes pricingFn and returns the quoted total.
func callerHandler(ctx durable.Context, req priceRequest) (int, error) {
	quote, err := durable.Invoke[priceQuote](ctx, "quote", pricingFn, req)
	if err != nil {
		return 0, err
	}
	return quote.Total, nil
}

func TestRegisteredDurableTargetSuccess(t *testing.T) {
	var targetRuns int
	pricing := func(ctx durable.Context, req priceRequest) (priceQuote, error) {
		targetRuns++
		total, err := durable.Step(ctx, "compute", func(durable.StepContext) (int, error) {
			return req.Quantity * 3, nil
		})
		if err != nil {
			return priceQuote{}, err
		}
		return priceQuote{SKU: req.SKU, Total: total}, nil
	}

	runner := durabletest.NewLocalRunner(callerHandler)
	runner.RegisterFunction(pricingFn, durabletest.DurableFunction(pricing))

	result := runner.RunUntilComplete(t, priceRequest{SKU: "widget", Quantity: 4})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error: %+v)", result.Status, result.Error)
	}
	total, err := durabletest.ResultAs[int](result)
	if err != nil {
		t.Fatal(err)
	}
	if total != 12 {
		t.Errorf("total = %d, want 12", total)
	}
	if targetRuns != 1 {
		t.Errorf("target ran %d times, want 1", targetRuns)
	}

	op := result.Operation("quote")
	if op == nil || op.Type != "CHAINED_INVOKE" {
		t.Fatalf("quote operation = %+v, want CHAINED_INVOKE", op)
	}
	if op.Status != "SUCCEEDED" {
		t.Errorf("quote status = %s, want SUCCEEDED", op.Status)
	}
	if op.InvokeDetails == nil || op.InvokeDetails.Result != `{"sku":"widget","total":12}` {
		t.Errorf("quote result = %+v, want the target's serialized return value", op.InvokeDetails)
	}
}

func TestRegisteredDurableTargetRunExecutesTarget(t *testing.T) {
	pricing := func(_ durable.Context, req priceRequest) (priceQuote, error) {
		return priceQuote{SKU: req.SKU, Total: req.Quantity}, nil
	}

	runner := durabletest.NewLocalRunner(callerHandler)
	runner.RegisterFunction(pricingFn, durabletest.DurableFunction(pricing))

	// The first invocation suspends on the invoke; the target runs before
	// Run returns, so the operation is already settled.
	first := runner.Run(t, priceRequest{SKU: "a", Quantity: 7})
	if first.Status != durabletest.Pending {
		t.Fatalf("first status = %s, want PENDING", first.Status)
	}
	if op := first.Operation("quote"); op == nil || op.Status != "SUCCEEDED" {
		t.Fatalf("quote after first Run = %+v, want SUCCEEDED", op)
	}

	second := runner.Run(t, priceRequest{SKU: "a", Quantity: 7})
	if second.Status != durabletest.Succeeded {
		t.Fatalf("second status = %s, want SUCCEEDED", second.Status)
	}
}

type pricingUnavailable struct{ region string }

func (e *pricingUnavailable) Error() string { return "pricing unavailable in " + e.region }

// errorString and wrapError are user-defined error types whose names collide
// with unexported standard-library error types. They must keep their own
// names when recorded.
type errorString struct{ msg string }

func (e *errorString) Error() string { return e.msg }

type wrapError struct{ inner error }

func (e wrapError) Error() string { return "wrapped: " + e.inner.Error() }

func TestRegisteredDurableTargetFailure(t *testing.T) {
	pricing := func(ctx durable.Context, req priceRequest) (priceQuote, error) {
		_, err := durable.Step(ctx, "lookup", func(durable.StepContext) (int, error) {
			return 0, &pricingUnavailable{region: "eu"}
		})
		return priceQuote{}, err
	}

	var seen *durable.InvokeError
	handler := func(ctx durable.Context, req priceRequest) (int, error) {
		_, err := durable.Invoke[priceQuote](ctx, "quote", pricingFn, req)
		if err != nil {
			var ie *durable.InvokeError
			if errors.As(err, &ie) {
				seen = ie
			}
			return 0, err
		}
		return 0, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	runner.RegisterFunction(pricingFn, durabletest.DurableFunction(pricing))

	result := runner.RunUntilComplete(t, priceRequest{SKU: "widget", Quantity: 1})
	if result.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", result.Status)
	}
	if result.Error == nil || result.Error.Type != "InvokeError" {
		t.Fatalf("error = %+v, want InvokeError", result.Error)
	}
	if seen == nil {
		t.Fatal("handler did not observe a *durable.InvokeError")
	}
	if seen.Name != "quote" || seen.FunctionID != pricingFn {
		t.Errorf("InvokeError name/function = %q/%q, want quote/%s", seen.Name, seen.FunctionID, pricingFn)
	}
	if seen.Status != durable.OperationStatusFailed {
		t.Errorf("InvokeError status = %s, want FAILED", seen.Status)
	}
	// The target's own failure is a StepError wrapping the step's cause;
	// the recorded type and message are the ones the target's invocation
	// response carried.
	if seen.ErrorType != "StepError" {
		t.Errorf("InvokeError type = %q, want StepError", seen.ErrorType)
	}
	if !strings.Contains(seen.Message, "pricing unavailable in eu") {
		t.Errorf("InvokeError message = %q, want it to carry the step failure", seen.Message)
	}

	op := result.Operation("quote")
	if op == nil || op.InvokeDetails == nil {
		t.Fatalf("quote operation = %+v, want invoke details", op)
	}
	if op.Status != "FAILED" || op.InvokeDetails.ErrorType != "StepError" {
		t.Errorf("quote op = %s/%s, want FAILED/StepError", op.Status, op.InvokeDetails.ErrorType)
	}
}

// TestRegisteredTargetErrorShapeMatchesStub runs the same caller against a
// registered target and against FailChainedInvoke with the same error, and
// requires the caller to observe identical InvokeError fields.
func TestRegisteredTargetErrorShapeMatchesStub(t *testing.T) {
	capture := func() (durable.Handler[priceRequest, int], **durable.InvokeError) {
		var seen *durable.InvokeError
		h := func(ctx durable.Context, req priceRequest) (int, error) {
			_, err := durable.Invoke[priceQuote](ctx, "quote", pricingFn, req)
			if err != nil {
				var ie *durable.InvokeError
				if errors.As(err, &ie) {
					seen = ie
				}
			}
			return 0, err
		}
		return h, &seen
	}

	// Stubbed path.
	stubHandler, stubSeen := capture()
	stub := durabletest.NewLocalRunner(stubHandler)
	if r := stub.RunUntilComplete(t, priceRequest{}); r.Status != durabletest.Pending {
		t.Fatalf("stub first status = %s, want PENDING", r.Status)
	}
	if err := stub.FailChainedInvoke("quote", "Error", "boom"); err != nil {
		t.Fatal(err)
	}
	if r := stub.RunUntilComplete(t, priceRequest{}); r.Status != durabletest.Failed {
		t.Fatalf("stub second status = %s, want FAILED", r.Status)
	}

	// Registered plain target that fails the same way.
	regHandler, regSeen := capture()
	reg := durabletest.NewLocalRunner(regHandler)
	reg.RegisterFunction(pricingFn, durabletest.PlainFunction(func(context.Context, priceRequest) (priceQuote, error) {
		return priceQuote{}, errors.New("boom")
	}))
	if r := reg.RunUntilComplete(t, priceRequest{}); r.Status != durabletest.Failed {
		t.Fatalf("registered status = %s, want FAILED", r.Status)
	}

	if *stubSeen == nil || *regSeen == nil {
		t.Fatal("both callers must observe an InvokeError")
	}
	s, r := *stubSeen, *regSeen
	if s.Name != r.Name || s.FunctionID != r.FunctionID || s.Status != r.Status ||
		s.ErrorType != r.ErrorType || s.Message != r.Message || s.ErrorData != r.ErrorData {
		t.Errorf("InvokeError shape differs:\n stub: %+v\n registered: %+v", s, r)
	}
	if s.Error() != r.Error() {
		t.Errorf("InvokeError text differs: %q vs %q", s.Error(), r.Error())
	}
}

func TestRegisteredDurableTargetSuspendsAndResumes(t *testing.T) {
	var targetInvocations int
	pricing := func(ctx durable.Context, req priceRequest) (priceQuote, error) {
		targetInvocations++
		if err := durable.Wait(ctx, "cooldown", time.Hour); err != nil {
			return priceQuote{}, err
		}
		return priceQuote{SKU: req.SKU, Total: req.Quantity * 2}, nil
	}

	runner := durabletest.NewLocalRunner(callerHandler)
	runner.RegisterFunction(pricingFn, durabletest.DurableFunction(pricing))

	result := runner.RunUntilComplete(t, priceRequest{SKU: "w", Quantity: 5})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error: %+v)", result.Status, result.Error)
	}
	total, err := durabletest.ResultAs[int](result)
	if err != nil {
		t.Fatal(err)
	}
	if total != 10 {
		t.Errorf("total = %d, want 10", total)
	}
	// The target suspended on its wait and was re-invoked once the timer
	// completed: one invocation to start the wait, one to finish.
	if targetInvocations != 2 {
		t.Errorf("target invocations = %d, want 2 (suspend on wait, then resume)", targetInvocations)
	}
}

func TestRegisteredDurableTargetBlockedOnCallbackStaysOpen(t *testing.T) {
	pricing := func(ctx durable.Context, req priceRequest) (priceQuote, error) {
		cb, err := durable.CreateCallback[string](ctx, "approval")
		if err != nil {
			return priceQuote{}, err
		}
		if _, err := cb.Result(); err != nil {
			return priceQuote{}, err
		}
		return priceQuote{SKU: req.SKU}, nil
	}

	runner := durabletest.NewLocalRunner(callerHandler)
	runner.RegisterFunction(pricingFn, durabletest.DurableFunction(pricing))

	result := runner.RunUntilComplete(t, priceRequest{SKU: "w"})
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING while the target awaits its callback", result.Status)
	}
	if result.CapReached {
		t.Error("CapReached = true, want false: the target is blocked, not spinning")
	}
	if op := result.Operation("quote"); op == nil || op.Status != "STARTED" {
		t.Errorf("quote op = %+v, want STARTED", op)
	}
}

func TestRegisteredPlainTargetSuccess(t *testing.T) {
	var got priceRequest
	runner := durabletest.NewLocalRunner(callerHandler)
	runner.RegisterFunction(pricingFn, durabletest.PlainFunction(func(_ context.Context, req priceRequest) (priceQuote, error) {
		got = req
		return priceQuote{SKU: req.SKU, Total: req.Quantity * 10}, nil
	}))

	result := runner.RunUntilComplete(t, priceRequest{SKU: "bolt", Quantity: 3})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error: %+v)", result.Status, result.Error)
	}
	total, err := durabletest.ResultAs[int](result)
	if err != nil {
		t.Fatal(err)
	}
	if total != 30 {
		t.Errorf("total = %d, want 30", total)
	}
	if got != (priceRequest{SKU: "bolt", Quantity: 3}) {
		t.Errorf("plain target received %+v, want the invoke input", got)
	}
}

func TestRegisteredPlainTargetFailureTypes(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantType string
	}{
		{name: "unnamed", err: fmt.Errorf("no price for %s", "x"), wantType: "Error"},
		{name: "wrapped", err: fmt.Errorf("quote: %w", errors.New("timeout")), wantType: "Error"},
		{name: "multi-wrapped", err: fmt.Errorf("quote: %w and %w", errors.New("timeout"), errors.New("throttled")), wantType: "Error"},
		{name: "joined", err: errors.Join(errors.New("timeout"), errors.New("throttled")), wantType: "Error"},
		{name: "named", err: &pricingUnavailable{region: "us"}, wantType: "pricingUnavailable"},
		{name: "named-collides-with-errors", err: &errorString{msg: "no price"}, wantType: "errorString"},
		{name: "named-collides-with-fmt", err: wrapError{inner: errors.New("timeout")}, wantType: "wrapError"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runner := durabletest.NewLocalRunner(callerHandler)
			runner.RegisterFunction(pricingFn, durabletest.PlainFunction(func(context.Context, priceRequest) (priceQuote, error) {
				return priceQuote{}, tc.err
			}))

			result := runner.RunUntilComplete(t, priceRequest{})
			if result.Status != durabletest.Failed {
				t.Fatalf("status = %s, want FAILED", result.Status)
			}
			if result.Error == nil || result.Error.Type != "InvokeError" {
				t.Fatalf("error = %+v, want InvokeError", result.Error)
			}
			op := result.Operation("quote")
			if op == nil || op.InvokeDetails == nil {
				t.Fatalf("quote op = %+v, want invoke details", op)
			}
			if op.InvokeDetails.ErrorType != tc.wantType || op.InvokeDetails.ErrorMessage != tc.err.Error() {
				t.Errorf("recorded error = %q/%q, want %q/%q",
					op.InvokeDetails.ErrorType, op.InvokeDetails.ErrorMessage, tc.wantType, tc.err.Error())
			}
		})
	}
}

func TestUnregisteredInvokeStillStubbed(t *testing.T) {
	handler := func(ctx durable.Context, req priceRequest) (int, error) {
		quote, err := durable.Invoke[priceQuote](ctx, "quote", pricingFn, req)
		if err != nil {
			return 0, err
		}
		tax, err := durable.Invoke[int](ctx, "tax", "tax-function", quote.Total)
		if err != nil {
			return 0, err
		}
		return quote.Total + tax, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	runner.RegisterFunction(pricingFn, durabletest.PlainFunction(func(_ context.Context, req priceRequest) (priceQuote, error) {
		return priceQuote{Total: req.Quantity}, nil
	}))

	// The registered target settles; the unregistered one blocks.
	result := runner.RunUntilComplete(t, priceRequest{Quantity: 40})
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING on the unregistered invoke", result.Status)
	}
	if op := result.Operation("quote"); op == nil || op.Status != "SUCCEEDED" {
		t.Errorf("quote op = %+v, want SUCCEEDED", op)
	}
	if op := result.Operation("tax"); op == nil || op.Status != "STARTED" {
		t.Errorf("tax op = %+v, want STARTED", op)
	}

	if err := runner.CompleteChainedInvoke("tax", 2); err != nil {
		t.Fatal(err)
	}
	result = runner.RunUntilComplete(t, priceRequest{Quantity: 40})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	total, err := durabletest.ResultAs[int](result)
	if err != nil {
		t.Fatal(err)
	}
	if total != 42 {
		t.Errorf("total = %d, want 42", total)
	}
}

// fxCaller is a durable target that invokes an unregistered identifier, so
// the invoke inside the target must be resolved through the runner.
func fxCaller(ctx durable.Context, req priceRequest) (priceQuote, error) {
	rate, err := durable.Invoke[int](ctx, "fx", "fx-function", req.SKU)
	if err != nil {
		return priceQuote{}, err
	}
	return priceQuote{SKU: req.SKU, Total: req.Quantity * rate}, nil
}

func TestRegisteredDurableTargetInvokesUnregisteredTarget(t *testing.T) {
	runner := durabletest.NewLocalRunner(callerHandler)
	runner.RegisterFunction(pricingFn, durabletest.DurableFunction(fxCaller))

	// The target runs, reaches its own unregistered invoke, and blocks.
	// The caller's invoke of the target therefore stays open too.
	result := runner.RunUntilComplete(t, priceRequest{SKU: "eur", Quantity: 3})
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING while the target awaits its invoke", result.Status)
	}
	if result.CapReached {
		t.Error("CapReached = true, want false: the target is blocked, not spinning")
	}
	if op := result.Operation("quote"); op == nil || op.Status != "STARTED" {
		t.Fatalf("quote op = %+v, want STARTED", op)
	}
	// The target's invoke lives in the target's own checkpoint log, not in
	// the caller's operations.
	if op := result.Operation("fx"); op != nil {
		t.Errorf("fx op = %+v in the caller's operations, want none", op)
	}

	// Resolving the target's invoke by name reaches into the target.
	if err := runner.CompleteChainedInvoke("fx", 7); err != nil {
		t.Fatalf("CompleteChainedInvoke(fx): %v", err)
	}
	result = runner.RunUntilComplete(t, priceRequest{SKU: "eur", Quantity: 3})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error: %+v)", result.Status, result.Error)
	}
	total, err := durabletest.ResultAs[int](result)
	if err != nil {
		t.Fatal(err)
	}
	if total != 21 {
		t.Errorf("total = %d, want 21", total)
	}
	if op := result.Operation("quote"); op == nil || op.Status != "SUCCEEDED" {
		t.Errorf("quote op = %+v, want SUCCEEDED", op)
	}
}

func TestRegisteredDurableTargetInvokeFailedThroughRunner(t *testing.T) {
	var seen *durable.InvokeError
	handler := func(ctx durable.Context, req priceRequest) (int, error) {
		_, err := durable.Invoke[priceQuote](ctx, "quote", pricingFn, req)
		var ie *durable.InvokeError
		if errors.As(err, &ie) {
			seen = ie
		}
		return 0, err
	}

	runner := durabletest.NewLocalRunner(handler)
	runner.RegisterFunction(pricingFn, durabletest.DurableFunction(fxCaller))

	if r := runner.RunUntilComplete(t, priceRequest{SKU: "eur"}); r.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", r.Status)
	}
	if err := runner.FailChainedInvoke("fx", "RateUnavailable", "no rate for eur"); err != nil {
		t.Fatalf("FailChainedInvoke(fx): %v", err)
	}
	result := runner.RunUntilComplete(t, priceRequest{SKU: "eur"})
	if result.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", result.Status)
	}
	if seen == nil {
		t.Fatal("caller did not observe an InvokeError")
	}
	// The target failed with the InvokeError of its own invoke, so the
	// caller sees an InvokeError whose recorded type is InvokeError and
	// whose message carries the stubbed failure.
	if seen.Name != "quote" || seen.ErrorType != "InvokeError" {
		t.Errorf("InvokeError name/type = %q/%q, want quote/InvokeError", seen.Name, seen.ErrorType)
	}
	if !strings.Contains(seen.Message, "no rate for eur") {
		t.Errorf("InvokeError message = %q, want it to carry the stubbed failure", seen.Message)
	}
}

func TestChainedInvokeNameOpenInSeveralExecutionsIsRejected(t *testing.T) {
	// The caller and the target both have an open invoke named "call":
	// the caller's targets the registered function, the target's targets
	// an unregistered one.
	target := func(ctx durable.Context, n int) (int, error) {
		return durable.Invoke[int](ctx, "call", "unregistered-fn", n)
	}
	handler := func(ctx durable.Context, n int) (int, error) {
		return durable.Invoke[int](ctx, "call", "target-fn", n)
	}

	runner := durabletest.NewLocalRunner(handler)
	runner.RegisterFunction("target-fn", durabletest.DurableFunction(target))
	if r := runner.RunUntilComplete(t, 1); r.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", r.Status)
	}

	err := runner.CompleteChainedInvoke("call", 2)
	if err == nil {
		t.Fatal("CompleteChainedInvoke settled an ambiguous name, want an error")
	}
	if !strings.Contains(err.Error(), "open in 2 executions") {
		t.Errorf("error = %q, want it to report the ambiguity", err)
	}
	if err := runner.FailChainedInvoke("call", "Error", "boom"); err == nil {
		t.Fatal("FailChainedInvoke settled an ambiguous name, want an error")
	}

	// Nothing was settled: both invokes are still open.
	result := runner.RunUntilComplete(t, 1)
	if result.Status != durabletest.Pending {
		t.Fatalf("status after rejected completion = %s, want PENDING", result.Status)
	}
	if op := result.Operation("call"); op == nil || op.Status != "STARTED" {
		t.Errorf("caller's call op = %+v, want STARTED", op)
	}
}

func TestUnknownChainedInvokeNameStillReportsNotFound(t *testing.T) {
	runner := durabletest.NewLocalRunner(callerHandler)
	runner.RegisterFunction(pricingFn, durabletest.DurableFunction(fxCaller))
	if r := runner.RunUntilComplete(t, priceRequest{SKU: "eur"}); r.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", r.Status)
	}
	err := runner.CompleteChainedInvoke("no-such-invoke", 1)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %v, want a not-found error", err)
	}
}

// waitForever is a durable target that never settles: every invocation
// starts a new wait, and every timer completion re-invokes it.
func waitForever(ctx durable.Context, n int) (int, error) {
	for i := 0; ; i++ {
		if err := durable.Wait(ctx, fmt.Sprintf("wait-%d", i), time.Hour); err != nil {
			return 0, err
		}
	}
}

func TestRunReportsCapReachedByRegisteredTarget(t *testing.T) {
	handler := func(ctx durable.Context, n int) (int, error) {
		return durable.Invoke[int](ctx, "spin", "spinner-fn", n)
	}
	runner := durabletest.NewLocalRunner(handler)
	runner.RegisterFunction("spinner-fn", durabletest.DurableFunction(waitForever))

	result := runner.Run(t, 1)
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", result.Status)
	}
	if !result.CapReached {
		t.Error("CapReached = false, want true: the target exhausted its invocation cap")
	}
	if op := result.Operation("spin"); op == nil || op.Status != "STARTED" {
		t.Errorf("spin op = %+v, want STARTED", op)
	}
}

func TestRunUntilCompleteReportsCapReachedByRegisteredTarget(t *testing.T) {
	handler := func(ctx durable.Context, n int) (int, error) {
		return durable.Invoke[int](ctx, "spin", "spinner-fn", n)
	}
	runner := durabletest.NewLocalRunner(handler)
	runner.RegisterFunction("spinner-fn", durabletest.DurableFunction(waitForever))

	result := runner.RunUntilComplete(t, 1, durabletest.WithMaxInvocations(5))
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", result.Status)
	}
	if !result.CapReached {
		t.Error("CapReached = false, want true: the target exhausted its invocation cap")
	}
}

func TestRegisteredTargetsNest(t *testing.T) {
	// caller -> "outer" (durable) -> "inner" (plain).
	inner := func(_ context.Context, n int) (int, error) { return n + 1, nil }
	outer := func(ctx durable.Context, n int) (int, error) {
		v, err := durable.Invoke[int](ctx, "inner", "inner-fn", n*2)
		if err != nil {
			return 0, err
		}
		return v, nil
	}
	handler := func(ctx durable.Context, n int) (int, error) {
		return durable.Invoke[int](ctx, "outer", "outer-fn", n)
	}

	runner := durabletest.NewLocalRunner(handler)
	runner.RegisterFunction("outer-fn", durabletest.DurableFunction(outer))
	runner.RegisterFunction("inner-fn", durabletest.PlainFunction(inner))

	result := runner.RunUntilComplete(t, 20)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error: %+v)", result.Status, result.Error)
	}
	got, err := durabletest.ResultAs[int](result)
	if err != nil {
		t.Fatal(err)
	}
	if got != 41 {
		t.Errorf("result = %d, want 41", got)
	}
}

func TestRegisteredTargetRecursionIsBounded(t *testing.T) {
	const self = "recursive-fn"
	// Each depth receives a distinct input, so the set of inputs seen is
	// the set of depths that ran. An execution is invoked twice per depth
	// (once to start the invoke, once to resume), so invocations are not
	// counted.
	depthsSeen := map[int]bool{}
	recurse := func(ctx durable.Context, n int) (int, error) {
		depthsSeen[n] = true
		return durable.Invoke[int](ctx, "again", self, n+1)
	}

	var seen *durable.InvokeError
	handler := func(ctx durable.Context, n int) (int, error) {
		v, err := durable.Invoke[int](ctx, "start", self, n)
		var ie *durable.InvokeError
		if errors.As(err, &ie) {
			seen = ie
		}
		return v, err
	}

	runner := durabletest.NewLocalRunner(handler)
	runner.RegisterFunction(self, durabletest.DurableFunction(recurse))

	result := runner.RunUntilComplete(t, 0)
	if result.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED (recursion must be bounded)", result.Status)
	}
	if result.CapReached {
		t.Error("CapReached = true, want the depth bound to stop the recursion first")
	}
	// The target ran once at each permitted depth, and the invoke that
	// would have started depth MaxInvokeDepth+1 failed.
	if len(depthsSeen) != durabletest.MaxInvokeDepth {
		t.Errorf("target ran at %d depths, want %d", len(depthsSeen), durabletest.MaxInvokeDepth)
	}
	if seen == nil {
		t.Fatal("caller did not observe an InvokeError")
	}
	// Each level fails with the InvokeError of the level below, so the
	// outermost error's recorded type is the SDK's InvokeError. The depth
	// failure itself is recorded on the deepest invoke operation, which
	// this test verifies through the caller's chain of messages.
	if !strings.Contains(seen.Message, "MaxInvokeDepth") {
		t.Errorf("InvokeError message = %q, want it to name MaxInvokeDepth", seen.Message)
	}
}

func TestRegisterFunctionRejectsZeroValue(t *testing.T) {
	runner := durabletest.NewLocalRunner(callerHandler)
	defer func() {
		if recover() == nil {
			t.Error("RegisterFunction accepted a zero Function, want panic")
		}
	}()
	runner.RegisterFunction(pricingFn, durabletest.Function{})
}
