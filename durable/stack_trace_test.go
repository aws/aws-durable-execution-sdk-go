package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable/internal/wire"
	"github.com/aws/aws-sdk-go-v2/aws"
)

// The functions below are named so their frames can be located in a trace.

// stackTraceInner is the innermost frame of the capture fixtures.
func stackTraceInner(skip int) []string { return captureStackTrace(skip) }

// stackTraceOuter calls stackTraceInner so a trace holds both frames.
func stackTraceOuter(skip int) []string { return stackTraceInner(skip) }

// stackTraceDeep recurses depth times before capturing, to exceed the bound.
func stackTraceDeep(depth int) []string {
	if depth == 0 {
		return captureStackTrace(0)
	}
	return stackTraceDeep(depth - 1)
}

// stepBodyFails is a step body that returns an error.
func stepBodyFails(StepContext) (string, error) { return "", errors.New("step failed") }

// stepBodyPanics is a step body that panics.
func stepBodyPanics(StepContext) (string, error) { panic("step exploded") }

// handlerFails is a handler that returns an error.
func handlerFails(Context, string) (string, error) { return "", errors.New("handler failed") }

// handlerPanics is a handler that panics through a nested call.
func handlerPanics(Context, string) (string, error) { return explode() }

// childBodyFails is a child context body that returns an error.
func childBodyFails(Context) (string, error) { return "", errors.New("child failed") }

// checkFails is a WaitForCondition check that returns an error.
func checkFails(StepContext, int) (int, error) { return 0, errors.New("check failed") }

// checkSucceeds is a WaitForCondition check that returns its state.
func checkSucceeds(_ StepContext, s int) (int, error) { return s + 1, nil }

// strategyFails is a WaitForCondition strategy that stops with an error.
func strategyFails(int, int) WaitDecision {
	return WaitDecision{Continue: false, Err: errors.New("strategy gave up")}
}

// strategyStops is a WaitForCondition strategy that stops without error.
func strategyStops(int, int) WaitDecision { return WaitDecision{Continue: false} }

// assertSameTrace checks that got equals want frame for frame.
func assertSameTrace(t *testing.T, what string, got, want []string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

// explode is the panicking frame inside handlerPanics.
func explode() (string, error) { panic("handler exploded") }

// frameIndex returns the index of the first frame whose text contains
// substr, or -1.
func frameIndex(trace []string, substr string) int {
	for i, f := range trace {
		if strings.Contains(f, substr) {
			return i
		}
	}
	return -1
}

// assertOrdered checks that frame inner appears in trace before frame outer.
func assertOrdered(t *testing.T, trace []string, inner, outer string) {
	t.Helper()
	i, o := frameIndex(trace, inner), frameIndex(trace, outer)
	if i < 0 {
		t.Fatalf("trace has no frame containing %q:\n%s", inner, strings.Join(trace, "\n"))
	}
	if o < 0 {
		t.Fatalf("trace has no frame containing %q:\n%s", outer, strings.Join(trace, "\n"))
	}
	if i >= o {
		t.Errorf("frame %q at %d is not before frame %q at %d:\n%s", inner, i, outer, o, strings.Join(trace, "\n"))
	}
}

// failedResponse decodes a FAILED invocation response.
func failedResponse(t *testing.T, resp string) wire.ErrorObject {
	t.Helper()
	var out wire.InvocationResponse
	if err := json.Unmarshal([]byte(resp), &out); err != nil {
		t.Fatalf("decode response %s: %v", resp, err)
	}
	if out.Status != wire.StatusFailed || out.Error == nil {
		t.Fatalf("response = %s, want FAILED with an error", resp)
	}
	return *out.Error
}

func TestCaptureStackTraceInnermostFirst(t *testing.T) {
	trace := stackTraceOuter(0)
	if len(trace) < 3 {
		t.Fatalf("trace has %d frames, want at least inner, outer, and this test:\n%s", len(trace), strings.Join(trace, "\n"))
	}
	if !strings.Contains(trace[0], "stackTraceInner") {
		t.Errorf("trace[0] = %q, want the stackTraceInner frame", trace[0])
	}
	assertOrdered(t, trace, "stackTraceInner", "stackTraceOuter")
	assertOrdered(t, trace, "stackTraceOuter", "TestCaptureStackTraceInnermostFirst")
	for _, f := range trace {
		if strings.HasPrefix(f, "runtime.") {
			t.Errorf("trace holds runtime frame %q", f)
		}
		if !strings.Contains(f, ".go:") {
			t.Errorf("frame %q does not end in file:line", f)
		}
	}
}

func TestCaptureStackTraceSkipsFrames(t *testing.T) {
	trace := stackTraceOuter(1)
	if frameIndex(trace, "stackTraceInner") >= 0 {
		t.Errorf("skip=1 still holds the stackTraceInner frame:\n%s", strings.Join(trace, "\n"))
	}
	if !strings.Contains(trace[0], "stackTraceOuter") {
		t.Errorf("trace[0] = %q, want the stackTraceOuter frame", trace[0])
	}
}

func TestCaptureStackTraceIsBounded(t *testing.T) {
	trace := stackTraceDeep(MaxStackTraceFrames * 3)
	if len(trace) != MaxStackTraceFrames {
		t.Fatalf("deep trace has %d frames, want exactly %d", len(trace), MaxStackTraceFrames)
	}
	// The bound keeps the innermost frames and drops the outermost.
	for i, f := range trace {
		if !strings.Contains(f, "stackTraceDeep") {
			t.Errorf("trace[%d] = %q, want a stackTraceDeep frame", i, f)
		}
	}
}

func TestStepFailureRecordsStackTrace(t *testing.T) {
	fake := &fakeLambda{}
	var stepErr *StepError
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := Step(ctx, "s", stepBodyFails, WithRetry(NoRetry()))
		if !errors.As(err, &stepErr) {
			t.Errorf("Step() error = %v, want *StepError", err)
		}
		return "", err
	})

	updates := updateBatch(t, fake)
	if len(updates) != 2 || updates[1].Error == nil {
		t.Fatalf("updates = %+v, want START then FAIL with an error", updates)
	}
	trace := updates[1].Error.StackTrace
	if len(trace) == 0 {
		t.Fatal("FAIL update has no StackTrace")
	}
	// The body returned normally, so the trace names the body first,
	// then the SDK frame that received the error, then the handler that
	// called Step.
	if !strings.Contains(trace[0], "stepBodyFails") {
		t.Errorf("trace[0] = %q, want the step body that failed:\n%s", trace[0], strings.Join(trace, "\n"))
	}
	assertOrdered(t, trace, "stepBodyFails", "runStepFunc")
	assertOrdered(t, trace, "runStepFunc", "TestStepFailureRecordsStackTrace")
	if len(trace) > MaxStackTraceFrames {
		t.Errorf("trace has %d frames, want at most %d", len(trace), MaxStackTraceFrames)
	}

	// The typed error exposes the same trace to the handler.
	if stepErr == nil {
		t.Fatal("handler did not observe a *StepError")
	}
	if strings.Join(stepErr.StackTrace, "\n") != strings.Join(trace, "\n") {
		t.Errorf("StepError.StackTrace = %v, want the recorded trace %v", stepErr.StackTrace, trace)
	}

	// The FAILED response carries the step's trace, not a trace taken
	// where the handler returned.
	got := failedResponse(t, resp)
	if strings.Join(got.StackTrace, "\n") != strings.Join(trace, "\n") {
		t.Errorf("response StackTrace = %v, want the step's trace %v", got.StackTrace, trace)
	}
}

func TestStepPanicStackTraceStartsAtPanicSite(t *testing.T) {
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s", stepBodyPanics, WithRetry(NoRetry()))
	})

	updates := updateBatch(t, fake)
	if len(updates) != 2 || updates[1].Error == nil {
		t.Fatalf("updates = %+v, want START then FAIL with an error", updates)
	}
	trace := updates[1].Error.StackTrace
	if len(trace) == 0 {
		t.Fatal("FAIL update has no StackTrace")
	}
	if !strings.Contains(trace[0], "stepBodyPanics") {
		t.Errorf("trace[0] = %q, want the panicking step body:\n%s", trace[0], strings.Join(trace, "\n"))
	}
	assertOrdered(t, trace, "stepBodyPanics", "runStepFunc")
	assertOrdered(t, trace, "runStepFunc", "TestStepPanicStackTraceStartsAtPanicSite")
}

func TestStepRetryUpdateCarriesStackTrace(t *testing.T) {
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s", stepBodyFails,
			WithRetry(MustNewRetryStrategy(RetryConfig{MaxAttempts: 3, InitialDelay: 1e9, Jitter: JitterNone})))
	})

	updates := updateBatch(t, fake)
	if len(updates) != 2 || updates[1].Action != OperationActionRetry || updates[1].Error == nil {
		t.Fatalf("updates = %+v, want START then RETRY with an error", updates)
	}
	if len(updates[1].Error.StackTrace) == 0 {
		t.Error("RETRY update has no StackTrace")
	}
}

func TestStepReplayKeepsRecordedStackTrace(t *testing.T) {
	recorded := []string{"origin frame", "caller frame"}
	fake := &fakeLambda{}
	var stepErr *StepError
	resp := invokeStep(t, fake,
		stepPayload(`""`, checkpointedStep("1", "FAILED", &wireStepDetails{
			Attempt: 1,
			Error:   &wireStepError{ErrorType: "Error", ErrorMessage: "step failed", StackTrace: recorded},
		})),
		func(ctx Context, _ string) (string, error) {
			_, err := Step(ctx, "s", stepBodyFails, WithRetry(NoRetry()))
			if !errors.As(err, &stepErr) {
				t.Errorf("Step() error = %v, want *StepError", err)
			}
			return "", err
		})

	if stepErr == nil {
		t.Fatal("handler did not observe a *StepError")
	}
	if strings.Join(stepErr.StackTrace, "\n") != strings.Join(recorded, "\n") {
		t.Errorf("replayed StepError.StackTrace = %v, want %v", stepErr.StackTrace, recorded)
	}
	got := failedResponse(t, resp)
	if strings.Join(got.StackTrace, "\n") != strings.Join(recorded, "\n") {
		t.Errorf("response StackTrace = %v, want the recorded trace %v", got.StackTrace, recorded)
	}
}

func TestHandlerPanicStackTraceStartsAtPanicSite(t *testing.T) {
	resp := invokeStep(t, &fakeLambda{}, stepPayload(`""`), handlerPanics)

	got := failedResponse(t, resp)
	if got.ErrorMessage != "durable: handler panicked: handler exploded" {
		t.Errorf("ErrorMessage = %q", got.ErrorMessage)
	}
	if len(got.StackTrace) == 0 {
		t.Fatal("response has no StackTrace")
	}
	if !strings.Contains(got.StackTrace[0], "explode") {
		t.Errorf("trace[0] = %q, want the panicking function:\n%s", got.StackTrace[0], strings.Join(got.StackTrace, "\n"))
	}
	assertOrdered(t, got.StackTrace, "explode", "handlerPanics")
}

func TestHandlerReturnedErrorRecordsStackTrace(t *testing.T) {
	resp := invokeStep(t, &fakeLambda{}, stepPayload(`""`), handlerFails)

	got := failedResponse(t, resp)
	if got.ErrorType != "Error" || got.ErrorMessage != "handler failed" {
		t.Errorf("error = %+v", got)
	}
	if len(got.StackTrace) == 0 {
		t.Fatal("response has no StackTrace")
	}
	if len(got.StackTrace) > MaxStackTraceFrames {
		t.Errorf("trace has %d frames, want at most %d", len(got.StackTrace), MaxStackTraceFrames)
	}
	// The handler returned normally, so the trace names the handler
	// first, then the SDK frame that received its error.
	if !strings.Contains(got.StackTrace[0], "handlerFails") {
		t.Errorf("trace[0] = %q, want the handler that failed:\n%s", got.StackTrace[0], strings.Join(got.StackTrace, "\n"))
	}
	assertOrdered(t, got.StackTrace, "handlerFails", "Invoke")
}

func TestChildContextFailureRecordsStackTrace(t *testing.T) {
	fake := &fakeLambda{}
	var childErr *ChildContextError
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := RunInChildContext(ctx, "child", childBodyFails)
		if !errors.As(err, &childErr) {
			t.Errorf("RunInChildContext() error = %v, want *ChildContextError", err)
		}
		return "", err
	})

	updates := updateBatch(t, fake)
	if len(updates) != 2 || updates[1].Action != OperationActionFail || updates[1].Error == nil {
		t.Fatalf("updates = %+v, want START then FAIL with an error", updates)
	}
	trace := updates[1].Error.StackTrace
	if len(trace) == 0 {
		t.Fatal("FAIL update has no StackTrace")
	}
	if !strings.Contains(trace[0], "childBodyFails") {
		t.Errorf("trace[0] = %q, want the child body that failed:\n%s", trace[0], strings.Join(trace, "\n"))
	}
	assertOrdered(t, trace, "childBodyFails", "RunInChildContext")
	assertOrdered(t, trace, "RunInChildContext", "TestChildContextFailureRecordsStackTrace")

	if childErr == nil {
		t.Fatal("handler did not observe a *ChildContextError")
	}
	assertSameTrace(t, "ChildContextError.StackTrace", childErr.StackTrace, trace)
	assertSameTrace(t, "response StackTrace", failedResponse(t, resp).StackTrace, trace)
}

func TestChildContextAsyncFailureRecordsStackTrace(t *testing.T) {
	fake := &fakeLambda{}
	var childErr *ChildContextError
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := Go(ctx, "child", childBodyFails).Result()
		if !errors.As(err, &childErr) {
			t.Errorf("Go().Result() error = %v, want *ChildContextError", err)
		}
		return "", err
	})

	updates := updateBatch(t, fake)
	if len(updates) != 2 || updates[1].Action != OperationActionFail || updates[1].Error == nil {
		t.Fatalf("updates = %+v, want START then FAIL with an error", updates)
	}
	trace := updates[1].Error.StackTrace
	if len(trace) == 0 {
		t.Fatal("FAIL update has no StackTrace")
	}
	if !strings.Contains(trace[0], "childBodyFails") {
		t.Errorf("trace[0] = %q, want the child body that failed:\n%s", trace[0], strings.Join(trace, "\n"))
	}
	if childErr == nil {
		t.Fatal("handler did not observe a *ChildContextError")
	}
	assertSameTrace(t, "ChildContextError.StackTrace", childErr.StackTrace, trace)
	assertSameTrace(t, "response StackTrace", failedResponse(t, resp).StackTrace, trace)
}

// waitForConditionFailTrace runs a WaitForCondition through the invocation
// path and returns the FAIL update's trace, the typed error the handler
// observed, and the raw response.
func waitForConditionFailTrace(t *testing.T, check func(StepContext, int) (int, error), strategy func(int, int) WaitDecision) ([]string, *WaitForConditionError, string) {
	t.Helper()
	fake := &fakeLambda{}
	var condErr *WaitForConditionError
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "cond", check, ConditionConfig[int]{WaitStrategy: strategy})
		if !errors.As(err, &condErr) {
			t.Errorf("WaitForCondition() error = %v, want *WaitForConditionError", err)
		}
		return "", err
	})
	var trace []string
	found := false
	for _, u := range updateBatch(t, fake) {
		if u.Action == OperationActionFail && u.Error != nil {
			found = true
			trace = u.Error.StackTrace
		}
	}
	if !found {
		t.Fatal("no FAIL update with an error")
	}
	if condErr == nil {
		t.Fatal("handler did not observe a *WaitForConditionError")
	}
	return trace, condErr, resp
}

func TestWaitForConditionCheckFailureRecordsStackTrace(t *testing.T) {
	trace, condErr, resp := waitForConditionFailTrace(t, checkFails, strategyStops)
	if len(trace) == 0 {
		t.Fatal("FAIL update has no StackTrace")
	}
	if !strings.Contains(trace[0], "checkFails") {
		t.Errorf("trace[0] = %q, want the check that failed:\n%s", trace[0], strings.Join(trace, "\n"))
	}
	assertOrdered(t, trace, "checkFails", "runCheckFunc")
	assertOrdered(t, trace, "runCheckFunc", "waitForConditionFailTrace")
	assertSameTrace(t, "WaitForConditionError.StackTrace", condErr.StackTrace, trace)
	assertSameTrace(t, "response StackTrace", failedResponse(t, resp).StackTrace, trace)
}

func TestWaitForConditionStrategyFailureRecordsStackTrace(t *testing.T) {
	trace, condErr, resp := waitForConditionFailTrace(t, checkSucceeds, strategyFails)
	if len(trace) == 0 {
		t.Fatal("FAIL update has no StackTrace")
	}
	if !strings.Contains(trace[0], "strategyFails") {
		t.Errorf("trace[0] = %q, want the strategy that failed:\n%s", trace[0], strings.Join(trace, "\n"))
	}
	assertOrdered(t, trace, "strategyFails", "waitForConditionFailTrace")
	assertSameTrace(t, "WaitForConditionError.StackTrace", condErr.StackTrace, trace)
	assertSameTrace(t, "response StackTrace", failedResponse(t, resp).StackTrace, trace)
}

func TestWithStackTracesFalseRecordsNone(t *testing.T) {
	fake := &fakeLambda{}
	var stepErr *StepError
	h := Wrap(func(ctx Context, _ string) (string, error) {
		_, err := Step(ctx, "s", stepBodyPanics, WithRetry(NoRetry()))
		if !errors.As(err, &stepErr) {
			t.Errorf("Step() error = %v, want *StepError", err)
		}
		return "", err
	}, withLambdaAPI(fake), WithStackTraces(false))
	raw, err := h(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}

	updates := updateBatch(t, fake)
	if len(updates) != 2 || updates[1].Error == nil {
		t.Fatalf("updates = %+v, want START then FAIL with an error", updates)
	}
	if updates[1].Error.StackTrace != nil {
		t.Errorf("FAIL update StackTrace = %v, want none", updates[1].Error.StackTrace)
	}
	if stepErr == nil || stepErr.StackTrace != nil {
		t.Errorf("StepError = %+v, want one with no StackTrace", stepErr)
	}
	got := failedResponse(t, string(raw))
	if got.StackTrace != nil {
		t.Errorf("response StackTrace = %v, want none", got.StackTrace)
	}
	if !strings.Contains(string(raw), `"ErrorMessage"`) || strings.Contains(string(raw), "StackTrace") {
		t.Errorf("response = %s, want an error object without a StackTrace key", raw)
	}

	// The handler path is disabled too.
	h = Wrap(handlerPanics, withLambdaAPI(&fakeLambda{}), WithStackTraces(false))
	raw, err = h(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	if got := failedResponse(t, string(raw)); got.StackTrace != nil {
		t.Errorf("handler panic response StackTrace = %v, want none", got.StackTrace)
	}
}

func TestErrorObjectFromErrorPrefersRecordedTrace(t *testing.T) {
	recorded := []string{"step frame"}
	err := &StepError{Name: "s", Attempts: 1, ErrorType: "Error", Message: "m", StackTrace: recorded, Err: errors.New("m")}
	got := errorObjectFromError(err, []string{"handler frame"})
	if strings.Join(got.StackTrace, "\n") != "step frame" {
		t.Errorf("StackTrace = %v, want the step's recorded trace", got.StackTrace)
	}

	// A wrapped SDK error keeps its trace through the wrapper.
	got = errorObjectFromError(errors.Join(errors.New("outer"), err), []string{"handler frame"})
	if strings.Join(got.StackTrace, "\n") != "step frame" {
		t.Errorf("wrapped StackTrace = %v, want the step's recorded trace", got.StackTrace)
	}

	// Without a recorded trace the handler's trace is used.
	got = errorObjectFromError(errors.New("plain"), []string{"handler frame"})
	if strings.Join(got.StackTrace, "\n") != "handler frame" {
		t.Errorf("plain StackTrace = %v, want the handler's trace", got.StackTrace)
	}
}

// mapItemFails is a Map item function that returns an error.
func mapItemFails(Context, int, int) (string, error) { return "", errors.New("item failed") }

func TestMapItemFailureRecordsStackTrace(t *testing.T) {
	fake := &fakeLambda{}
	var itemErr error
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		res, err := Map(ctx, "m", []int{1}, mapItemFails)
		if err := batchOnly(err); err != nil {
			return "", err
		}
		if len(res.Items) != 1 || res.Items[0].Status != BatchItemFailed {
			t.Fatalf("items = %+v, want one failed item", res.Items)
		}
		itemErr = res.Items[0].Err
		return "done", nil
	})

	var trace []string
	for _, u := range updateBatch(t, fake) {
		if u.Action == OperationActionFail && u.Error != nil && u.Type == OperationTypeContext {
			trace = u.Error.StackTrace
		}
	}
	if len(trace) == 0 {
		t.Fatal("item FAIL update has no StackTrace")
	}
	if !strings.Contains(trace[0], "mapItemFails") {
		t.Errorf("trace[0] = %q, want the item function that failed:\n%s", trace[0], strings.Join(trace, "\n"))
	}
	var childErr *ChildContextError
	if !errors.As(itemErr, &childErr) {
		t.Fatalf("item Err = %v, want *ChildContextError", itemErr)
	}
	assertSameTrace(t, "item ChildContextError.StackTrace", childErr.StackTrace, trace)
}

// TestFlatMapItemFailureKeepsStackTrace checks that a FLAT-nesting item
// failure keeps the trace of the item function. FLAT mode records the item
// function's own error as the item error, so the trace rides in a
// transparent wrapper: the error's type and message are unchanged, and the
// FAILED invocation response points at the item function, not at the
// handler that returned the error.
func TestFlatMapItemFailureKeepsStackTrace(t *testing.T) {
	fake := &fakeLambda{}
	var itemErr error
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		res, err := Map(ctx, "m", []int{1}, mapItemFails, WithNesting(NestingFlat))
		if err := batchOnly(err); err != nil {
			return "", err
		}
		if len(res.Items) != 1 || res.Items[0].Status != BatchItemFailed {
			t.Fatalf("items = %+v, want one failed item", res.Items)
		}
		itemErr = res.Items[0].Err
		return "", itemErr
	})

	// The wrapper is transparent for message and wire type.
	if itemErr.Error() != "item failed" {
		t.Errorf("item Err message = %q, want the raw error text", itemErr.Error())
	}
	if wireErrorType(itemErr) != "Error" {
		t.Errorf("wireErrorType = %q, want %q", wireErrorType(itemErr), "Error")
	}

	// The item error supplies the item function's trace.
	trace := suppliedStackTrace(itemErr)
	if len(trace) == 0 {
		t.Fatal("flat item error carries no StackTrace")
	}
	if !strings.Contains(trace[0], "mapItemFails") {
		t.Errorf("trace[0] = %q, want the item function that failed:\n%s", trace[0], strings.Join(trace, "\n"))
	}

	// The FAILED response carries the item's trace, not the handler's.
	got := failedResponse(t, resp)
	if len(got.StackTrace) == 0 {
		t.Fatal("response has no StackTrace")
	}
	if !strings.Contains(got.StackTrace[0], "mapItemFails") {
		t.Errorf("response trace[0] = %q, want the item function:\n%s", got.StackTrace[0], strings.Join(got.StackTrace, "\n"))
	}

	// The aggregate checkpoint payload persists the trace, and a replay
	// from that payload restores it on the reconstructed item error.
	var parentOp wireOperation
	for _, u := range updateBatch(t, fake) {
		if u.Action == OperationActionSucceed && aws.ToString(u.SubType) == "Map" {
			parentOp = wireOperation{
				Id:             aws.ToString(u.Id),
				Status:         "SUCCEEDED",
				Type:           "CONTEXT",
				SubType:        "Map",
				Name:           aws.ToString(u.Name),
				ContextDetails: &wireContextDetails{Result: aws.ToString(u.Payload)},
			}
		}
	}
	if parentOp.Id == "" {
		t.Fatal("no Map SUCCEED update recorded")
	}
	if !strings.Contains(parentOp.ContextDetails.Result, "stackTrace") {
		t.Errorf("aggregate payload carries no stackTrace: %s", parentOp.ContextDetails.Result)
	}

	var replayErr error
	invokeStep(t, &fakeLambda{}, stepPayload(`""`, parentOp), func(ctx Context, _ string) (string, error) {
		res, err := Map(ctx, "m", []int{1}, mapItemFails, WithNesting(NestingFlat))
		if err := batchOnly(err); err != nil {
			return "", err
		}
		replayErr = res.Items[0].Err
		return "done", nil
	})
	var childErr *ChildContextError
	if !errors.As(replayErr, &childErr) {
		t.Fatalf("replayed item Err = %v, want *ChildContextError", replayErr)
	}
	assertSameTrace(t, "replayed item StackTrace", childErr.StackTrace, trace)
}

// tracedError is a user error that supplies its own stack trace through
// the StackTrace method.
type tracedError struct{ trace []string }

func (e *tracedError) Error() string { return "traced failure" }

func (e *tracedError) StackTrace() []string { return e.trace }

// mapItemFailsWithOversizedTrace is a Map item function that returns an
// error supplying three times the frame bound.
func mapItemFailsWithOversizedTrace(Context, int, int) (string, error) {
	trace := make([]string, MaxStackTraceFrames*3)
	for i := range trace {
		trace[i] = "supplied frame"
	}
	return "", &tracedError{trace: trace}
}

// flatMapAggregateItems runs a FLAT-nesting Map with one failing item
// through the invocation path under opts and returns the item error the
// handler observed and the failed item as the Map's aggregate checkpoint
// payload recorded it. The error is the item error, not a failure of the
// helper.
func flatMapAggregateItems(t *testing.T, item func(Context, int, int) (string, error), opts ...HandlerOption) (batchCheckpointItem, error) {
	t.Helper()
	fake := &fakeLambda{}
	var itemErr error
	h := Wrap(func(ctx Context, _ string) (string, error) {
		res, err := Map(ctx, "m", []int{1}, item, WithNesting(NestingFlat))
		if err := batchOnly(err); err != nil {
			return "", err
		}
		if len(res.Items) != 1 || res.Items[0].Status != BatchItemFailed {
			t.Fatalf("items = %+v, want one failed item", res.Items)
		}
		itemErr = res.Items[0].Err
		return "done", nil
	}, append([]HandlerOption{withLambdaAPI(fake)}, opts...)...)
	if _, err := h(context.Background(), stepPayload(`""`)); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}

	for _, u := range updateBatch(t, fake) {
		if u.Action == OperationActionSucceed && aws.ToString(u.SubType) == "Map" {
			var payload batchCheckpointPayload
			if err := json.Unmarshal([]byte(aws.ToString(u.Payload)), &payload); err != nil {
				t.Fatalf("aggregate payload: %v", err)
			}
			if len(payload.Results) != 1 {
				t.Fatalf("aggregate payload items = %+v, want one", payload.Results)
			}
			return payload.Results[0], itemErr
		}
	}
	t.Fatal("no Map SUCCEED update recorded")
	return batchCheckpointItem{}, nil
}

// TestFlatMapItemSuppliedTraceIsBounded checks that a FLAT-nesting item
// error supplying more frames than the bound is recorded with at most
// [MaxStackTraceFrames] frames, in the aggregate payload and on the item
// error the handler sees.
func TestFlatMapItemSuppliedTraceIsBounded(t *testing.T) {
	cpItem, itemErr := flatMapAggregateItems(t, mapItemFailsWithOversizedTrace)

	if got := len(cpItem.StackTrace); got != MaxStackTraceFrames {
		t.Errorf("aggregate payload trace has %d frames, want exactly %d", got, MaxStackTraceFrames)
	}
	if got := len(itemErrorTrace(itemErr)); got != MaxStackTraceFrames {
		t.Errorf("item error trace has %d frames, want exactly %d", got, MaxStackTraceFrames)
	}
	// The item function's own error is still the item error.
	var traced *tracedError
	if !errors.As(itemErr, &traced) {
		t.Fatalf("item Err = %v, want *tracedError in the chain", itemErr)
	}
	if got := len(traced.trace); got != MaxStackTraceFrames*3 {
		t.Errorf("user error trace mutated to %d frames, want the original %d", got, MaxStackTraceFrames*3)
	}
}

// TestFlatMapItemWithStackTracesFalseRecordsNone checks that a
// FLAT-nesting item failure records no trace when capture is disabled,
// even when the item error supplies one itself.
func TestFlatMapItemWithStackTracesFalseRecordsNone(t *testing.T) {
	for name, item := range map[string]func(Context, int, int) (string, error){
		"plain error":    mapItemFails,
		"supplied trace": mapItemFailsWithOversizedTrace,
	} {
		t.Run(name, func(t *testing.T) {
			cpItem, itemErr := flatMapAggregateItems(t, item, WithStackTraces(false))
			if cpItem.StackTrace != nil {
				t.Errorf("aggregate payload StackTrace = %v, want none", cpItem.StackTrace)
			}
			if got := itemErrorTrace(itemErr); got != nil {
				t.Errorf("item error trace = %v, want none", got)
			}
			var wrapped *flatItemTraceError
			if errors.As(itemErr, &wrapped) {
				t.Errorf("item Err = %T, want no trace wrapper when capture is disabled", itemErr)
			}
		})
	}
}

// TestMapFailedItemReplayKeepsRecordedStackTrace replays a NORMAL-nesting
// Map whose failed item is already terminal in the checkpoint log, on both
// replay routes: mid-batch from the child's own FAILED checkpoint, and
// after batch completion from the parent's aggregate payload. The
// reconstructed item error must carry the trace the live run recorded.
func TestMapFailedItemReplayKeepsRecordedStackTrace(t *testing.T) {
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		if _, err := Map(ctx, "m", []int{1}, mapItemFails); batchOnly(err) != nil {
			return "", err
		}
		return "done", nil
	})

	// Rebuild the checkpointed state from the live updates.
	var parentID, parentName, childID, childName string
	var childErrObj *ErrorObject
	var parentPayload string
	for _, u := range updateBatch(t, fake) {
		switch aws.ToString(u.SubType) {
		case "Map":
			switch u.Action {
			case OperationActionStart:
				parentID = aws.ToString(u.Id)
				parentName = aws.ToString(u.Name)
			case OperationActionSucceed:
				parentPayload = aws.ToString(u.Payload)
			}
		case "MapIteration":
			if u.Action == OperationActionFail {
				childID = aws.ToString(u.Id)
				childName = aws.ToString(u.Name)
				childErrObj = u.Error
			}
		}
	}
	if childErrObj == nil || len(childErrObj.StackTrace) == 0 {
		t.Fatal("live run recorded no item FAIL trace")
	}
	liveTrace := childErrObj.StackTrace

	// Route B: mid-batch replay from the child's FAILED checkpoint.
	routeB := []wireOperation{
		{Id: parentID, Status: "STARTED", Type: "CONTEXT", SubType: "Map", Name: parentName},
		{Id: childID, ParentId: parentID, Status: "FAILED", Type: "CONTEXT", SubType: "MapIteration", Name: childName,
			ContextDetails: &wireContextDetails{Error: &wireFullError{
				ErrorType:    aws.ToString(childErrObj.ErrorType),
				ErrorMessage: aws.ToString(childErrObj.ErrorMessage),
				ErrorData:    aws.ToString(childErrObj.ErrorData),
				StackTrace:   liveTrace,
			}}},
	}
	assertReplayedItemTrace(t, routeB, liveTrace)

	// Route A: replay from the parent's aggregate payload.
	if !strings.Contains(parentPayload, "stackTrace") {
		t.Errorf("aggregate payload carries no stackTrace: %s", parentPayload)
	}
	routeA := []wireOperation{
		{Id: parentID, Status: "SUCCEEDED", Type: "CONTEXT", SubType: "Map", Name: parentName,
			ContextDetails: &wireContextDetails{Result: parentPayload}},
	}
	assertReplayedItemTrace(t, routeA, liveTrace)
}

// assertReplayedItemTrace replays a one-item Map against the checkpointed
// ops and checks the reconstructed item error carries the expected trace.
func assertReplayedItemTrace(t *testing.T, ops []wireOperation, want []string) {
	t.Helper()
	var itemErr error
	invokeStep(t, &fakeLambda{}, stepPayload(`""`, ops...), func(ctx Context, _ string) (string, error) {
		res, err := Map(ctx, "m", []int{1}, mapItemFails)
		if err := batchOnly(err); err != nil {
			return "", err
		}
		if len(res.Items) != 1 || res.Items[0].Status != BatchItemFailed {
			t.Fatalf("items = %+v, want one failed item", res.Items)
		}
		itemErr = res.Items[0].Err
		return "done", nil
	})
	var childErr *ChildContextError
	if !errors.As(itemErr, &childErr) {
		t.Fatalf("replayed item Err = %v, want *ChildContextError", itemErr)
	}
	assertSameTrace(t, "replayed item StackTrace", childErr.StackTrace, want)
}
