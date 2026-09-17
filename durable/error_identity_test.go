package durable_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// paymentDeclinedError is a user-defined error type returned from step and
// child-context bodies in these tests.
type paymentDeclinedError struct{ Card string }

func (e *paymentDeclinedError) Error() string { return "card " + e.Card + " declined" }

// errorObservation is what a handler records about an operation error on
// one invocation. The tests compare the observation from the first
// invocation with the one from replay.
type errorObservation struct {
	MatchesUserType bool   `json:"matchesUserType"`
	MatchesOpError  bool   `json:"matchesOpError"`
	ErrorType       string `json:"errorType"`
	Message         string `json:"message"`
	ErrorData       string `json:"errorData"`
	ErrorString     string `json:"errorString"`
}

func observe(err error) errorObservation {
	var declined *paymentDeclinedError
	var opErr *durable.OperationError
	obs := errorObservation{
		MatchesUserType: errors.As(err, &declined),
		MatchesOpError:  errors.As(err, &opErr),
		ErrorString:     err.Error(),
	}
	if opErr != nil {
		obs.ErrorType = opErr.ErrorType
		obs.Message = opErr.Message
		obs.ErrorData = opErr.ErrorData
	}
	return obs
}

// runTwice invokes the handler until it suspends on the wait that follows
// the observed failure, then completes the wait and invokes again so the
// second invocation replays the failure. It returns the observations from
// both invocations, which the handler stores in observations by invocation
// index.
func runTwice[I, O any](t *testing.T, handler durable.Handler[I, O], event I, observations *[]errorObservation) {
	t.Helper()
	runner := durabletest.NewLocalRunner(handler)
	first := runner.Run(t, event)
	if first.Status != durabletest.Pending {
		t.Fatalf("first invocation status = %s, want PENDING (suspended on wait); error = %+v", first.Status, first.Error)
	}
	if !runner.CompletePendingTimers() {
		t.Fatal("no pending wait to complete")
	}
	second := runner.Run(t, event)
	if second.Status != durabletest.Succeeded {
		t.Fatalf("second invocation status = %s, want SUCCEEDED; error = %+v", second.Status, second.Error)
	}
	if len(*observations) != 2 {
		t.Fatalf("recorded %d observations, want 2", len(*observations))
	}
}

func assertSameObservation(t *testing.T, obs []errorObservation) {
	t.Helper()
	if obs[0] != obs[1] {
		live, _ := json.MarshalIndent(obs[0], "", "  ")
		replay, _ := json.MarshalIndent(obs[1], "", "  ")
		t.Fatalf("observation differs between first invocation and replay\nfirst:\n%s\nreplay:\n%s", live, replay)
	}
}

// TestStepErrorIdentitySameOnLiveAndReplay covers the first acceptance
// criterion: for a step failing with a user-defined error type, errors.As
// against that type returns the same result on the first invocation and on
// replay, and ErrorType equals the user type's name on both.
func TestStepErrorIdentitySameOnLiveAndReplay(t *testing.T) {
	var observations []errorObservation
	handler := func(ctx durable.Context, _ string) (string, error) {
		_, err := durable.Step(ctx, "charge", func(durable.StepContext) (string, error) {
			return "", &paymentDeclinedError{Card: "4242"}
		}, durable.WithRetry(durable.NoRetry()))
		if err == nil {
			return "", errors.New("step succeeded, want failure")
		}
		var stepErr *durable.StepError
		if !errors.As(err, &stepErr) {
			return "", fmt.Errorf("error = %T, want *StepError", err)
		}
		observations = append(observations, observe(err))
		if err := durable.Wait(ctx, "pause", time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}
	runTwice(t, handler, "", &observations)
	assertSameObservation(t, observations)

	got := observations[0]
	if got.MatchesUserType {
		t.Error("errors.As against the user type = true, want false: the cause is a stand-in")
	}
	if !got.MatchesOpError {
		t.Error("errors.As against *OperationError = false, want true")
	}
	if got.ErrorType != "paymentDeclinedError" {
		t.Errorf("ErrorType = %q, want %q", got.ErrorType, "paymentDeclinedError")
	}
	if got.Message != "card 4242 declined" {
		t.Errorf("Message = %q, want %q", got.Message, "card 4242 declined")
	}
}

// TestCallbackTimedOutSentinelOnLiveAndReplay covers the acceptance
// criterion that errors.Is(err, ErrCallbackTimedOut) is true on both the
// invocation that first observes the timeout and on replay, and that the
// error is a CallbackTimeoutError matching *CallbackError.
func TestCallbackTimedOutSentinelOnLiveAndReplay(t *testing.T) {
	type obs struct {
		IsTimedOut  bool
		IsTimeout   bool
		IsCallback  bool
		IsOperation bool
		Heartbeat   bool
	}
	var observations []obs
	handler := func(ctx durable.Context, _ string) (string, error) {
		cb, err := durable.CreateCallback[string](ctx, "approval", durable.WithCallbackTimeout(time.Minute))
		if err != nil {
			return "", err
		}
		_, err = cb.Result()
		if err == nil {
			return "", errors.New("callback succeeded, want timeout")
		}
		var cbErr *durable.CallbackError
		if !errors.As(err, &cbErr) {
			// Not a callback failure: a suspension signal on the first
			// invocation. Propagate it unchanged.
			return "", err
		}
		var timeoutErr *durable.CallbackTimeoutError
		var opErr *durable.OperationError
		o := obs{
			IsTimedOut:  errors.Is(err, durable.ErrCallbackTimedOut),
			IsTimeout:   errors.As(err, &timeoutErr),
			IsCallback:  true,
			IsOperation: errors.As(err, &opErr),
		}
		if timeoutErr != nil {
			o.Heartbeat = timeoutErr.Heartbeat
		}
		observations = append(observations, o)
		if err := durable.Wait(ctx, "pause", time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}

	runner := durabletest.NewLocalRunner(handler)
	if r := runner.Run(t, ""); r.Status != durabletest.Pending {
		t.Fatalf("first invocation status = %s, want PENDING", r.Status)
	}
	open := runner.OpenCallbacks()
	if len(open) != 1 {
		t.Fatalf("open callbacks = %d, want 1", len(open))
	}
	if err := runner.TimeoutCallback(open[0].CallbackID); err != nil {
		t.Fatal(err)
	}
	if r := runner.Run(t, ""); r.Status != durabletest.Pending {
		t.Fatalf("second invocation status = %s, want PENDING (suspended on wait)", r.Status)
	}
	runner.CompletePendingTimers()
	if r := runner.Run(t, ""); r.Status != durabletest.Succeeded {
		t.Fatalf("third invocation status = %s, want SUCCEEDED; error = %+v", r.Status, r.Error)
	}
	if len(observations) != 2 {
		t.Fatalf("recorded %d observations, want 2", len(observations))
	}
	want := obs{IsTimedOut: true, IsTimeout: true, IsCallback: true, IsOperation: true}
	for i, o := range observations {
		if o != want {
			t.Errorf("observation[%d] = %+v, want %+v", i, o, want)
		}
	}
}

// TestErrorDataRoundTripThroughStepAndChildContext covers the ErrorData
// acceptance criterion: data attached with WithErrorData is readable on the
// StepError and, through two child-context boundaries, on the outermost
// ChildContextError, identically on the first invocation and on replay.
func TestErrorDataRoundTripThroughStepAndChildContext(t *testing.T) {
	const data = `{"reason":"operator-cancelled"}`
	var stepObs, childObs []errorObservation
	handler := func(ctx durable.Context, _ string) (string, error) {
		_, err := durable.Step(ctx, "flagged", func(durable.StepContext) (string, error) {
			return "", durable.WithErrorData(&paymentDeclinedError{Card: "1111"}, data)
		}, durable.WithRetry(durable.NoRetry()))
		if err == nil {
			return "", errors.New("step succeeded, want failure")
		}
		stepObs = append(stepObs, observe(err))

		_, err = durable.RunInChildContext(ctx, "outer", func(outer durable.Context) (string, error) {
			return durable.RunInChildContext(outer, "inner", func(inner durable.Context) (string, error) {
				return durable.Step(inner, "deep", func(durable.StepContext) (string, error) {
					return "", durable.WithErrorData(errors.New("deep failure"), data)
				}, durable.WithRetry(durable.NoRetry()))
			})
		})
		if err == nil {
			return "", errors.New("child succeeded, want failure")
		}
		var childErr *durable.ChildContextError
		if !errors.As(err, &childErr) {
			return "", fmt.Errorf("error = %T, want *ChildContextError", err)
		}
		childObs = append(childObs, observe(err))

		if err := durable.Wait(ctx, "pause", time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}

	runner := durabletest.NewLocalRunner(handler)
	if r := runner.Run(t, ""); r.Status != durabletest.Pending {
		t.Fatalf("first invocation status = %s, want PENDING; error = %+v", r.Status, r.Error)
	}
	runner.CompletePendingTimers()
	if r := runner.Run(t, ""); r.Status != durabletest.Succeeded {
		t.Fatalf("second invocation status = %s, want SUCCEEDED; error = %+v", r.Status, r.Error)
	}
	if len(stepObs) != 2 || len(childObs) != 2 {
		t.Fatalf("recorded %d step and %d child observations, want 2 each", len(stepObs), len(childObs))
	}
	assertSameObservation(t, stepObs)
	assertSameObservation(t, childObs)
	if stepObs[0].ErrorData != data {
		t.Errorf("StepError.ErrorData = %q, want %q", stepObs[0].ErrorData, data)
	}
	if stepObs[0].ErrorType != "paymentDeclinedError" {
		t.Errorf("StepError.ErrorType = %q, want %q (WithErrorData must not change the type)", stepObs[0].ErrorType, "paymentDeclinedError")
	}
	if childObs[0].ErrorData != data {
		t.Errorf("outer ChildContextError.ErrorData = %q, want %q", childObs[0].ErrorData, data)
	}
	if childObs[0].ErrorType != "ChildContextError" {
		t.Errorf("outer ChildContextError.ErrorType = %q, want %q", childObs[0].ErrorType, "ChildContextError")
	}
}

// TestWaitForCallbackSubmitterErrorOnLiveAndReplay verifies that a failing
// submitter step surfaces as a CallbackSubmitterError carrying the
// submitter's error type, on both the first invocation and on replay.
func TestWaitForCallbackSubmitterErrorOnLiveAndReplay(t *testing.T) {
	type obs struct {
		IsSubmitter bool
		IsCallback  bool
		ErrorType   string
		Message     string
	}
	var observations []obs
	handler := func(ctx durable.Context, _ string) (string, error) {
		_, err := durable.WaitForCallback[string](ctx, "notify",
			func(durable.StepContext, string) error {
				return &paymentDeclinedError{Card: "0000"}
			},
			durable.WithSubmitterRetry(durable.NoRetry()))
		if err == nil {
			return "", errors.New("wait-for-callback succeeded, want submitter failure")
		}
		var cbErr *durable.CallbackError
		if !errors.As(err, &cbErr) {
			return "", err
		}
		var subErr *durable.CallbackSubmitterError
		o := obs{IsSubmitter: errors.As(err, &subErr), IsCallback: true}
		if cbErr != nil {
			o.ErrorType = cbErr.ErrorType
			o.Message = cbErr.Message
		}
		observations = append(observations, o)
		if err := durable.Wait(ctx, "pause", time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}

	runner := durabletest.NewLocalRunner(handler)
	if r := runner.Run(t, ""); r.Status != durabletest.Pending {
		t.Fatalf("first invocation status = %s, want PENDING; error = %+v", r.Status, r.Error)
	}
	runner.CompletePendingTimers()
	if r := runner.Run(t, ""); r.Status != durabletest.Succeeded {
		t.Fatalf("second invocation status = %s, want SUCCEEDED; error = %+v", r.Status, r.Error)
	}
	if len(observations) != 2 {
		t.Fatalf("recorded %d observations, want 2", len(observations))
	}
	want := obs{IsSubmitter: true, IsCallback: true, ErrorType: "paymentDeclinedError", Message: "card 0000 declined"}
	for i, o := range observations {
		if o != want {
			t.Errorf("observation[%d] = %+v, want %+v", i, o, want)
		}
	}
}
