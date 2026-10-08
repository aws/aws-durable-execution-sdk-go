package durable

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// updatesOfType returns the updates of type opType that fake recorded, in
// checkpoint order.
func updatesOfType(t *testing.T, fake *fakeLambda, opType OperationType) []OperationUpdate {
	t.Helper()
	var out []OperationUpdate
	for _, u := range updateBatch(t, fake) {
		if u.Type == opType {
			out = append(out, u)
		}
	}
	return out
}

// assertUpdates checks that updates hold exactly the actions want, in
// order, and that each carries subType.
func assertUpdates(t *testing.T, updates []OperationUpdate, subType string, want ...OperationAction) {
	t.Helper()
	if len(updates) != len(want) {
		t.Fatalf("updates = %d %+v, want %v", len(updates), updates, want)
	}
	for i, action := range want {
		if updates[i].Action != action {
			t.Errorf("update %d Action = %q, want %q", i, updates[i].Action, action)
		}
		if got := aws.ToString(updates[i].SubType); got != subType {
			t.Errorf("%s update SubType = %q, want %q", updates[i].Action, got, subType)
		}
	}
}

// attemptRecorder records the SubType of every attempt hook call.
type attemptRecorder struct {
	mu       sync.Mutex
	subTypes []string
}

func (r *attemptRecorder) plugin() Plugin {
	record := func(s string) {
		r.mu.Lock()
		r.subTypes = append(r.subTypes, s)
		r.mu.Unlock()
	}
	return Plugin{
		OnOperationAttemptStart: func(_ context.Context, info AttemptHookInfo) { record("start:" + info.SubType) },
		OnOperationAttemptEnd:   func(_ context.Context, info AttemptEndHookInfo) { record("end:" + info.SubType) },
	}
}

func (r *attemptRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.subTypes
	r.subTypes = nil
	return out
}

// stepSubTypeHandlers returns a Step and a StepAsync handler that each run
// one step named "s" with opts and return its result.
func stepSubTypeHandlers(fn func(StepContext) (string, error), opts ...StepOption) map[string]Handler[string, string] {
	return map[string]Handler[string, string]{
		"Step": func(ctx Context, _ string) (string, error) {
			return Step(ctx, "s", fn, opts...)
		},
		"StepAsync": func(ctx Context, _ string) (string, error) {
			return StepAsync(ctx, "s", fn, opts...).Result(ctx)
		},
	}
}

func stepReturnsV(StepContext) (string, error) { return "v", nil }

// TestStepSubTypeRecordedInCheckpointAndPlugin asserts that the subtype a
// caller supplies, and the default without the option, is written to the
// step's START and SUCCEED checkpoints and reported in every operation and
// attempt hook live, and in the replayed operation hooks.
func TestStepSubTypeRecordedInCheckpointAndPlugin(t *testing.T) {
	cases := []struct {
		name    string
		opts    []StepOption
		subType string
	}{
		{"default", nil, OperationSubTypeStep},
		{"empty", []StepOption{WithStepSubType("")}, OperationSubTypeStep},
		{"custom", []StepOption{WithStepSubType("Payment")}, "Payment"},
	}
	for _, tc := range cases {
		opts := append([]StepOption{WithSemantics(AtMostOncePerRetry)}, tc.opts...)
		for variant, handler := range stepSubTypeHandlers(stepReturnsV, opts...) {
			t.Run(tc.name+"/"+variant, func(t *testing.T) {
				rec := &opRecorder{}
				attempts := &attemptRecorder{}
				fake := &fakeLambda{}
				h := Wrap(handler, WithPlugins(rec.plugin(), attempts.plugin()), withLambdaAPI(fake))

				resp, err := h(context.Background(), stepPayload(`"x"`))
				if err != nil {
					t.Fatal(err)
				}
				if want := `{"Status":"SUCCEEDED","Result":"\"v\""}`; string(resp) != want {
					t.Fatalf("response = %s, want %s", resp, want)
				}
				assertUpdates(t, updatesOfType(t, fake, OperationTypeStep), tc.subType,
					OperationActionStart, OperationActionSucceed)
				live := eventsForName(rec.take(), "s")
				assertSequence(t, live, "start:s:STARTED:false", "end:s:SUCCEEDED:false")
				for _, ev := range live {
					assertIdentity(t, ev, "1", "s", string(OperationTypeStep), tc.subType, "")
				}
				if got, want := strings.Join(attempts.take(), ","), "start:"+tc.subType+",end:"+tc.subType; got != want {
					t.Errorf("attempt hooks = %s, want %s", got, want)
				}

				// Replay from a checkpoint with the same subtype: the
				// stored result is returned and the replayed end reports
				// the recorded subtype.
				op := checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"v"`})
				op.Type, op.SubType, op.Name = "STEP", tc.subType, "s"
				replayFake := &fakeLambda{}
				h = Wrap(handler, WithPlugins(rec.plugin()), withLambdaAPI(replayFake))
				resp, err = h(context.Background(), stepPayload(`"x"`, op))
				if err != nil {
					t.Fatal(err)
				}
				if want := `{"Status":"SUCCEEDED","Result":"\"v\""}`; string(resp) != want {
					t.Fatalf("replay response = %s, want %s", resp, want)
				}
				if updates := updatesOfType(t, replayFake, OperationTypeStep); len(updates) != 0 {
					t.Fatalf("replay wrote step updates: %+v", updates)
				}
				replayed := eventsForName(rec.take(), "s")
				assertSequence(t, replayed, "start:s:SUCCEEDED:true", "end:s:SUCCEEDED:true")
				for _, ev := range replayed {
					assertIdentity(t, ev, "1", "s", string(OperationTypeStep), tc.subType, "")
				}
			})
		}
	}
}

// TestStepSubTypeRecordedOnRetryAndFailure asserts that a custom subtype
// is written to the RETRY of a failed attempt, and, in the next
// invocation, to the START and FAIL of the attempt that exhausts the
// strategy.
func TestStepSubTypeRecordedOnRetryAndFailure(t *testing.T) {
	strategy := func(a RetryAttempt) RetryDecision {
		return RetryDecision{Retry: a.Attempt < 2, Delay: DefaultRetryDelay}
	}
	handler := func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s", func(StepContext) (string, error) {
			return "", errors.New("boom")
		}, WithRetry(strategy), WithStepSubType("Payment"))
	}

	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"x"`), handler)
	if !strings.Contains(resp, `"Status":"PENDING"`) {
		t.Fatalf("first response = %s, want PENDING", resp)
	}
	assertUpdates(t, updatesOfType(t, fake, OperationTypeStep), "Payment",
		OperationActionStart, OperationActionRetry)

	op := checkpointedStep("1", "READY", &wireStepDetails{Attempt: 1})
	op.Type, op.SubType, op.Name = "STEP", "Payment", "s"
	fake = &fakeLambda{}
	resp = invokeStep(t, fake, stepPayload(`"x"`, op), handler)
	if !strings.Contains(resp, `"Status":"FAILED"`) {
		t.Fatalf("second response = %s, want FAILED", resp)
	}
	assertUpdates(t, updatesOfType(t, fake, OperationTypeStep), "Payment",
		OperationActionStart, OperationActionFail)
}

// TestStepSubTypeValidation asserts the value is checked before the
// operation claims an ID: a rejected value is returned by Step and settles
// the StepAsync future, runs no body, and writes no checkpoint. An
// accepted value reaches the checkpoint unchanged.
func TestStepSubTypeValidation(t *testing.T) {
	cases := subTypeValidationCases(OperationSubTypeStep)
	for _, tc := range cases {
		ran := false
		fn := func(StepContext) (string, error) { ran = true; return "v", nil }
		for variant, handler := range stepSubTypeHandlers(fn, WithStepSubType(tc.subType)) {
			t.Run(tc.name+"/"+variant, func(t *testing.T) {
				ran = false
				fake := &fakeLambda{}
				resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, event string) (string, error) {
					return checkConfigError(handler(ctx, event))(tc.wantErr, "WithStepSubType")
				})
				if tc.wantErr != "" {
					if want := `{"Status":"SUCCEEDED","Result":"\"rejected\""}`; resp != want {
						t.Fatalf("response = %s, want %s", resp, want)
					}
					if ran {
						t.Error("the step body ran for a rejected subtype")
					}
					if updates := updateBatch(t, fake); len(updates) != 0 {
						t.Fatalf("a checkpoint was written for the rejected subtype: %+v", updates)
					}
					return
				}
				if want := `{"Status":"SUCCEEDED","Result":"\"v\""}`; resp != want {
					t.Fatalf("response = %s, want %s", resp, want)
				}
				updates := updatesOfType(t, fake, OperationTypeStep)
				if len(updates) == 0 {
					t.Fatal("no step update was written")
				}
				for _, u := range updates {
					if got := aws.ToString(u.SubType); got != tc.wantSub {
						t.Errorf("%s update SubType = %q, want %q", u.Action, got, tc.wantSub)
					}
				}
			})
		}
	}
}

// TestStepSubTypeMismatchOnReplay asserts that a step whose checkpoint
// records one subtype fails with a NonDeterministicExecutionError when
// the code supplies another, in either direction between the default and
// a caller's value.
func TestStepSubTypeMismatchOnReplay(t *testing.T) {
	cases := []struct {
		name         string
		checkpointed string
		opts         []StepOption
		current      string
	}{
		{"custom replaced by default", "Payment", nil, OperationSubTypeStep},
		{"default replaced by custom", OperationSubTypeStep, []StepOption{WithStepSubType("Payment")}, "Payment"},
		{"custom replaced by custom", "Payment", []StepOption{WithStepSubType("Refund")}, "Refund"},
	}
	for _, tc := range cases {
		for variant, handler := range stepSubTypeHandlers(stepReturnsV, tc.opts...) {
			t.Run(tc.name+"/"+variant, func(t *testing.T) {
				op := checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"v"`})
				op.Type, op.SubType, op.Name = "STEP", tc.checkpointed, "s"
				fake := &fakeLambda{}
				resp := invokeStep(t, fake, stepPayload(`"x"`, op), func(ctx Context, event string) (string, error) {
					_, err := handler(ctx, event)
					return checkMismatch(err, tc.checkpointed, tc.current)
				})
				if want := `{"Status":"SUCCEEDED","Result":"\"detected\""}`; resp != want {
					t.Fatalf("response = %s, want %s", resp, want)
				}
				if updates := updatesOfType(t, fake, OperationTypeStep); len(updates) != 0 {
					t.Fatalf("a step checkpoint was written for the mismatched step: %+v", updates)
				}
			})
		}
	}
}

// TestCallbackSubTypeRecordedInCheckpointAndPlugin asserts that the
// subtype a caller supplies, and the default without the option, is
// written to the callback's START checkpoint and reported in its start
// hook, and that on replay a callback recorded with that subtype returns
// its result and reports the subtype in its end hook.
func TestCallbackSubTypeRecordedInCheckpointAndPlugin(t *testing.T) {
	cases := []struct {
		name    string
		opts    []CallbackOption
		subType string
	}{
		{"default", nil, OperationSubTypeCallback},
		{"empty", []CallbackOption{WithCallbackSubType("")}, OperationSubTypeCallback},
		{"custom", []CallbackOption{WithCallbackSubType("ManagerApproval")}, "ManagerApproval"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := func(ctx Context, _ string) (string, error) {
				cb, err := CreateCallback[string](ctx, "cb", tc.opts...)
				if err != nil {
					return "", err
				}
				return cb.Result(ctx)
			}
			rec := &opRecorder{}
			fake := &fakeLambda{}
			h := Wrap(handler, WithPlugins(rec.plugin()), withLambdaAPI(fake))
			resp, err := h(context.Background(), callbackPayload(`"x"`))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(resp), `"Status":"PENDING"`) {
				t.Fatalf("response = %s, want PENDING", resp)
			}
			assertUpdates(t, updatesOfType(t, fake, OperationTypeCallback), tc.subType, OperationActionStart)
			live := eventsForName(rec.take(), "cb")
			assertSequence(t, live, "start:cb:STARTED:false")
			assertIdentity(t, live[0], "1", "cb", string(OperationTypeCallback), tc.subType, "")

			op := checkpointedCallback("1", "SUCCEEDED", &wireCallbackDetails{CallbackId: "cb-1", Result: "approved"})
			op.Type, op.SubType, op.Name = "CALLBACK", tc.subType, "cb"
			resp, err = h(context.Background(), callbackPayload(`"x"`, op))
			if err != nil {
				t.Fatal(err)
			}
			if want := `{"Status":"SUCCEEDED","Result":"\"approved\""}`; string(resp) != want {
				t.Fatalf("replay response = %s, want %s", resp, want)
			}
			ends := eventsForName(rec.take(), "cb")
			assertSequence(t, ends, "end:cb:SUCCEEDED:true")
			assertIdentity(t, ends[0], "1", "cb", string(OperationTypeCallback), tc.subType, "")
		})
	}
}

// TestCallbackSubTypeValidation asserts the value is checked before the
// operation claims an ID, for CreateCallback and WaitForCallback: a
// rejected value is returned as an error and writes no checkpoint. An
// accepted value reaches the callback's START checkpoint unchanged.
func TestCallbackSubTypeValidation(t *testing.T) {
	variants := map[string]func(ctx Context, opt CallbackOption) error{
		"CreateCallback": func(ctx Context, opt CallbackOption) error {
			cb, err := CreateCallback[string](ctx, "cb", opt)
			if err != nil {
				return err
			}
			_, err = cb.Result(ctx)
			return err
		},
		"WaitForCallback": func(ctx Context, opt CallbackOption) error {
			_, err := WaitForCallback[string](ctx, "cb", func(StepContext, string) error { return nil }, opt)
			return err
		},
	}
	for _, tc := range subTypeValidationCases(OperationSubTypeCallback) {
		for variant, run := range variants {
			t.Run(tc.name+"/"+variant, func(t *testing.T) {
				fake := &fakeLambda{}
				resp := invokeStep(t, fake, callbackPayload(`"x"`), func(ctx Context, _ string) (string, error) {
					err := run(ctx, WithCallbackSubType(tc.subType))
					if tc.wantErr == "" {
						return "", err
					}
					return checkConfigError("", err)(tc.wantErr, "WithCallbackSubType")
				})
				if tc.wantErr != "" {
					if want := `{"Status":"SUCCEEDED","Result":"\"rejected\""}`; resp != want {
						t.Fatalf("response = %s, want %s", resp, want)
					}
					if updates := updateBatch(t, fake); len(updates) != 0 {
						t.Fatalf("a checkpoint was written for the rejected subtype: %+v", updates)
					}
					return
				}
				if !strings.Contains(resp, `"Status":"PENDING"`) {
					t.Fatalf("response = %s, want PENDING", resp)
				}
				assertUpdates(t, updatesOfType(t, fake, OperationTypeCallback), tc.wantSub, OperationActionStart)
			})
		}
	}
}

// TestWaitForCallbackSubTypeAppliesToItsCallback asserts that
// WithCallbackSubType passed to WaitForCallback labels the callback it
// creates, while the WaitForCallback context and its submitter step keep
// their own subtypes.
func TestWaitForCallbackSubTypeAppliesToItsCallback(t *testing.T) {
	rec := &opRecorder{}
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		return WaitForCallback[string](ctx, "approval", func(StepContext, string) error { return nil },
			WithCallbackSubType("ManagerApproval"))
	}, WithPlugins(rec.plugin()), withLambdaAPI(fake))
	resp, err := h(context.Background(), callbackPayload(`"x"`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(resp), `"Status":"PENDING"`) {
		t.Fatalf("response = %s, want PENDING", resp)
	}
	assertUpdates(t, updatesOfType(t, fake, OperationTypeCallback), "ManagerApproval", OperationActionStart)
	assertUpdates(t, updatesOfType(t, fake, OperationTypeContext), OperationSubTypeWaitForCallback, OperationActionStart)
	assertUpdates(t, updatesOfType(t, fake, OperationTypeStep), OperationSubTypeStep,
		OperationActionStart, OperationActionSucceed)
	found := false
	for _, ev := range rec.take() {
		if ev.info.Type == string(OperationTypeCallback) {
			found = true
			if ev.info.SubType != "ManagerApproval" {
				t.Errorf("%s callback SubType = %q, want ManagerApproval", ev.hook, ev.info.SubType)
			}
		}
	}
	if !found {
		t.Error("no callback hook was dispatched")
	}
}

// TestCallbackSubTypeMismatchOnReplay asserts that a callback whose
// checkpoint records one subtype fails with a
// NonDeterministicExecutionError when the code supplies another.
func TestCallbackSubTypeMismatchOnReplay(t *testing.T) {
	cases := []struct {
		name         string
		checkpointed string
		opts         []CallbackOption
		current      string
	}{
		{"custom replaced by default", "ManagerApproval", nil, OperationSubTypeCallback},
		{"default replaced by custom", OperationSubTypeCallback, []CallbackOption{WithCallbackSubType("ManagerApproval")}, "ManagerApproval"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			op := checkpointedCallback("1", "SUCCEEDED", &wireCallbackDetails{CallbackId: "cb-1", Result: "approved"})
			op.Type, op.SubType, op.Name = "CALLBACK", tc.checkpointed, "cb"
			fake := &fakeLambda{}
			resp := invokeStep(t, fake, callbackPayload(`"x"`, op), func(ctx Context, _ string) (string, error) {
				_, err := CreateCallback[string](ctx, "cb", tc.opts...)
				return checkMismatch(err, tc.checkpointed, tc.current)
			})
			if want := `{"Status":"SUCCEEDED","Result":"\"detected\""}`; resp != want {
				t.Fatalf("response = %s, want %s", resp, want)
			}
		})
	}
}

// subTypeValidationCase is one subtype option value and its outcome.
type subTypeValidationCase struct {
	name    string
	subType string
	wantErr string // substring of the error; "" when the value is accepted
	wantSub string // subtype recorded when accepted
}

// subTypeValidationCases returns the validation cases for an operation
// whose default subtype is def: def is accepted, every other SDK subtype
// is reserved.
func subTypeValidationCases(def string) []subTypeValidationCase {
	cases := []subTypeValidationCase{
		{"empty selects default", "", "", def},
		{"default constant", def, "", def},
		{"one character", "a", "", "a"},
		{"all accepted characters", "Az09-_", "", "Az09-_"},
		{"maximum length", strings.Repeat("s", 32), "", strings.Repeat("s", 32)},
		{"too long", strings.Repeat("s", 33), "is 33 characters, the limit is 32", ""},
		{"space", "Pay ment", `has character ' ' at index 3`, ""},
		{"dot", "pay.ment", `has character '.' at index 3`, ""},
		{"non-ASCII", "Payé", "has character", ""},
	}
	for _, reserved := range []string{
		OperationSubTypeStep,
		OperationSubTypeWait,
		OperationSubTypeCallback,
		OperationSubTypeChainedInvoke,
		OperationSubTypeRunInChildContext,
		OperationSubTypeWaitForCallback,
		OperationSubTypeWaitForCondition,
		OperationSubTypeMap,
		OperationSubTypeMapIteration,
		OperationSubTypeParallel,
		OperationSubTypeParallelBranch,
	} {
		if reserved == def {
			continue
		}
		cases = append(cases, subTypeValidationCase{
			"reserved " + reserved, reserved,
			fmt.Sprintf("value %q is the subtype of an SDK operation and is reserved", reserved), "",
		})
	}
	return cases
}

// checkConfigError returns a function that reports "rejected" when err is
// a configuration error naming option and containing wantErr, the
// operation's own result when wantErr is empty, and an error otherwise.
func checkConfigError(out string, err error) func(wantErr, option string) (string, error) {
	return func(wantErr, option string) (string, error) {
		if wantErr == "" {
			return out, err
		}
		if err == nil {
			return "", errors.New("expected a configuration error")
		}
		if !strings.Contains(err.Error(), option) || !strings.Contains(err.Error(), wantErr) {
			return "", fmt.Errorf("error %q does not contain %q and %q", err, option, wantErr)
		}
		return "rejected", nil
	}
}

// checkMismatch reports "detected" when err is a
// NonDeterministicExecutionError with the given recorded and current
// subtypes.
func checkMismatch(err error, recorded, current string) (string, error) {
	var ndErr *NonDeterministicExecutionError
	if !errors.As(err, &ndErr) {
		return "", fmt.Errorf("err = %v, want a NonDeterministicExecutionError", err)
	}
	if ndErr.RecordedSubType != recorded || ndErr.CurrentSubType != current {
		return "", fmt.Errorf("subtypes = (recorded %q, current %q), want (%q, %q)",
			ndErr.RecordedSubType, ndErr.CurrentSubType, recorded, current)
	}
	return "detected", nil
}
