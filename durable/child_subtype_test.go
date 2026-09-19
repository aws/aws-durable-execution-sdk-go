package durable

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// childSubTypeHandlers returns the blocking and asynchronous handlers under
// test. Each runs one child context named "child" with opts, and a step
// inside it, and returns the child's result.
func childSubTypeHandlers(opts ...ChildOption) map[string]Handler[string, string] {
	body := func(c Context) (string, error) {
		return Step(c, "inner", func(StepContext) (string, error) { return "v", nil })
	}
	return map[string]Handler[string, string]{
		"RunInChildContext": func(ctx Context, _ string) (string, error) {
			return RunInChildContext(ctx, "child", body, opts...)
		},
		"Go": func(ctx Context, _ string) (string, error) {
			return Go(ctx, "child", body, opts...).Result()
		},
	}
}

// contextUpdates returns the context-operation updates fake recorded, in
// checkpoint order.
func contextUpdates(t *testing.T, fake *fakeLambda) []OperationUpdate {
	t.Helper()
	var out []OperationUpdate
	for _, u := range updateBatch(t, fake) {
		if u.Type == OperationTypeContext {
			out = append(out, u)
		}
	}
	return out
}

// assertContextSubTypes checks that the child context's START and SUCCEED
// checkpoints, and no other context checkpoint, were written with subType.
func assertContextSubTypes(t *testing.T, fake *fakeLambda, subType string) {
	t.Helper()
	updates := contextUpdates(t, fake)
	if len(updates) != 2 {
		t.Fatalf("context updates = %d, want START and SUCCEED", len(updates))
	}
	for i, action := range []OperationAction{OperationActionStart, OperationActionSucceed} {
		if updates[i].Action != action {
			t.Errorf("context update %d Action = %q, want %q", i, updates[i].Action, action)
		}
		if got := aws.ToString(updates[i].SubType); got != subType {
			t.Errorf("context update %d SubType = %q, want %q", i, got, subType)
		}
	}
}

// TestChildSubTypeRecordedInCheckpointAndPlugin asserts that the subtype a
// caller supplies is written to the child's START and SUCCEED checkpoints
// and reported in the start and end plugin notifications, live and on
// replay, for RunInChildContext and Go. Without the option the default
// subtype is recorded and reported in the same places.
func TestChildSubTypeRecordedInCheckpointAndPlugin(t *testing.T) {
	cases := []struct {
		name    string
		opts    []ChildOption
		subType string
	}{
		{"default", nil, OperationSubTypeRunInChildContext},
		{"custom", []ChildOption{WithChildSubType("OrderSaga")}, "OrderSaga"},
	}
	for _, tc := range cases {
		for variant, handler := range childSubTypeHandlers(tc.opts...) {
			t.Run(tc.name+"/"+variant, func(t *testing.T) {
				rec := &opRecorder{}
				fake := &fakeLambda{}
				h := Wrap(handler, WithPlugins(rec.plugin()), withLambdaAPI(fake))

				resp, err := h(context.Background(), childPayload(`"x"`))
				if err != nil {
					t.Fatal(err)
				}
				if want := `{"Status":"SUCCEEDED","Result":"\"v\""}`; string(resp) != want {
					t.Fatalf("response = %s, want %s", resp, want)
				}
				assertContextSubTypes(t, fake, tc.subType)

				live := eventsForName(rec.take(), "child")
				assertSequence(t, live, "start:child:STARTED:false", "end:child:SUCCEEDED:false")
				for _, ev := range live {
					assertIdentity(t, ev, "1", "child", string(OperationTypeContext), tc.subType, "")
				}

				// Replay from the checkpoint the first run wrote: the
				// replayed end reports the recorded subtype.
				replay := childPayload(`"x"`,
					contextOp("1", "", tc.subType, "child", "SUCCEEDED", &wireContextDetails{Result: `"v"`}))
				resp, err = h(context.Background(), replay)
				if err != nil {
					t.Fatal(err)
				}
				if want := `{"Status":"SUCCEEDED","Result":"\"v\""}`; string(resp) != want {
					t.Fatalf("replay response = %s, want %s", resp, want)
				}
				replayed := rec.take()
				assertSequence(t, replayed, "end:child:SUCCEEDED:true")
				assertIdentity(t, replayed[0], "1", "child", string(OperationTypeContext), tc.subType, "")
			})
		}
	}
}

// TestChildSubTypeRecordedOnFailure asserts a failed child's FAIL
// checkpoint and end notification carry the caller's subtype.
func TestChildSubTypeRecordedOnFailure(t *testing.T) {
	boom := errors.New("boom")
	rec := &opRecorder{}
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		_, err := RunInChildContext(ctx, "child", func(Context) (string, error) {
			return "", boom
		}, WithChildSubType("OrderSaga"))
		var childErr *ChildContextError
		if !errors.As(err, &childErr) {
			return "", fmt.Errorf("err = %v, want a ChildContextError", err)
		}
		return "handled", nil
	}, WithPlugins(rec.plugin()), withLambdaAPI(fake))

	resp, err := h(context.Background(), childPayload(`"x"`))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"Status":"SUCCEEDED","Result":"\"handled\""}`; string(resp) != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	updates := contextUpdates(t, fake)
	if len(updates) != 2 || updates[1].Action != OperationActionFail {
		t.Fatalf("context updates = %+v, want START then FAIL", updates)
	}
	for _, u := range updates {
		if got := aws.ToString(u.SubType); got != "OrderSaga" {
			t.Errorf("%s update SubType = %q, want OrderSaga", u.Action, got)
		}
	}
	evs := eventsForName(rec.take(), "child")
	assertSequence(t, evs, "start:child:STARTED:false", "end:child:FAILED:false")
	for _, ev := range evs {
		assertIdentity(t, ev, "1", "child", string(OperationTypeContext), "OrderSaga", "")
	}
}

// TestChildSubTypeValidation asserts the value is checked before the
// operation claims an ID: a rejected value is returned as an error by
// RunInChildContext and settles the Go future, and writes no checkpoint.
// An accepted value reaches the checkpoint unchanged.
func TestChildSubTypeValidation(t *testing.T) {
	cases := []struct {
		name    string
		subType string
		wantErr string // substring of the error; "" when the value is accepted
		wantSub string // subtype recorded when accepted
	}{
		{"empty selects default", "", "", OperationSubTypeRunInChildContext},
		{"one character", "a", "", "a"},
		{"all accepted characters", "Az09-_", "", "Az09-_"},
		{"maximum length", strings.Repeat("s", 32), "", strings.Repeat("s", 32)},
		{"default constant", OperationSubTypeRunInChildContext, "", OperationSubTypeRunInChildContext},
		{"too long", strings.Repeat("s", 33), "is 33 characters, the limit is 32", ""},
		{"space", "Order Saga", `has character ' ' at index 5`, ""},
		{"dot", "order.saga", `has character '.' at index 5`, ""},
		{"non-ASCII", "Sagé", "has character", ""},
		{"control character", "Saga\n", `has character '\n' at index 4`, ""},
	}
	for _, reserved := range []string{
		OperationSubTypeStep,
		OperationSubTypeWait,
		OperationSubTypeCallback,
		OperationSubTypeChainedInvoke,
		OperationSubTypeWaitForCallback,
		OperationSubTypeWaitForCondition,
		OperationSubTypeMap,
		OperationSubTypeMapIteration,
		OperationSubTypeParallel,
		OperationSubTypeParallelBranch,
	} {
		cases = append(cases, struct {
			name    string
			subType string
			wantErr string
			wantSub string
		}{"reserved " + reserved, reserved, fmt.Sprintf("value %q is the subtype of an SDK operation and is reserved", reserved), ""})
	}

	for _, tc := range cases {
		for variant, handler := range childSubTypeHandlers(WithChildSubType(tc.subType)) {
			t.Run(tc.name+"/"+variant, func(t *testing.T) {
				fake := &fakeLambda{}
				resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, event string) (string, error) {
					out, err := handler(ctx, event)
					if tc.wantErr == "" {
						return out, err
					}
					if err == nil {
						return "", fmt.Errorf("expected a configuration error")
					}
					if !strings.Contains(err.Error(), "WithChildSubType") || !strings.Contains(err.Error(), tc.wantErr) {
						return "", fmt.Errorf("error %q does not contain %q", err, tc.wantErr)
					}
					return "rejected", nil
				})
				if tc.wantErr != "" {
					if want := `{"Status":"SUCCEEDED","Result":"\"rejected\""}`; resp != want {
						t.Fatalf("response = %s, want %s", resp, want)
					}
					if updates := contextUpdates(t, fake); len(updates) != 0 {
						t.Fatalf("a context checkpoint was written for the rejected subtype: %+v", updates)
					}
					return
				}
				if want := `{"Status":"SUCCEEDED","Result":"\"v\""}`; resp != want {
					t.Fatalf("response = %s, want %s", resp, want)
				}
				assertContextSubTypes(t, fake, tc.wantSub)
			})
		}
	}
}

// TestChildSubTypeMismatchOnReplay asserts that a child context whose
// checkpoint records one subtype fails with a NonDeterministicReplayError
// when the code supplies another, in either direction between the default
// and a caller's value, for RunInChildContext and Go.
func TestChildSubTypeMismatchOnReplay(t *testing.T) {
	cases := []struct {
		name         string
		checkpointed string
		opts         []ChildOption
		expected     string
	}{
		{"custom replaced by default", "OrderSaga", nil, OperationSubTypeRunInChildContext},
		{"default replaced by custom", OperationSubTypeRunInChildContext, []ChildOption{WithChildSubType("OrderSaga")}, "OrderSaga"},
		{"custom replaced by custom", "OrderSaga", []ChildOption{WithChildSubType("Refund")}, "Refund"},
	}
	for _, tc := range cases {
		for variant, handler := range childSubTypeHandlers(tc.opts...) {
			t.Run(tc.name+"/"+variant, func(t *testing.T) {
				fake := &fakeLambda{}
				payload := childPayload(`"x"`,
					contextOp("1", "", tc.checkpointed, "child", "SUCCEEDED", &wireContextDetails{Result: `"v"`}))
				resp := invokeStep(t, fake, payload, func(ctx Context, event string) (string, error) {
					_, err := handler(ctx, event)
					var ndErr *NonDeterministicReplayError
					if !errors.As(err, &ndErr) {
						return "", fmt.Errorf("err = %v, want a NonDeterministicReplayError", err)
					}
					if ndErr.ExpectedSubType != tc.expected || ndErr.ActualSubType != tc.checkpointed {
						return "", fmt.Errorf("subtypes = (expected %q, actual %q), want (%q, %q)",
							ndErr.ExpectedSubType, ndErr.ActualSubType, tc.expected, tc.checkpointed)
					}
					return "detected", nil
				})
				if want := `{"Status":"SUCCEEDED","Result":"\"detected\""}`; resp != want {
					t.Fatalf("response = %s, want %s", resp, want)
				}
				if updates := contextUpdates(t, fake); len(updates) != 0 {
					t.Fatalf("a context checkpoint was written for the mismatched child: %+v", updates)
				}
			})
		}
	}
}
