package durable

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

func TestInvokeStartsAndSuspends(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"order-1"`), func(ctx Context, event string) (string, error) {
		return Invoke[string](ctx, "charge", "arn:aws:lambda:us-west-2:123:function:target:1", event)
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	updates := updateBatch(t, fake)
	if len(updates) != 1 {
		t.Fatalf("received %d updates, want 1 (START)", len(updates))
	}
	u := updates[0]
	if got, want := aws.ToString(u.Id), hashID("1"); got != want {
		t.Errorf("update Id = %q, want %q", got, want)
	}
	if u.Type != types.OperationTypeChainedInvoke {
		t.Errorf("update Type = %q, want CHAINED_INVOKE", u.Type)
	}
	if got := aws.ToString(u.SubType); got != "ChainedInvoke" {
		t.Errorf("update SubType = %q, want ChainedInvoke", got)
	}
	if u.Action != types.OperationActionStart {
		t.Errorf("update Action = %q, want START", u.Action)
	}
	if got := aws.ToString(u.Payload); got != `"order-1"` {
		t.Errorf("update Payload = %q, want %q", got, `"order-1"`)
	}
	if u.ChainedInvokeOptions == nil {
		t.Fatal("update has no ChainedInvokeOptions")
	}
	if got := aws.ToString(u.ChainedInvokeOptions.FunctionName); !strings.HasSuffix(got, "function:target:1") {
		t.Errorf("FunctionName = %q, want target ARN", got)
	}
	if u.ChainedInvokeOptions.TenantId != nil {
		t.Errorf("TenantId = %q, want nil", aws.ToString(u.ChainedInvokeOptions.TenantId))
	}
	if got := aws.ToString(u.Name); got != "charge" {
		t.Errorf("Name = %q, want charge", got)
	}
}

func TestInvokeTenantID(t *testing.T) {
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		return Invoke[string](ctx, "", "target", "payload", WithTenantID("tenant-a"))
	})

	updates := updateBatch(t, fake)
	if len(updates) != 1 {
		t.Fatalf("received %d updates, want 1", len(updates))
	}
	if got := aws.ToString(updates[0].ChainedInvokeOptions.TenantId); got != "tenant-a" {
		t.Errorf("TenantId = %q, want tenant-a", got)
	}
	if updates[0].Name != nil {
		t.Errorf("unnamed invoke Name = %q, want nil", aws.ToString(updates[0].Name))
	}
}

func TestInvokeReplaySucceeded(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake,
		stepPayload(`""`, wireOperation{
			Id:                   hashID("1"),
			Status:               "SUCCEEDED",
			ChainedInvokeDetails: &wireChainedInvokeDetails{Result: `"echoed"`},
		}),
		func(ctx Context, _ string) (string, error) {
			return Invoke[string](ctx, "", "target", "ignored")
		})

	if want := `{"Status":"SUCCEEDED","Result":"\"echoed\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if n := len(updateBatch(t, fake)); n != 0 {
		t.Errorf("replayed invoke sent %d updates, want 0", n)
	}
}

func TestInvokeReplayFailedStatuses(t *testing.T) {
	for _, status := range []string{"FAILED", "TIMED_OUT", "STOPPED", "CANCELLED"} {
		t.Run(status, func(t *testing.T) {
			fake := &fakeLambda{}
			var gotErr error
			resp := invokeStep(t, fake,
				stepPayload(`""`, wireOperation{
					Id:     hashID("1"),
					Status: status,
					ChainedInvokeDetails: &wireChainedInvokeDetails{
						Error: &wireFullError{ErrorType: "Error", ErrorMessage: "target blew up"},
					},
				}),
				func(ctx Context, _ string) (string, error) {
					out, err := Invoke[string](ctx, "inv", "target-fn", "x")
					gotErr = err
					return out, err
				})

			var invokeErr *InvokeError
			if !errors.As(gotErr, &invokeErr) {
				t.Fatalf("error = %v, want *InvokeError", gotErr)
			}
			if invokeErr.FunctionID != "target-fn" {
				t.Errorf("FunctionID = %q, want target-fn", invokeErr.FunctionID)
			}
			if !strings.Contains(invokeErr.Error(), "target blew up") {
				t.Errorf("error %q does not carry checkpointed message", invokeErr.Error())
			}
			if !strings.Contains(resp, `"Status":"FAILED"`) {
				t.Errorf("response = %s, want FAILED", resp)
			}
		})
	}
}

func TestInvokeTimedOutUsesCorrectSentinel(t *testing.T) {
	fake := &fakeLambda{}
	var gotErr error
	invokeStep(t, fake,
		stepPayload(`""`, wireOperation{
			Id:     hashID("1"),
			Status: "TIMED_OUT",
			ChainedInvokeDetails: &wireChainedInvokeDetails{
				Error: &wireFullError{ErrorType: "Error", ErrorMessage: "invoke timed out"},
			},
		}),
		func(ctx Context, _ string) (string, error) {
			out, err := Invoke[string](ctx, "inv", "fn", "x")
			gotErr = err
			return out, err
		})

	// Must match ErrInvokeTimedOut, NOT ErrCallbackTimedOut.
	if !errors.Is(gotErr, ErrInvokeTimedOut) {
		t.Errorf("errors.Is(err, ErrInvokeTimedOut) = false, want true; err = %v", gotErr)
	}
	if errors.Is(gotErr, ErrCallbackTimedOut) {
		t.Error("errors.Is(err, ErrCallbackTimedOut) = true, want false for invoke timeout")
	}
}

func TestInvokeCancelledUsesCorrectSentinel(t *testing.T) {
	fake := &fakeLambda{}
	var gotErr error
	invokeStep(t, fake,
		stepPayload(`""`, wireOperation{
			Id:     hashID("1"),
			Status: "CANCELLED",
			ChainedInvokeDetails: &wireChainedInvokeDetails{
				Error: &wireFullError{ErrorType: "Error", ErrorMessage: "invoked execution cancelled"},
			},
		}),
		func(ctx Context, _ string) (string, error) {
			out, err := Invoke[string](ctx, "inv", "fn", "x")
			gotErr = err
			return out, err
		})

	if !errors.Is(gotErr, ErrExecutionCancelled) {
		t.Errorf("errors.Is(err, ErrExecutionCancelled) = false, want true; err = %v", gotErr)
	}
	var invokeErr *InvokeError
	if !errors.As(gotErr, &invokeErr) {
		t.Fatalf("error = %v, want *InvokeError", gotErr)
	}
	if invokeErr.Status != OperationStatusCancelled {
		t.Errorf("Status = %v, want CANCELLED", invokeErr.Status)
	}
}

func TestInvokeStillRunningSuspends(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake,
		stepPayload(`""`, wireOperation{Id: hashID("1"), Status: "STARTED"}),
		func(ctx Context, _ string) (string, error) {
			return Invoke[string](ctx, "", "target", "x")
		})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if n := len(updateBatch(t, fake)); n != 0 {
		t.Errorf("in-flight invoke sent %d updates, want 0", n)
	}
}

func TestInvokeCustomSerdes(t *testing.T) {
	// Payload serdes shapes the START payload; result serdes decodes the
	// checkpointed result.
	t.Run("payload serdes", func(t *testing.T) {
		fake := &fakeLambda{}
		invokeStep(t, fake, stepPayload(`"hi"`), func(ctx Context, event string) (string, error) {
			return Invoke[string](ctx, "", "target", event, WithInvokePayloadSerdes(upperSerdes{}))
		})
		updates := updateBatch(t, fake)
		if got := aws.ToString(updates[0].Payload); got != `"HI"` {
			t.Errorf("Payload = %q, want %q", got, `"HI"`)
		}
	})

	t.Run("result serdes", func(t *testing.T) {
		fake := &fakeLambda{}
		resp := invokeStep(t, fake,
			stepPayload(`""`, wireOperation{
				Id:                   hashID("1"),
				Status:               "SUCCEEDED",
				ChainedInvokeDetails: &wireChainedInvokeDetails{Result: `"value"`},
			}),
			func(ctx Context, _ string) (string, error) {
				return Invoke[string](ctx, "", "target", "x", WithInvokeResultSerdes(upperSerdes{}))
			})
		// upperSerdes uppercases on Marshal only; Unmarshal is standard,
		// so the checkpointed result decodes unchanged.
		if want := `{"Status":"SUCCEEDED","Result":"\"value\""}`; resp != want {
			t.Errorf("response = %s, want %s", resp, want)
		}
	})
}

func TestChildContextFirstRun(t *testing.T) {
	// A child context wrapping an invoke: first invocation checkpoints
	// ContextStarted then ChainedInvokeStarted with ParentId, then
	// suspends (conformance 5-13 shape).
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "wrapper", func(child Context) (string, error) {
			return Invoke[string](child, "", "target", "x")
		})
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	updates := updateBatch(t, fake)
	if len(updates) != 2 {
		t.Fatalf("received %d updates, want 2 (context START, invoke START)", len(updates))
	}
	if updates[0].Type != types.OperationTypeContext {
		t.Errorf("first update Type = %q, want CONTEXT", updates[0].Type)
	}
	if got := aws.ToString(updates[0].SubType); got != "RunInChildContext" {
		t.Errorf("first update SubType = %q, want RunInChildContext", got)
	}
	if got, want := aws.ToString(updates[1].Id), hashID("1-1"); got != want {
		t.Errorf("invoke Id = %q, want hash of 1-1 (%q)", got, want)
	}
	if got, want := aws.ToString(updates[1].ParentId), hashID("1"); got != want {
		t.Errorf("invoke ParentId = %q, want hash of 1 (%q)", got, want)
	}
}

func TestChildContextReplayCompletes(t *testing.T) {
	// Replay of 5-13: context STARTED, inner invoke SUCCEEDED. The child
	// re-enters in replay mode, the invoke returns the cached result, and
	// the context checkpoints SUCCEED with the serialized result.
	fake := &fakeLambda{}
	executedInvokeStart := false
	resp := invokeStep(t, fake,
		stepPayload(`""`,
			wireOperation{Id: hashID("1"), Status: "STARTED"},
			wireOperation{
				Id:                   hashID("1-1"),
				Status:               "SUCCEEDED",
				ChainedInvokeDetails: &wireChainedInvokeDetails{Result: `"echoed"`},
			},
		),
		func(ctx Context, _ string) (string, error) {
			return RunInChildContext(ctx, "wrapper", func(child Context) (string, error) {
				if !child.IsReplaying() {
					executedInvokeStart = true
				}
				return Invoke[string](child, "", "target", "x")
			})
		})

	if executedInvokeStart {
		t.Error("child context re-entered in execution mode, want replay")
	}
	if want := `{"Status":"SUCCEEDED","Result":"\"echoed\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	updates := updateBatch(t, fake)
	if len(updates) != 1 {
		t.Fatalf("received %d updates, want 1 (context SUCCEED)", len(updates))
	}
	if updates[0].Action != types.OperationActionSucceed {
		t.Errorf("update Action = %q, want SUCCEED", updates[0].Action)
	}
	if got := aws.ToString(updates[0].Payload); got != `"echoed"` {
		t.Errorf("context SUCCEED Payload = %q, want %q", got, `"echoed"`)
	}
}

func TestChildContextReplaySucceeded(t *testing.T) {
	fake := &fakeLambda{}
	executed := false
	resp := invokeStep(t, fake,
		stepPayload(`""`, wireOperation{
			Id:             hashID("1"),
			Status:         "SUCCEEDED",
			ContextDetails: &wireContextDetails{Result: `"done"`},
		}),
		func(ctx Context, _ string) (string, error) {
			return RunInChildContext(ctx, "wrapper", func(Context) (string, error) {
				executed = true
				return "recomputed", nil
			})
		})

	if executed {
		t.Error("child function re-executed during replay of a SUCCEEDED context")
	}
	if want := `{"Status":"SUCCEEDED","Result":"\"done\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if n := len(updateBatch(t, fake)); n != 0 {
		t.Errorf("replayed context sent %d updates, want 0", n)
	}
}

func TestChildContextFnError(t *testing.T) {
	fake := &fakeLambda{}
	var gotErr error
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		out, err := RunInChildContext(ctx, "wrapper", func(Context) (string, error) {
			return "", errors.New("child broke")
		})
		gotErr = err
		return out, err
	})

	var childErr *ChildContextError
	if !errors.As(gotErr, &childErr) {
		t.Fatalf("error = %v, want *ChildContextError", gotErr)
	}
	if !strings.Contains(resp, `"Status":"FAILED"`) {
		t.Errorf("response = %s, want FAILED", resp)
	}
	updates := updateBatch(t, fake)
	if len(updates) != 2 {
		t.Fatalf("received %d updates, want 2 (START, FAIL)", len(updates))
	}
	if updates[1].Action != types.OperationActionFail {
		t.Errorf("second update Action = %q, want FAIL", updates[1].Action)
	}
}

func TestChildContextSuspensionIsNotFailure(t *testing.T) {
	// When an operation inside the child suspends, the child must not
	// checkpoint FAIL: the suspension propagates and the invocation ends
	// PENDING.
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "wrapper", func(child Context) (string, error) {
			return Invoke[string](child, "", "target", "x")
		})
	})

	for _, u := range updateBatch(t, fake) {
		if u.Action == types.OperationActionFail {
			t.Errorf("suspension inside child produced a FAIL update for %s", aws.ToString(u.Id))
		}
	}
}

func TestChildContextStepInside(t *testing.T) {
	// A step inside a child context completes synchronously: the whole
	// invocation finishes in one pass with context result checkpointed.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"in"`), func(ctx Context, event string) (string, error) {
		return RunInChildContext(ctx, "wrapper", func(child Context) (string, error) {
			return Step(child, "inner", func(StepContext) (string, error) {
				return event + "-processed", nil
			})
		})
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"in-processed\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	updates := updateBatch(t, fake)
	// context START, step START (id 1-1, parent 1), step SUCCEED, context SUCCEED
	if len(updates) != 4 {
		t.Fatalf("received %d updates, want 4", len(updates))
	}
	if got, want := aws.ToString(updates[1].Id), hashID("1-1"); got != want {
		t.Errorf("inner step Id = %q, want %q", got, want)
	}
	if got, want := aws.ToString(updates[1].ParentId), hashID("1"); got != want {
		t.Errorf("inner step ParentId = %q, want %q", got, want)
	}
	if updates[3].Action != types.OperationActionSucceed || updates[3].Type != types.OperationTypeContext {
		t.Errorf("final update = %q %q, want context SUCCEED", updates[3].Type, updates[3].Action)
	}
}

func TestInvokeAfterSuspensionFailsFast(t *testing.T) {
	// After the first invoke commits to PENDING, subsequent claims on
	// the same context fail.
	fake := &fakeLambda{}
	var secondErr error
	handlerDone := make(chan struct{})
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		defer close(handlerDone)
		_, _ = Invoke[string](ctx, "", "target", "x")
		_, secondErr = Invoke[string](ctx, "", "target2", "y")
		return "", secondErr
	})
	<-handlerDone

	if !errors.Is(secondErr, errSuspendExecution) {
		t.Errorf("invoke after suspension error = %v, want errSuspendExecution", secondErr)
	}
	if n := len(updateBatch(t, fake)); n != 1 {
		t.Errorf("received %d updates, want 1 (first invoke START only)", n)
	}
}

func TestInvokeNilInput(t *testing.T) {
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (string, error) {
		return Invoke[string, any](ctx, "", "target", nil)
	}, withLambdaAPI(fake))
	if _, err := h.Invoke(context.Background(), stepPayload(`""`)); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	updates := updateBatch(t, fake)
	if got := aws.ToString(updates[0].Payload); got != "null" {
		t.Errorf("nil input Payload = %q, want null", got)
	}
}
