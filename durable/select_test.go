package durable

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// selectReport is what the handlers below return so a test can inspect the
// winner, the value, and the error text Select produced.
type selectReport struct {
	Winner string `json:"winner"`
	Value  string `json:"value"`
	Err    string `json:"err,omitempty"`
}

func reportSelect(winner, value string, err error) (string, error) {
	r := selectReport{Winner: winner, Value: value}
	if err != nil {
		r.Err = err.Error()
	}
	b, merr := json.Marshal(r)
	if merr != nil {
		return "", merr
	}
	return string(b), nil
}

// selectFailureError checks that err is the failure of the Select
// operation named name: a *ChildContextError with ErrorType
// PromiseCombinatorError whose cause is a *CombinatorError holding one
// error. It returns that *ChildContextError.
func selectFailureError(t *testing.T, err error, name string) *ChildContextError {
	t.Helper()
	var outer *ChildContextError
	if !errors.As(err, &outer) || outer.Name != name || outer.ErrorType != "PromiseCombinatorError" {
		t.Fatalf("Select error = %T %v, want *ChildContextError %q with ErrorType PromiseCombinatorError", err, err, name)
	}
	var combErr *CombinatorError
	if !errors.As(err, &combErr) || len(combErr.Errors) != 1 {
		t.Fatalf("Select error = %v, want a *CombinatorError cause holding one error", err)
	}
	return outer
}

// decodeSelectReport parses the SUCCEEDED response of a handler that
// returned reportSelect's JSON.
func decodeSelectReport(t *testing.T, resp string) selectReport {
	t.Helper()
	r := parseResponse(t, []byte(resp))
	if r.Status != "SUCCEEDED" || r.Result == nil {
		t.Fatalf("response = %s, want SUCCEEDED with a result", resp)
	}
	var inner string
	if err := json.Unmarshal([]byte(*r.Result), &inner); err != nil {
		t.Fatalf("unmarshal result string: %v", err)
	}
	var report selectReport
	if err := json.Unmarshal([]byte(inner), &report); err != nil {
		t.Fatalf("unmarshal select report %q: %v", inner, err)
	}
	return report
}

func TestSelectFirstTerminalWins(t *testing.T) {
	// The slow branch is gated until Select has returned, so the fast
	// branch is the only one that can settle first. The winner's name and
	// value are checkpointed together as the Select operation's result.
	fake := &fakeLambda{}
	gate := make(chan struct{})
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		winner, value, err := Select(ctx, "pick", []Branch[string]{
			{Name: "fast", Func: func(Context) (string, error) { return "fast-result", nil }},
			{Name: "slow", Func: func(Context) (string, error) {
				<-gate
				return "slow-result", nil
			}},
		})
		close(gate)
		return reportSelect(winner, value, err)
	})

	got := decodeSelectReport(t, resp)
	if got.Winner != "fast" || got.Value != "fast-result" || got.Err != "" {
		t.Errorf("Select = %+v, want winner fast, value fast-result, no error", got)
	}
	want := `{"winner":"fast","outcome":{"status":"fulfilled","value":"fast-result"}}`
	if payload := succeedPayload(t, fake, "1"); payload != want {
		t.Errorf("Select SUCCEED payload = %s, want %s", payload, want)
	}
}

func TestSelectWinnerFailureNamesWinner(t *testing.T) {
	// A branch that fails first wins: winner names it, and err is a
	// ChildContextError named after the Select operation with ErrorType
	// PromiseCombinatorError and a CombinatorError cause. The Select
	// operation is recorded as FAILED with ErrorType PromiseCombinatorError
	// and the winner's name as its ErrorData. The branch's own child
	// context is recorded as FAILED too.
	fake := &fakeLambda{}
	gate := make(chan struct{})
	errCh := make(chan error, 1)
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		winner, value, err := Select(ctx, "pick", []Branch[string]{
			{Name: "bad", Func: func(Context) (string, error) { return "", errors.New("boom") }},
			{Name: "slow", Func: func(Context) (string, error) {
				<-gate
				return "slow-result", nil
			}},
		})
		close(gate)
		errCh <- err
		return reportSelect(winner, value, err)
	})

	got := decodeSelectReport(t, resp)
	if got.Winner != "bad" || got.Value != "" {
		t.Errorf("Select = %+v, want winner bad with zero value", got)
	}
	outer := selectFailureError(t, <-errCh, "pick")
	wantMsg := `durable: child context "bad" failed: Error: boom`
	if outer.Message != wantMsg || outer.ErrorData != `{"winner":"bad"}` {
		t.Errorf("Select error = {Message %q, ErrorData %q}, want {%q, %q}", outer.Message, outer.ErrorData, wantMsg, `{"winner":"bad"}`)
	}

	var selectFailed, branchFailed bool
	for _, u := range updateBatch(t, fake) {
		if u.Type != OperationTypeContext {
			continue
		}
		id := aws.ToString(u.Id)
		if id == hashID("1") && u.Action == OperationActionSucceed {
			t.Error("Select operation 1 was recorded as SUCCEEDED, want FAILED")
		}
		if id == hashID("1") && u.Action == OperationActionFail {
			selectFailed = true
			if u.Error == nil || aws.ToString(u.Error.ErrorType) != "PromiseCombinatorError" ||
				aws.ToString(u.Error.ErrorMessage) != wantMsg || aws.ToString(u.Error.ErrorData) != `{"winner":"bad"}` {
				t.Errorf("Select failure record = %+v, want ErrorType PromiseCombinatorError, message %q, ErrorData {\"winner\":\"bad\"}", u.Error, wantMsg)
			}
		}
		if id == hashID("1-1") && u.Action == OperationActionFail {
			branchFailed = true
		}
	}
	if !selectFailed {
		t.Error("Select operation 1 was not recorded as FAILED")
	}
	if !branchFailed {
		t.Error("branch child context 1-1 was not recorded as FAILED")
	}
}

// TestSelectFailureRecordsWinnerWithoutHTMLEscaping asserts that the
// winner name in a failed Select's ErrorData keeps <, >, and & literal,
// as every other stored JSON value does.
func TestSelectFailureRecordsWinnerWithoutHTMLEscaping(t *testing.T) {
	const winner = "<a&b>"
	const wantData = `{"winner":"<a&b>"}`
	fake := &fakeLambda{}
	errCh := make(chan error, 1)
	invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		gotWinner, value, err := Select(ctx, "pick", []Branch[string]{
			{Name: winner, Func: func(Context) (string, error) { return "", errors.New("boom") }},
		})
		errCh <- err
		return reportSelect(gotWinner, value, err)
	})

	if outer := selectFailureError(t, <-errCh, "pick"); outer.ErrorData != wantData {
		t.Errorf("Select error ErrorData = %q, want %q", outer.ErrorData, wantData)
	}
	var recorded bool
	for _, u := range updateBatch(t, fake) {
		if u.Type != OperationTypeContext || aws.ToString(u.Id) != hashID("1") || u.Action != OperationActionFail {
			continue
		}
		recorded = true
		if u.Error == nil {
			t.Fatal("Select failure record has no Error")
		}
		if got := aws.ToString(u.Error.ErrorData); got != wantData {
			t.Errorf("Select failure record ErrorData = %q, want %q", got, wantData)
		}
	}
	if !recorded {
		t.Error("Select operation 1 was not recorded as FAILED")
	}
}

func TestSelectReplayReturnsCheckpointedWinner(t *testing.T) {
	// A SUCCEEDED Select operation replays from its checkpoint: the stored
	// winner and value are returned and no branch body runs, so a branch
	// that would now finish first cannot change the outcome.
	fake := &fakeLambda{}
	stored := `{"winner":"b","outcome":{"status":"fulfilled","value":"b-val"}}`
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{Result: stored}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		winner, value, err := Select(ctx, "pick", []Branch[string]{
			{Name: "a", Func: func(Context) (string, error) {
				t.Error("branch a ran on replay")
				return "a-val", nil
			}},
			{Name: "b", Func: func(Context) (string, error) {
				t.Error("branch b ran on replay")
				return "b-val", nil
			}},
		})
		return reportSelect(winner, value, err)
	})

	got := decodeSelectReport(t, resp)
	if got.Winner != "b" || got.Value != "b-val" || got.Err != "" {
		t.Errorf("Select on replay = %+v, want winner b, value b-val", got)
	}
}

func TestSelectReplayReturnsCheckpointedFailure(t *testing.T) {
	// A FAILED Select operation replays from its checkpoint: no branch body
	// runs, winner is read from the recorded ErrorData, and err has the
	// same shape as on the first invocation.
	fake := &fakeLambda{}
	msg := `durable: child context "bad" failed: Error: boom`
	payload := childPayload(`"x"`,
		checkpointedChild("1", "FAILED", &wireContextDetails{
			Error: &wireFullError{ErrorType: "PromiseCombinatorError", ErrorMessage: msg, ErrorData: `{"winner":"bad"}`},
		}),
	)
	errCh := make(chan error, 1)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		winner, value, err := Select(ctx, "pick", []Branch[string]{
			{Name: "bad", Func: func(Context) (string, error) {
				t.Error("branch bad ran on replay")
				return "", errors.New("boom")
			}},
		})
		errCh <- err
		return reportSelect(winner, value, err)
	})

	got := decodeSelectReport(t, resp)
	if got.Winner != "bad" {
		t.Errorf("Select on replay = %+v, want winner bad", got)
	}
	if outer := selectFailureError(t, <-errCh, "pick"); outer.Message != msg {
		t.Errorf("Select error message on replay = %q, want %q", outer.Message, msg)
	}
}

func TestSelectReplayRejectedOutcomeResult(t *testing.T) {
	// A SUCCEEDED Select record whose result holds a rejected outcome
	// replays as the same failure shape: winner names the branch, and err
	// is a PromiseCombinatorError ChildContextError holding the branch's
	// rebuilt error.
	fake := &fakeLambda{}
	stored := `{"winner":"bad","outcome":{"status":"rejected","error":"boom","errorType":"ChildContextError",` +
		`"operation":{"name":"bad","errorType":"Error","message":"boom"}}}`
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{Result: stored}),
	)
	errCh := make(chan error, 1)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		winner, value, err := Select(ctx, "pick", []Branch[string]{
			{Name: "bad", Func: func(Context) (string, error) {
				t.Error("branch bad ran on replay")
				return "", errors.New("boom")
			}},
		})
		errCh <- err
		return reportSelect(winner, value, err)
	})

	got := decodeSelectReport(t, resp)
	if got.Winner != "bad" {
		t.Errorf("Select on replay = %+v, want winner bad", got)
	}
	if outer := selectFailureError(t, <-errCh, "pick"); outer.Message != `durable: child context "bad" failed: Error: boom` {
		t.Errorf("Select error message on replay = %q, want the branch error's message", outer.Message)
	}
}

func TestSelectEmptyBranchesErrors(t *testing.T) {
	// No branches can never produce a winner, so Select fails immediately
	// instead of suspending, and records no operation.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		winner, value, err := Select[string](ctx, "empty", nil)
		if err == nil || errors.Is(err, errSuspendExecution) {
			return "", errors.New("expected an immediate error")
		}
		return reportSelect(winner, value, err)
	})

	got := decodeSelectReport(t, resp)
	if want := `durable: Select "empty": no branches`; got.Err != want || got.Winner != "" {
		t.Errorf("Select = %+v, want error %q and no winner", got, want)
	}
	if n := len(fake.gotUpdateBatches); n != 0 {
		t.Errorf("recorded %d checkpoint batches, want none", n)
	}
}

func TestSelectDuplicateBranchNamesError(t *testing.T) {
	// Two branches with one name would make the winner ambiguous.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		winner, value, err := Select(ctx, "dup", []Branch[string]{
			{Name: "same", Func: func(Context) (string, error) { return "one", nil }},
			{Name: "other", Func: func(Context) (string, error) { return "two", nil }},
			{Name: "same", Func: func(Context) (string, error) { return "three", nil }},
		})
		if err == nil {
			return "", errors.New("expected an error")
		}
		return reportSelect(winner, value, err)
	})

	got := decodeSelectReport(t, resp)
	if want := `durable: Select "dup": duplicate branch name "same"`; got.Err != want || got.Winner != "" {
		t.Errorf("Select = %+v, want error %q and no winner", got, want)
	}
	if n := len(fake.gotUpdateBatches); n != 0 {
		t.Errorf("recorded %d checkpoint batches, want none", n)
	}
}

func TestSelectFastWinsWhileSiblingReachesBlockingPoint(t *testing.T) {
	// One branch suspends on a wait and the other returns at once. The
	// fast branch wins. The suspending branch is gated until Select has
	// returned, so it demonstrably reaches its blocking point after the
	// winner was decided: its wait START is checkpointed. The handler then
	// waits too, so no branch can make progress and the invocation
	// responds PENDING. The next invocation replays the checkpointed
	// winner.
	fake := &fakeLambda{}
	gate := make(chan struct{})
	reportCh := make(chan selectReport, 1)
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		winner, value, err := Select(ctx, "pick", []Branch[string]{
			{Name: "fast", Func: func(Context) (string, error) { return "fast-result", nil }},
			{Name: "suspender", Func: func(childCtx Context) (string, error) {
				<-gate
				return "", Wait(childCtx, "slow-wait", time.Minute)
			}},
		})
		report := selectReport{Winner: winner, Value: value}
		if err != nil {
			report.Err = err.Error()
		}
		reportCh <- report
		close(gate)
		if werr := Wait(ctx, "root-wait", time.Minute); werr != nil {
			return "", werr
		}
		return value, nil
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	got := <-reportCh
	if got.Winner != "fast" || got.Value != "fast-result" || got.Err != "" {
		t.Errorf("Select = %+v, want winner fast, value fast-result, no error", got)
	}
	want := `{"winner":"fast","outcome":{"status":"fulfilled","value":"fast-result"}}`
	if payload := succeedPayload(t, fake, "1"); payload != want {
		t.Errorf("Select SUCCEED payload = %s, want %s", payload, want)
	}
	var waitStarted bool
	for _, u := range updateBatch(t, fake) {
		if u.Type == OperationTypeWait && u.Action == OperationActionStart && aws.ToString(u.Name) == "slow-wait" {
			waitStarted = true
		}
	}
	if !waitStarted {
		t.Error("suspending branch's wait START was not checkpointed")
	}
}

func TestSelectSuspendThenTerminalPropagatesSuspension(t *testing.T) {
	// Once a suspension is observed, Select drains, and the suspension
	// propagates, matching Race. The invocation suspends only when no
	// branch can make progress, so every branch is blocked by then and
	// every outcome the drain observes is a suspension: one branch awaits
	// a callback, one a wait, and one a callback after running a step.
	fake := &fakeLambda{}
	errCh := make(chan error, 1)
	outcomesCh := make(chan []error, 1)
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, rec := setCombinatorObserve(t, ctx)

		_, _, err := Select(ctx, "pick", []Branch[string]{
			{Name: "suspender", Func: func(childCtx Context) (string, error) {
				cb, err := CreateCallback[string](childCtx, "cb")
				if err != nil {
					return "", err
				}
				return cb.Result(childCtx)
			}},
			{Name: "terminal", Func: func(childCtx Context) (string, error) {
				if _, err := Step(childCtx, "t-step", func(StepContext) (string, error) {
					return "terminal-result", nil
				}); err != nil {
					return "", err
				}
				cb, err := CreateCallback[string](childCtx, "cb-after-step")
				if err != nil {
					return "", err
				}
				return cb.Result(childCtx)
			}},
			{Name: "failing", Func: func(childCtx Context) (string, error) {
				return "", Wait(childCtx, "w", time.Minute)
			}},
		})
		outcomesCh <- rec.snapshot()
		errCh <- err
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	if err := <-errCh; !errors.Is(err, errSuspendExecution) {
		t.Errorf("Select error = %v, want errSuspendExecution", err)
	}
	outcomes := <-outcomesCh
	if len(outcomes) != 3 {
		t.Fatalf("observed %d outcomes at return, want 3: %v", len(outcomes), outcomes)
	}
	for _, o := range outcomes {
		if !errors.Is(o, errSuspendExecution) {
			t.Errorf("observed outcome %v, want errSuspendExecution", o)
		}
	}
	assertStepCheckpointed(t, fake, "t-step")
}

func TestSelectHonoursChildSerdes(t *testing.T) {
	// WithChildSerdes reaches the Select operation's checkpoint: upperSerdes
	// uppercases on Marshal and decodes standard JSON, so the transformed
	// record is visible in the payload and in the returned value.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		winner, value, err := Select(ctx, "pick", []Branch[string]{
			{Name: "only", Func: func(Context) (string, error) { return "value", nil }},
		}, WithChildSerdes(upperSerdes{}))
		return reportSelect(winner, value, err)
	})

	got := decodeSelectReport(t, resp)
	if got.Winner != "ONLY" || got.Value != "VALUE" || got.Err != "" {
		t.Errorf("Select = %+v, want uppercased winner and value", got)
	}
	want := `{"WINNER":"ONLY","OUTCOME":{"STATUS":"FULFILLED","VALUE":"VALUE"}}`
	if payload := succeedPayload(t, fake, "1"); payload != want {
		t.Errorf("Select SUCCEED payload = %s, want %s", payload, want)
	}
}

// errSelectMapped is the unrelated error the mapper in the tests below
// returns in place of the Select failure.
var errSelectMapped = errors.New("mapped select failure")

func mapSelectFailure(*ChildContextError) error { return errSelectMapped }

func TestSelectErrorMapperKeepsWinner(t *testing.T) {
	// A mapper that replaces the failure with an unrelated error does not
	// erase the winner. Select returns the winner read from the recorded
	// failure and the mapper's error unchanged.
	fake := &fakeLambda{}
	gate := make(chan struct{})
	errCh := make(chan error, 1)
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		winner, value, err := Select(ctx, "pick", []Branch[string]{
			{Name: "bad", Func: func(Context) (string, error) { return "", errors.New("boom") }},
			{Name: "slow", Func: func(Context) (string, error) {
				<-gate
				return "slow-result", nil
			}},
		}, WithChildErrorMapper(mapSelectFailure))
		close(gate)
		errCh <- err
		return reportSelect(winner, value, err)
	})

	got := decodeSelectReport(t, resp)
	if got.Winner != "bad" || got.Value != "" {
		t.Errorf("Select = %+v, want winner bad with zero value", got)
	}
	if err := <-errCh; err != errSelectMapped {
		t.Errorf("Select error = %T %v, want the mapper's error", err, err)
	}
}

func TestSelectReplayErrorMapperKeepsWinner(t *testing.T) {
	// On replay of a FAILED Select the mapper runs on the rebuilt failure.
	// Its unrelated error is returned, and the winner is still read from
	// the recorded ErrorData.
	fake := &fakeLambda{}
	msg := `durable: child context "bad" failed: Error: boom`
	payload := childPayload(`"x"`,
		checkpointedChild("1", "FAILED", &wireContextDetails{
			Error: &wireFullError{ErrorType: "PromiseCombinatorError", ErrorMessage: msg, ErrorData: `{"winner":"bad"}`},
		}),
	)
	errCh := make(chan error, 1)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		winner, value, err := Select(ctx, "pick", []Branch[string]{
			{Name: "bad", Func: func(Context) (string, error) {
				t.Error("branch bad ran on replay")
				return "", errors.New("boom")
			}},
		}, WithChildErrorMapper(mapSelectFailure))
		errCh <- err
		return reportSelect(winner, value, err)
	})

	got := decodeSelectReport(t, resp)
	if got.Winner != "bad" {
		t.Errorf("Select on replay = %+v, want winner bad", got)
	}
	if err := <-errCh; err != errSelectMapped {
		t.Errorf("Select error on replay = %T %v, want the mapper's error", err, err)
	}
}
