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

// selectPayload is the checkpoint payload Select stores for its winner.
type selectPayload struct {
	Winner  string          `json:"winner"`
	Outcome json.RawMessage `json:"outcome"`
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
	// A branch that fails first wins: err is that branch's error, a
	// ChildContextError naming the branch, and winner names it too. The
	// Select operation is recorded as SUCCEEDED with the failure inside its
	// result; the branch's own child context is recorded as FAILED.
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
	err := <-errCh
	var childErr *ChildContextError
	if !errors.As(err, &childErr) {
		t.Fatalf("Select error = %T %v, want *ChildContextError", err, err)
	}
	if childErr.Name != "bad" || childErr.Message != "boom" {
		t.Errorf("ChildContextError = {Name %q, Message %q}, want {bad, boom}", childErr.Name, childErr.Message)
	}

	var stored selectPayload
	if err := json.Unmarshal([]byte(succeedPayload(t, fake, "1")), &stored); err != nil {
		t.Fatalf("unmarshal Select payload: %v", err)
	}
	if stored.Winner != "bad" {
		t.Errorf("stored winner = %q, want bad", stored.Winner)
	}
	var outcome settledJSON[string]
	if err := json.Unmarshal(stored.Outcome, &outcome); err != nil {
		t.Fatalf("unmarshal stored outcome: %v", err)
	}
	if outcome.Status != "rejected" || outcome.ErrorType != "ChildContextError" ||
		outcome.Operation == nil || outcome.Operation.Name != "bad" {
		t.Errorf("stored outcome = %+v, want rejected ChildContextError for branch bad", outcome)
	}

	var branchFailed bool
	for _, u := range updateBatch(t, fake) {
		if u.Type == OperationTypeContext && u.Action == OperationActionFail && aws.ToString(u.Id) == hashID("1-1") {
			branchFailed = true
		}
	}
	if !branchFailed {
		t.Error("branch child context 1-1 was not recorded as FAILED")
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
	// A stored rejected outcome replays as the winning branch's error: a
	// ChildContextError naming the branch, with winner naming it too.
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
	var childErr *ChildContextError
	if err := <-errCh; !errors.As(err, &childErr) || childErr.Name != "bad" || childErr.Message != "boom" {
		t.Errorf("Select error on replay = %v, want ChildContextError for branch bad with message boom", err)
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
	// winner was decided: its wait START is checkpointed, and because a
	// wait commits the invocation to PENDING, the invocation responds
	// PENDING. The next invocation replays the checkpointed winner.
	fake := &fakeLambda{}
	gate := make(chan struct{})
	blocked := make(chan struct{})
	reportCh := make(chan selectReport, 1)
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		winner, value, err := Select(ctx, "pick", []Branch[string]{
			{Name: "fast", Func: func(Context) (string, error) { return "fast-result", nil }},
			{Name: "suspender", Func: func(childCtx Context) (string, error) {
				<-gate
				werr := Wait(childCtx, "slow-wait", time.Minute)
				close(blocked)
				return "", werr
			}},
		})
		report := selectReport{Winner: winner, Value: value}
		if err != nil {
			report.Err = err.Error()
		}
		reportCh <- report
		close(gate)
		<-blocked
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
	// Once a suspension is observed, Select drains: the success and the
	// failure that follow are observed but discarded, and the suspension
	// propagates, matching Race. The interleaving is forced through the
	// observation hook: the terminal branches proceed only after the
	// receive loop has observed the suspension.
	fake := &fakeLambda{}
	errCh := make(chan error, 1)
	outcomesCh := make(chan []error, 1)
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		suspendObserved, rec := setCombinatorObserve(t, ctx)

		_, _, err := Select(ctx, "pick", []Branch[string]{
			{Name: "suspender", Func: func(childCtx Context) (string, error) {
				cb, err := CreateCallback[string](childCtx, "cb")
				if err != nil {
					return "", err
				}
				return cb.Result()
			}},
			{Name: "terminal", Func: func(childCtx Context) (string, error) {
				<-suspendObserved
				return Step(childCtx, "t-step", func(StepContext) (string, error) {
					return "terminal-result", nil
				})
			}},
			{Name: "failing", Func: func(Context) (string, error) {
				<-suspendObserved
				return "", errors.New("late-failure")
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
	assertDrainedOutcomes(t, <-outcomesCh, 3)
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
