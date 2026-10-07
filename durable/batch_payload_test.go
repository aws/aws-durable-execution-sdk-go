package durable_test

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestMapStoredPayloadMatchesJS asserts that the parent Map context stores the
// same JSON the JavaScript SDK stores: an envelope keyed "all" and
// "completionReason", string status and reason values, and each item result as
// a raw JSON value.
func TestMapStoredPayloadMatchesJS(t *testing.T) {
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", []int{10, 20},
			func(c durable.Context, item, _ int) (int, error) {
				return durable.Step(c, "s", func(_ durable.StepContext) (int, error) {
					return item + 1, nil
				})
			})
		if err != nil {
			return 0, err
		}
		return res.SuccessCount(), nil
	}
	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", r.Status)
	}
	op := r.Operation("m")
	if op == nil || op.ContextDetails == nil {
		t.Fatal("no stored Map context operation named \"m\"")
	}
	got := op.ContextDetails.Result
	// The exact bytes observed in the JavaScript SDK 2.6.0 for the same
	// handler.
	want := `{"all":[{"result":11,"index":0,"status":"SUCCEEDED"},{"result":21,"index":1,"status":"SUCCEEDED"}],"completionReason":"ALL_COMPLETED"}`
	if got != want {
		t.Errorf("stored Map payload mismatch\n got: %s\nwant: %s", got, want)
	}
}

// TestParallelStoredPayloadMatchesJS asserts the same for Parallel. The
// JavaScript SDK stores no branch name in the item, so the Go payload must not
// either.
func TestParallelStoredPayloadMatchesJS(t *testing.T) {
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Parallel(ctx, "p", []durable.Branch[int]{
			{Name: "a", Func: func(c durable.Context) (int, error) { return 1, nil }},
			{Name: "b", Func: func(c durable.Context) (int, error) { return 2, nil }},
		})
		if err != nil {
			return 0, err
		}
		return res.SuccessCount(), nil
	}
	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", r.Status)
	}
	op := r.Operation("p")
	if op == nil || op.ContextDetails == nil {
		t.Fatal("no stored Parallel context operation named \"p\"")
	}
	got := op.ContextDetails.Result
	// The exact bytes observed in the JavaScript SDK 2.6.0: no "name"
	// field on the item.
	want := `{"all":[{"result":1,"index":0,"status":"SUCCEEDED"},{"result":2,"index":1,"status":"SUCCEEDED"}],"completionReason":"ALL_COMPLETED"}`
	if got != want {
		t.Errorf("stored Parallel payload mismatch\n got: %s\nwant: %s", got, want)
	}
}

// TestBatchEnumsJSONWireForm asserts that BatchItemStatus and
// CompletionReason encode as their String() values and decode back, that an
// unrecognized string decodes to the zero value, and that the integer
// constants keep their pinned values.
func TestBatchEnumsJSONWireForm(t *testing.T) {
	pinnedStatus := map[durable.BatchItemStatus]int{
		durable.BatchItemNotStarted: 0,
		durable.BatchItemSucceeded:  1,
		durable.BatchItemFailed:     2,
		durable.BatchItemStarted:    4,
	}
	for s, n := range pinnedStatus {
		if int(s) != n {
			t.Errorf("%s = %d, want %d", s, int(s), n)
		}
	}
	for _, s := range []durable.BatchItemStatus{durable.BatchItemSucceeded, durable.BatchItemFailed, durable.BatchItemStarted} {
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if want := `"` + s.String() + `"`; string(b) != want {
			t.Errorf("json.Marshal(%d) = %s, want %s", int(s), b, want)
		}
		var back durable.BatchItemStatus
		if err := json.Unmarshal(b, &back); err != nil || back != s {
			t.Errorf("json.Unmarshal(%s) = %v, %v; want %v", b, back, err, s)
		}
	}

	pinnedReason := map[durable.CompletionReason]int{
		durable.CompletionAllCompleted:             1,
		durable.CompletionMinSuccessfulReached:     2,
		durable.CompletionFailureToleranceExceeded: 3,
		durable.CompletionCustomSucceeded:          4,
		durable.CompletionCustomFailed:             5,
	}
	for r, n := range pinnedReason {
		if int(r) != n {
			t.Errorf("%s = %d, want %d", r, int(r), n)
		}
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if want := `"` + r.String() + `"`; string(b) != want {
			t.Errorf("json.Marshal(%d) = %s, want %s", n, b, want)
		}
		var back durable.CompletionReason
		if err := json.Unmarshal(b, &back); err != nil || back != r {
			t.Errorf("json.Unmarshal(%s) = %v, %v; want %v", b, back, err, r)
		}
	}

	status := durable.BatchItemSucceeded
	if err := json.Unmarshal([]byte(`"NO_SUCH_STATUS"`), &status); err != nil || status != 0 {
		t.Errorf("unrecognized status decoded to %d, %v; want 0, nil", int(status), err)
	}
	reason := durable.CompletionAllCompleted
	if err := json.Unmarshal([]byte(`"NO_SUCH_REASON"`), &reason); err != nil || reason != 0 {
		t.Errorf("unrecognized reason decoded to %d, %v; want 0, nil", int(reason), err)
	}
}

// itemErrorView records what a handler observed of a failed batch item on
// one invocation.
type itemErrorView struct {
	Text      string
	AsStep    bool
	AsChild   bool
	Attempts  int
	ErrorType string
}

// viewItemError builds the itemErrorView of a failed item's error.
func viewItemError(err error) itemErrorView {
	v := itemErrorView{Text: err.Error()}
	var stepErr *durable.StepError
	if errors.As(err, &stepErr) {
		v.AsStep = true
		v.Attempts = stepErr.Attempts
	}
	var childErr *durable.ChildContextError
	if errors.As(err, &childErr) {
		v.AsChild = true
		v.ErrorType = childErr.ErrorType
	}
	return v
}

// failingStepMapHandler runs a Map with one item whose step fails after
// three attempts, then waits so that a second invocation replays the Map
// from its stored payload. views receives the item error each invocation
// observed.
func failingStepMapHandler(views *[]itemErrorView) func(durable.Context, any) (string, error) {
	return func(ctx durable.Context, _ any) (string, error) {
		strategy, err := durable.NewRetryStrategy(durable.RetryConfig{
			MaxAttempts: 3, InitialDelay: time.Second, BackoffRate: 1,
		})
		if err != nil {
			return "", err
		}
		res, err := durable.Map(ctx, "m", []int{1, 2},
			func(c durable.Context, item, _ int) (int, error) {
				return durable.Step(c, "s", func(_ durable.StepContext) (int, error) {
					if item == 2 {
						return 0, errors.New("boom")
					}
					return item, nil
				}, durable.WithRetry(strategy))
			},
			durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: intPtr(1)}))
		if err != nil {
			return "", err
		}
		failed := res.Failed()
		if len(failed) != 1 {
			return "", errors.New("want exactly one failed item")
		}
		*views = append(*views, viewItemError(failed[0].Err))
		if err := durable.Wait(ctx, "cooldown", time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}
}

// TestMapItemErrorStoredNested asserts that a failed item stores a nested
// error object whose Cause chain follows the error's Unwrap chain, and that
// errors.As finds the item's concrete error types after a replay.
func TestMapItemErrorStoredNested(t *testing.T) {
	var views []itemErrorView
	r, err := durabletest.NewLocalRunner(failingStepMapHandler(&views)).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error %+v)", r.Status, r.Error)
	}

	var stored struct {
		All []struct {
			Error  json.RawMessage `json:"error"`
			Index  int             `json:"index"`
			Status string          `json:"status"`
		} `json:"all"`
	}
	payload := r.Operation("m").ContextDetails.Result
	if err := json.Unmarshal([]byte(payload), &stored); err != nil {
		t.Fatalf("stored payload %s: %v", payload, err)
	}
	if len(stored.All) != 2 || stored.All[1].Status != "FAILED" {
		t.Fatalf("stored items = %s, want item 1 FAILED", payload)
	}
	type errObj struct {
		ErrorType    string  `json:"ErrorType"`
		ErrorMessage string  `json:"ErrorMessage"`
		Cause        *errObj `json:"Cause"`
	}
	var top errObj
	if err := json.Unmarshal(stored.All[1].Error, &top); err != nil {
		t.Fatalf("stored error %s: %v", stored.All[1].Error, err)
	}
	if !strings.HasPrefix(string(stored.All[1].Error), `{"ErrorType":"ChildContextError","ErrorMessage":`) {
		t.Errorf("stored error %s does not start with ErrorType then ErrorMessage", stored.All[1].Error)
	}
	if top.ErrorType != "ChildContextError" || top.Cause == nil {
		t.Fatalf("stored error = %s, want ChildContextError with a Cause", stored.All[1].Error)
	}
	step := top.Cause
	if step.ErrorType != "StepError" || step.ErrorMessage != "boom" || step.Cause == nil {
		t.Fatalf("stored Cause = %+v, want StepError \"boom\" with a Cause", step)
	}
	if leaf := step.Cause; leaf.ErrorType != "Error" || leaf.ErrorMessage != "boom" || leaf.Cause != nil {
		t.Fatalf("stored leaf = %+v, want Error \"boom\" with no Cause", leaf)
	}

	if len(views) != 2 {
		t.Fatalf("observed %d invocations after the Map, want 2 (live, then replay)", len(views))
	}
	if replay := views[1]; !replay.AsChild || !replay.AsStep {
		t.Errorf("replayed item error %+v: errors.As must find *ChildContextError and *StepError", replay)
	}
}

// TestMapStepErrorSameOnFirstRunAndReplay asserts that an item failing
// with a *StepError after three attempts reports the same error on the
// first run and on replay: the same concrete type, the same Error() text,
// and Attempts 0 on both.
func TestMapStepErrorSameOnFirstRunAndReplay(t *testing.T) {
	var views []itemErrorView
	r, err := durabletest.NewLocalRunner(failingStepMapHandler(&views)).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error %+v)", r.Status, r.Error)
	}
	if len(views) != 2 {
		t.Fatalf("observed %d invocations after the Map, want 2", len(views))
	}
	live, replay := views[0], views[1]
	if !live.AsStep || !live.AsChild {
		t.Fatalf("first-run item error %+v: want *ChildContextError wrapping *StepError", live)
	}
	if live.Attempts != 0 || replay.Attempts != 0 {
		t.Errorf("Attempts = %d live, %d replay; want 0 on both", live.Attempts, replay.Attempts)
	}
	if !reflect.DeepEqual(live, replay) {
		t.Errorf("item error differs\n first run: %+v\n    replay: %+v", live, replay)
	}
	// The step did run three times.
	attempts := 0
	for _, op := range r.Operations {
		if op.Name == "s" && op.StepDetails != nil && op.Status == "FAILED" {
			attempts = int(op.StepDetails.Attempt)
		}
	}
	if attempts != 3 {
		t.Errorf("failed step recorded %d attempts, want 3", attempts)
	}
}

// bigItem is a result large enough that three of them exceed the 262144
// byte checkpoint limit, while each stays under it.
var bigItem = strings.Repeat("x", 100*1024)

// TestMapOverflowStoresSummaryRecord asserts that a Map whose full result
// exceeds 262144 bytes stores the summary record, with the bytes the
// JavaScript SDK stores for the same batch, and marks the checkpoint for
// child replay.
func TestMapOverflowStoresSummaryRecord(t *testing.T) {
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", []int{0, 1, 2},
			func(c durable.Context, _, _ int) (string, error) { return bigItem, nil })
		if err != nil {
			return 0, err
		}
		return res.SuccessCount(), nil
	}
	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error %+v)", r.Status, r.Error)
	}
	op := r.Operation("m")
	want := `{"type":"MapResult","totalCount":3,"successCount":3,"failureCount":0,"completionReason":"ALL_COMPLETED","status":"SUCCEEDED","itemStatuses":"SSS"}`
	if got := op.ContextDetails.Result; got != want {
		t.Errorf("stored Map record mismatch (%d bytes)\n got: %.300s\nwant: %s", len(got), got, want)
	}
	if !op.ContextDetails.ReplayChildren {
		t.Error("Map checkpoint is not marked for child replay")
	}
}

// abandoningParallel runs a Parallel whose two first branches return big
// and whose third waits on a callback no one completes, under
// MinSuccessful 2, so the third branch is abandoned. It then waits so that a second invocation
// replays the Parallel. views receives a description of the result each
// invocation observed.
func abandoningParallel(big string, views *[]string) func(durable.Context, any) (string, error) {
	return func(ctx durable.Context, _ any) (string, error) {
		res, err := durable.Parallel(ctx, "p", []durable.Branch[string]{
			{Name: "a", Func: func(c durable.Context) (string, error) { return big, nil }},
			{Name: "b", Func: func(c durable.Context) (string, error) { return big, nil }},
			{Name: "slow", Func: func(c durable.Context) (string, error) {
				// The callback is never completed, so this branch is
				// still running when the other two have succeeded.
				return durable.WaitForCallback[string](c, "never",
					func(durable.StepContext, string) error { return nil })
			}},
		}, durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 2}))
		if err != nil {
			return "", err
		}
		*views = append(*views, describeBatch(res))
		if err := durable.Wait(ctx, "cooldown", time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}
}

// describeBatch renders every field of a batch result that replay must
// reproduce.
func describeBatch[O any](res durable.BatchResult[O]) string {
	var b strings.Builder
	b.WriteString(res.Reason.String())
	for _, item := range res.Items {
		b.WriteString("|")
		b.WriteString(item.Name)
		b.WriteString(":")
		b.WriteString(item.Status.String())
		if item.Err != nil {
			b.WriteString(":" + item.Err.Error())
		}
		if item.Status == durable.BatchItemSucceeded {
			raw, _ := json.Marshal(item.Result)
			fmt.Fprintf(&b, ":%d bytes:%x", len(raw), sha256.Sum256(raw))
		}
	}
	return b.String()
}

// TestParallelOverflowStoresSummaryRecord asserts that a Parallel whose full
// result exceeds 262144 bytes stores the summary record with startedCount,
// with the bytes the JavaScript SDK stores for the same batch, and marks the
// checkpoint for child replay.
func TestParallelOverflowStoresSummaryRecord(t *testing.T) {
	var views []string
	big := strings.Repeat("x", 140*1024)
	r, err := durabletest.NewLocalRunner(abandoningParallel(big, &views)).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error %+v)", r.Status, r.Error)
	}
	op := r.Operation("p")
	want := `{"type":"ParallelResult","totalCount":3,"successCount":2,"failureCount":0,"startedCount":1,"completionReason":"MIN_SUCCESSFUL_REACHED","status":"SUCCEEDED","itemStatuses":"SS-"}`
	if got := op.ContextDetails.Result; got != want {
		t.Errorf("stored Parallel record mismatch (%d bytes)\n got: %.300s\nwant: %s", len(got), got, want)
	}
	if !op.ContextDetails.ReplayChildren {
		t.Error("Parallel checkpoint is not marked for child replay")
	}
}

// TestBatchReplayReturnsFirstRunResult asserts that a replay of a stored
// full payload and a replay of a stored summary record each return the
// result the first run returned, the abandoned item included.
func TestBatchReplayReturnsFirstRunResult(t *testing.T) {
	for name, big := range map[string]string{
		"full payload":   "small",
		"summary record": strings.Repeat("x", 140*1024),
	} {
		t.Run(name, func(t *testing.T) {
			var views []string
			r, err := durabletest.NewLocalRunner(abandoningParallel(big, &views)).RunUntilComplete(nil)
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != durabletest.Succeeded {
				t.Fatalf("status = %s, want SUCCEEDED (error %+v)", r.Status, r.Error)
			}
			if got := r.Operation("p").ContextDetails.ReplayChildren; got != (big != "small") {
				t.Fatalf("ReplayChildren = %v, want %v", got, big != "small")
			}
			if len(views) != 2 {
				t.Fatalf("observed %d invocations after the Parallel, want 2", len(views))
			}
			if !strings.Contains(views[0], "|slow:STARTED") {
				t.Fatalf("first run = %s, want the slow branch abandoned", views[0])
			}
			if views[0] != views[1] {
				t.Errorf("replay differs from the first run\n first run: %s\n    replay: %s", views[0], views[1])
			}
		})
	}
}
