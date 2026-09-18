package durable

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// customResult is the handler output the custom-completion tests report.
type customResult struct {
	Reason  string   `json:"completionReason"`
	Status  string   `json:"status"`
	Success int      `json:"successCount"`
	Failure int      `json:"failureCount"`
	Started int      `json:"startedCount"`
	Total   int      `json:"totalCount"`
	Results []string `json:"results"`
	Err     string   `json:"err"`
}

func customResultOf[O any](br BatchResult[O], err error, results []string) customResult {
	r := customResult{
		Reason:  br.Reason.String(),
		Status:  br.Status().String(),
		Success: br.SuccessCount(),
		Failure: br.FailureCount(),
		Started: br.StartedCount(),
		Total:   br.TotalCount(),
		Results: results,
	}
	if err != nil {
		r.Err = err.Error()
	}
	return r
}

func decodeCustomResult(t *testing.T, resp batchResp) customResult {
	t.Helper()
	assertSucceeded(t, resp)
	var r customResult
	if err := json.Unmarshal([]byte(resp.Result), &r); err != nil {
		t.Fatalf("unmarshal result %s: %v", resp.Result, err)
	}
	return r
}

// TestCustomCompletionSucceedsEarly: the callback completes the batch as
// succeeded once two named items have succeeded. The remaining item never
// starts, so it is omitted from the result.
func TestCustomCompletionSucceedsEarly(t *testing.T) {
	var snapshots []BatchProgress
	resp := invokeBatch(t, &fakeLambda{}, batchPayload(`null`), func(ctx Context, _ any) (customResult, error) {
		items := []string{"a", "b", "c", "d"}
		br, err := Map(ctx, "quorum", items, func(_ Context, item string, _ int) (string, error) {
			return strings.ToUpper(item), nil
		},
			WithMaxConcurrency(1),
			WithItemNamer(func(i int) string { return items[i] }),
			WithCompletion(CompletionConfig{ShouldComplete: func(p BatchProgress) CompletionDecision {
				snapshots = append(snapshots, p)
				if p.Items[0].Status == BatchItemSucceeded && p.Items[1].Status == BatchItemSucceeded {
					return CompleteBatch(CompletionOutcomeSucceeded)
				}
				return ContinueBatch()
			}}))
		if err != nil {
			return customResult{}, err
		}
		return customResultOf(br, nil, br.Results()), nil
	})
	r := decodeCustomResult(t, resp)

	if r.Reason != "CUSTOM_COMPLETION_SUCCEEDED" || r.Status != "SUCCEEDED" {
		t.Errorf("reason/status = %s/%s, want CUSTOM_COMPLETION_SUCCEEDED/SUCCEEDED", r.Reason, r.Status)
	}
	if r.Success != 2 || r.Total != 2 || r.Started != 0 {
		t.Errorf("success/total/started = %d/%d/%d, want 2/2/0", r.Success, r.Total, r.Started)
	}
	if want := []string{"A", "B"}; !reflect.DeepEqual(r.Results, want) {
		t.Errorf("results = %v, want %v", r.Results, want)
	}

	// The callback ran once per terminal item and stopped being called
	// once it completed the batch.
	if len(snapshots) != 2 {
		t.Fatalf("callback invocations = %d, want 2", len(snapshots))
	}
	first := snapshots[0]
	if first.TotalCount != 4 || first.CompletedCount != 1 || first.SuccessCount != 1 || first.FailureCount != 0 {
		t.Errorf("first snapshot counts = %+v", first)
	}
	wantItems := []BatchItemProgress{
		{Index: 0, Name: "a", Status: BatchItemSucceeded},
		{Index: 1, Name: "b", Status: BatchItemNotStarted},
		{Index: 2, Name: "c", Status: BatchItemNotStarted},
		{Index: 3, Name: "d", Status: BatchItemNotStarted},
	}
	if !reflect.DeepEqual(first.Items, wantItems) {
		t.Errorf("first snapshot items = %+v, want %+v", first.Items, wantItems)
	}
	second := snapshots[1]
	if second.CompletedCount != 2 || second.Items[1].Status != BatchItemSucceeded {
		t.Errorf("second snapshot = %+v", second)
	}
}

// TestCustomCompletionFailsEarly: the callback completes the batch as
// failed although no item failed. The batch's Status is FAILED and Map
// returns a BatchError with no item errors.
func TestCustomCompletionFailsEarly(t *testing.T) {
	resp := invokeBatch(t, &fakeLambda{}, batchPayload(`null`), func(ctx Context, _ any) (customResult, error) {
		items := []int{1, 2, 3}
		br, err := Map(ctx, "veto", items, func(_ Context, item int, _ int) (int, error) {
			return item, nil
		},
			WithMaxConcurrency(1),
			WithCompletion(CompletionConfig{ShouldComplete: func(p BatchProgress) CompletionDecision {
				if p.CompletedCount >= 1 {
					return CompleteBatch(CompletionOutcomeFailed)
				}
				return ContinueBatch()
			}}))
		var berr *BatchError
		if !errors.As(err, &berr) {
			return customResult{}, err
		}
		if berr.Reason != CompletionCustomFailed || len(berr.Errors) != 0 {
			return customResult{}, errors.New("BatchError should carry CompletionCustomFailed and no item errors")
		}
		return customResultOf(br, err, nil), nil
	})
	r := decodeCustomResult(t, resp)

	if r.Reason != "CUSTOM_COMPLETION_FAILED" || r.Status != "FAILED" {
		t.Errorf("reason/status = %s/%s, want CUSTOM_COMPLETION_FAILED/FAILED", r.Reason, r.Status)
	}
	if r.Success != 1 || r.Failure != 0 || r.Total != 1 {
		t.Errorf("success/failure/total = %d/%d/%d, want 1/0/1", r.Success, r.Failure, r.Total)
	}
	if r.Err == "" {
		t.Error("Map returned nil error for a custom failed completion")
	}
}

// TestCustomCompletionContinuesToEnd: a callback that never completes the
// batch lets every item run, including after failures. No fail-fast
// applies, and the reason is ALL_COMPLETED.
func TestCustomCompletionContinuesToEnd(t *testing.T) {
	var calls int32
	resp := invokeBatch(t, &fakeLambda{}, batchPayload(`null`), func(ctx Context, _ any) (customResult, error) {
		items := []int{0, 1, 2, 3}
		br, err := Map(ctx, "all", items, func(_ Context, item int, _ int) (int, error) {
			if item%2 == 1 {
				return 0, errors.New("odd")
			}
			return item, nil
		},
			WithMaxConcurrency(1),
			WithCompletion(CompletionConfig{ShouldComplete: func(BatchProgress) CompletionDecision {
				atomic.AddInt32(&calls, 1)
				return ContinueBatch()
			}}))
		if err := batchOnly(err); err != nil {
			return customResult{}, err
		}
		return customResultOf(br, err, nil), nil
	})
	r := decodeCustomResult(t, resp)

	if r.Reason != "ALL_COMPLETED" || r.Status != "FAILED" {
		t.Errorf("reason/status = %s/%s, want ALL_COMPLETED/FAILED", r.Reason, r.Status)
	}
	if r.Success != 2 || r.Failure != 2 || r.Total != 4 {
		t.Errorf("success/failure/total = %d/%d/%d, want 2/2/4", r.Success, r.Failure, r.Total)
	}
	if got := atomic.LoadInt32(&calls); got != 4 {
		t.Errorf("callback invocations = %d, want 4 (once per terminal item)", got)
	}
}

// TestCustomCompletionAbandonsInFlight: in a concurrent batch the callback
// completes once branches 1 and 2 have succeeded. Branch 0 is still in
// flight and is reported STARTED; it is not awaited.
func TestCustomCompletionAbandonsInFlight(t *testing.T) {
	var inFlightSeen atomic.Bool
	resp := invokeBatch(t, &fakeLambda{}, batchPayload(`null`), func(ctx Context, _ any) (customResult, error) {
		slow := Branch[string]{Name: "A", Func: func(c Context) (string, error) {
			if _, err := Step(c, "a1", func(StepContext) (string, error) {
				time.Sleep(150 * time.Millisecond)
				return "a1", nil
			}); err != nil {
				return "", err
			}
			// Reached only if the branch was not abandoned.
			return Step(c, "a2", func(StepContext) (string, error) { return "A", nil })
		}}
		fast := func(name string, delay time.Duration) Branch[string] {
			return Branch[string]{Name: name, Func: func(c Context) (string, error) {
				return Step(c, name, func(StepContext) (string, error) {
					time.Sleep(delay)
					return name, nil
				})
			}}
		}
		br, err := Parallel(ctx, "quorum", []Branch[string]{slow, fast("B", 10*time.Millisecond), fast("C", 30*time.Millisecond)},
			WithCompletion(CompletionConfig{ShouldComplete: func(p BatchProgress) CompletionDecision {
				if p.Items[0].Status == BatchItemStarted {
					inFlightSeen.Store(true)
				}
				ok := func(i int) bool { return p.Items[i].Status == BatchItemSucceeded }
				if ok(0) || (ok(1) && ok(2)) {
					return CompleteBatch(CompletionOutcomeSucceeded)
				}
				return ContinueBatch()
			}}))
		if err != nil {
			return customResult{}, err
		}
		return customResultOf(br, nil, br.Results()), nil
	})
	r := decodeCustomResult(t, resp)

	if r.Reason != "CUSTOM_COMPLETION_SUCCEEDED" {
		t.Errorf("reason = %s, want CUSTOM_COMPLETION_SUCCEEDED", r.Reason)
	}
	if r.Success != 2 || r.Started != 1 || r.Total != 3 {
		t.Errorf("success/started/total = %d/%d/%d, want 2/1/3", r.Success, r.Started, r.Total)
	}
	if want := []string{"B", "C"}; !reflect.DeepEqual(r.Results, want) {
		t.Errorf("results = %v, want %v", r.Results, want)
	}
	if !inFlightSeen.Load() {
		t.Error("snapshot never reported the slow branch as BatchItemStarted")
	}
}

// TestCustomCompletionAbandonedBranchKeepsStartedCheckpoint: a branch whose
// body finishes after the batch completed is abandoned. Its child context
// was checkpointed STARTED when it was admitted and receives no SUCCEEDED
// checkpoint afterwards, so the log agrees with the batch result that
// reports it STARTED.
func TestCustomCompletionAbandonedBranchKeepsStartedCheckpoint(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (customResult, error) {
		sleeper := func(name string, delay time.Duration) Branch[string] {
			return Branch[string]{Name: name, Func: func(Context) (string, error) {
				time.Sleep(delay)
				return name, nil
			}}
		}
		br, err := Parallel(ctx, "quorum", []Branch[string]{
			sleeper("slow", 300*time.Millisecond),
			sleeper("fast", 10*time.Millisecond),
		}, WithCompletion(CompletionConfig{ShouldComplete: func(p BatchProgress) CompletionDecision {
			if p.Items[1].Status == BatchItemSucceeded {
				return CompleteBatch(CompletionOutcomeSucceeded)
			}
			return ContinueBatch()
		}}))
		if err != nil {
			return customResult{}, err
		}
		return customResultOf(br, nil, br.Results()), nil
	})
	r := decodeCustomResult(t, resp)
	if r.Success != 1 || r.Started != 1 || r.Total != 2 {
		t.Fatalf("success/started/total = %d/%d/%d, want 1/1/2", r.Success, r.Started, r.Total)
	}

	status := map[string]OperationAction{}
	for _, u := range updateBatch(t, fake) {
		if aws.ToString(u.SubType) != operationSubTypeParallelBranch {
			continue
		}
		status[aws.ToString(u.Name)] = u.Action
	}
	if got := status["slow"]; got != OperationActionStart {
		t.Errorf("abandoned branch's last checkpoint action = %s, want START", got)
	}
	if got := status["fast"]; got != OperationActionSucceed {
		t.Errorf("counted branch's last checkpoint action = %s, want SUCCEED", got)
	}
}

// TestCustomCompletionReplayDoesNotReinvokeCallback: replaying a batch
// whose checkpoint records a custom completion returns the same result
// and never calls the callback.
func TestCustomCompletionReplayDoesNotReinvokeCallback(t *testing.T) {
	var calls int32
	handler := func(ctx Context, _ any) (customResult, error) {
		items := []string{"a", "b", "c", "d"}
		br, err := Map(ctx, "early", items, func(_ Context, item string, _ int) (string, error) {
			if item == "b" {
				return "", errors.New("b failed")
			}
			return item, nil
		},
			WithMaxConcurrency(1),
			WithCompletion(CompletionConfig{ShouldComplete: func(p BatchProgress) CompletionDecision {
				atomic.AddInt32(&calls, 1)
				if p.FailureCount > 0 {
					return CompleteBatch(CompletionOutcomeFailed)
				}
				return ContinueBatch()
			}}))
		if err := batchOnly(err); err != nil {
			return customResult{}, err
		}
		return customResultOf(br, err, br.Results()), nil
	}

	fake := &fakeLambda{}
	live := decodeCustomResult(t, invokeBatch(t, fake, batchPayload(`null`), handler))
	if live.Reason != "CUSTOM_COMPLETION_FAILED" || live.Total != 2 || live.Failure != 1 {
		t.Fatalf("live result = %+v", live)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("live callback invocations = %d, want 2", got)
	}

	var mapPayload string
	for _, u := range updateBatch(t, fake) {
		if aws.ToString(u.SubType) == "Map" && u.Action == OperationActionSucceed {
			mapPayload = aws.ToString(u.Payload)
		}
	}
	if mapPayload == "" {
		t.Fatal("no Map SUCCEED payload found in checkpoint updates")
	}

	atomic.StoreInt32(&calls, 0)
	replayFake := &fakeLambda{}
	replay := decodeCustomResult(t, invokeBatch(t, replayFake, batchPayload(`null`, wireOperation{
		Id:             hashID("1"),
		Status:         "SUCCEEDED",
		ContextDetails: &wireContextDetails{Result: mapPayload},
	}), handler))

	if !reflect.DeepEqual(replay, live) {
		t.Errorf("replay result = %+v\nlive result   = %+v", replay, live)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Errorf("replay callback invocations = %d, want 0", got)
	}
	if len(replayFake.gotUpdateBatches) != 0 {
		t.Errorf("replay issued %d checkpoint batches, want 0", len(replayFake.gotUpdateBatches))
	}
}

// TestCustomCompletionFlatOversizedReplayUsesRecord: a NestingFlat batch
// whose aggregate result exceeds the checkpoint size limit stores a
// decision record instead of the results. Replay rebuilds the items from
// the operations recorded under the batch and takes the reason from the
// record, so the callback is not called again. The callback completes the
// batch as failed with no failed item; a replay that re-derived the reason
// from the outcomes would report ALL_COMPLETED instead.
func TestCustomCompletionFlatOversizedReplayUsesRecord(t *testing.T) {
	// Two results of this size exceed the aggregate limit while each step
	// payload stays under the per-operation limit.
	big := strings.Repeat("x", 170*1024)
	var calls int32
	handler := func(ctx Context, _ any) (customResult, error) {
		items := []int{0, 1, 2}
		br, err := Map(ctx, "flat", items, func(c Context, item int, _ int) (string, error) {
			return Step(c, "s", func(StepContext) (string, error) { return big, nil })
		},
			WithMaxConcurrency(1),
			WithNesting(NestingFlat),
			WithCompletion(CompletionConfig{ShouldComplete: func(p BatchProgress) CompletionDecision {
				atomic.AddInt32(&calls, 1)
				if p.SuccessCount >= 2 {
					return CompleteBatch(CompletionOutcomeFailed)
				}
				return ContinueBatch()
			}}))
		if err := batchOnly(err); err != nil {
			return customResult{}, err
		}
		// The results themselves are too large to return; report their
		// lengths so the handler result stays small.
		var lens []string
		for _, s := range br.Results() {
			lens = append(lens, fmt.Sprint(len(s)))
		}
		return customResultOf(br, err, lens), nil
	}

	fake := &fakeLambda{}
	live := decodeCustomResult(t, invokeBatch(t, fake, batchPayload(`null`), handler))
	if live.Reason != "CUSTOM_COMPLETION_FAILED" || live.Status != "FAILED" || live.Total != 2 || live.Success != 2 {
		t.Fatalf("live result = %+v", live)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("live callback invocations = %d, want 2", got)
	}

	// The parent Map SUCCEED carries ReplayChildren and a decision record
	// with the recorded reason and the admitted prefix.
	var sawParent bool
	for _, u := range updateBatch(t, fake) {
		if aws.ToString(u.SubType) != operationSubTypeMap || u.Action != OperationActionSucceed {
			continue
		}
		sawParent = true
		if u.ContextOptions == nil || !aws.ToBool(u.ContextOptions.ReplayChildren) {
			t.Fatal("Map SUCCEED did not set ReplayChildren; the aggregate did not exceed the size limit")
		}
		record, ok := parseBatchReplayRecord(aws.ToString(u.Payload))
		if !ok {
			t.Fatalf("Map SUCCEED payload %q is not a decision record", aws.ToString(u.Payload))
		}
		if record.Reason != CompletionCustomFailed || record.StartedTotal != 2 || len(record.abandonedSet()) != 0 {
			t.Fatalf("decision record = %+v, want CUSTOM_COMPLETION_FAILED over 2 terminal items", record)
		}
	}
	if !sawParent {
		t.Fatal("no Map SUCCEED update found")
	}

	atomic.StoreInt32(&calls, 0)
	replayFake := &fakeLambda{}
	replay := decodeCustomResult(t, invokeBatch(t, replayFake, batchPayload(`null`, replayOpsFromUpdates(updateBatch(t, fake))...), handler))

	if !reflect.DeepEqual(replay, live) {
		t.Errorf("replay result = %+v\nlive result   = %+v", replay, live)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Errorf("replay callback invocations = %d, want 0", got)
	}
	if len(replayFake.gotUpdateBatches) != 0 {
		t.Errorf("replay issued %d checkpoint batches, want 0", len(replayFake.gotUpdateBatches))
	}
}

// TestCustomCompletionRejectsThresholds: ShouldComplete together with a
// threshold is a configuration error reported before any item runs.
func TestCustomCompletionRejectsThresholds(t *testing.T) {
	always := func(BatchProgress) CompletionDecision { return ContinueBatch() }
	configs := map[string]CompletionConfig{
		"MinSuccessful":              {ShouldComplete: always, MinSuccessful: 1},
		"ToleratedFailureCount":      {ShouldComplete: always, ToleratedFailureCount: aws.Int(0)},
		"ToleratedFailurePercentage": {ShouldComplete: always, ToleratedFailurePercentage: aws.Int(0)},
	}
	for name, cfg := range configs {
		t.Run(name, func(t *testing.T) {
			var ran atomic.Bool
			var mapErr, parErr error
			fake := &fakeLambda{}
			resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (string, error) {
				_, mapErr = Map(ctx, "m", []int{1}, func(Context, int, int) (int, error) {
					ran.Store(true)
					return 0, nil
				}, WithCompletion(cfg))
				_, parErr = Parallel(ctx, "p", []Branch[int]{{Func: func(Context) (int, error) {
					ran.Store(true)
					return 0, nil
				}}}, WithCompletion(cfg))
				return "done", nil
			})
			assertSucceeded(t, resp)
			for op, err := range map[string]error{"Map": mapErr, "Parallel": parErr} {
				if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
					t.Errorf("%s error = %v, want mutual-exclusion error", op, err)
				}
				var berr *BatchError
				if errors.As(err, &berr) {
					t.Errorf("%s returned a BatchError for a configuration error", op)
				}
			}
			if ran.Load() {
				t.Error("an item ran despite the rejected configuration")
			}
			if len(fake.gotUpdateBatches) != 0 {
				t.Errorf("rejected batches issued %d checkpoint batches, want 0", len(fake.gotUpdateBatches))
			}
		})
	}
}

// TestCustomCompletionCallbackFailures: a callback that panics or returns
// a decision with an undefined outcome fails the batch operation with an
// error that is not a BatchError.
func TestCustomCompletionCallbackFailures(t *testing.T) {
	cases := map[string]struct {
		callback func(BatchProgress) CompletionDecision
		want     string
	}{
		"panic": {
			callback: func(BatchProgress) CompletionDecision { panic("boom") },
			want:     "ShouldComplete panicked: boom",
		},
		"invalid outcome": {
			callback: func(BatchProgress) CompletionDecision { return CompleteBatch(CompletionOutcome(9)) },
			want:     "invalid CompletionOutcome 9",
		},
		"zero decision": {
			callback: func(BatchProgress) CompletionDecision { return CompletionDecision{} },
			want:     "",
		},
	}
	for name, tc := range cases {
		for _, concurrency := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/concurrency=%d", name, concurrency), func(t *testing.T) {
				var got error
				resp := invokeBatch(t, &fakeLambda{}, batchPayload(`null`), func(ctx Context, _ any) (string, error) {
					_, got = Map(ctx, "m", []int{1, 2}, func(_ Context, item int, _ int) (int, error) {
						return item, nil
					}, WithMaxConcurrency(concurrency), WithCompletion(CompletionConfig{ShouldComplete: tc.callback}))
					return "done", nil
				})
				assertSucceeded(t, resp)
				if tc.want == "" {
					// The zero decision is ContinueBatch: no error.
					if got != nil {
						t.Errorf("concurrency %d: error = %v, want nil", concurrency, got)
					}
					return
				}
				if got == nil || !strings.Contains(got.Error(), tc.want) {
					t.Errorf("concurrency %d: error = %v, want containing %q", concurrency, got, tc.want)
				}
				var berr *BatchError
				if errors.As(got, &berr) {
					t.Errorf("concurrency %d: callback failure was returned as a BatchError", concurrency)
				}
			})
		}
	}
}

// TestCompletionDecisionAccessors pins the helper constructors.
func TestCompletionDecisionAccessors(t *testing.T) {
	if d := ContinueBatch(); d.Complete() || d.Outcome() != 0 {
		t.Errorf("ContinueBatch() = %+v", d)
	}
	if d := CompleteBatch(CompletionOutcomeFailed); !d.Complete() || d.Outcome() != CompletionOutcomeFailed {
		t.Errorf("CompleteBatch(Failed) = %+v", d)
	}
	for o, want := range map[CompletionOutcome]string{CompletionOutcomeSucceeded: "SUCCEEDED", CompletionOutcomeFailed: "FAILED", 0: "UNKNOWN"} {
		if got := o.String(); got != want {
			t.Errorf("CompletionOutcome(%d).String() = %q, want %q", o, got, want)
		}
	}
	if got := BatchItemNotStarted.String(); got != "NOT_STARTED" {
		t.Errorf("BatchItemNotStarted.String() = %q", got)
	}
}
