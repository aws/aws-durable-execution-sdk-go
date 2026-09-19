package durable

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// batchVerdict is the projection a test handler returns so a test can
// compare the batch outcome across live and replay invocations.
type batchVerdict struct {
	Err          bool   `json:"err"`
	IsBatchError bool   `json:"isBatchError"`
	Name         string `json:"name"`
	Reason       string `json:"reason"`
	ErrCount     int    `json:"errCount"`
	FirstErr     string `json:"firstErr"`
	Status       string `json:"status"`
	Success      int    `json:"successCount"`
	Failure      int    `json:"failureCount"`
	Total        int    `json:"totalCount"`
	Failed       int    `json:"failed"`
}

// verdictOf projects a Map/Parallel return value. An err that is not a
// *BatchError is propagated so the harness reports it as a failure.
func verdictOf[O any](br BatchResult[O], err error) (batchVerdict, error) {
	v := batchVerdict{
		Err:     err != nil,
		Status:  br.Status().String(),
		Success: br.SuccessCount(),
		Failure: br.FailureCount(),
		Total:   br.TotalCount(),
		Failed:  len(br.Failed()),
	}
	if err == nil {
		return v, nil
	}
	var berr *BatchError
	if !errors.As(err, &berr) {
		return batchVerdict{}, err
	}
	v.IsBatchError = true
	v.Name = berr.Name
	v.Reason = berr.Reason.String()
	v.ErrCount = len(berr.Errors)
	if len(berr.Errors) > 0 {
		v.FirstErr = berr.Errors[0].Error()
	}
	return v, nil
}

func runVerdict(t *testing.T, fake *fakeLambda, handler Handler[any, batchVerdict], ops ...wireOperation) batchVerdict {
	t.Helper()
	resp := invokeBatch(t, fake, batchPayload(`null`, ops...), handler)
	assertSucceeded(t, resp)
	var v batchVerdict
	if err := json.Unmarshal([]byte(resp.Result), &v); err != nil {
		t.Fatalf("unmarshal verdict: %v", err)
	}
	return v
}

// failOn returns a Map item function that fails the items in fails.
func failOn(fails ...string) func(Context, string, int) (string, error) {
	return func(_ Context, item string, _ int) (string, error) {
		for _, f := range fails {
			if item == f {
				return "", errors.New("item " + item + " failed")
			}
		}
		return item, nil
	}
}

// TestBatchDefaultIsFailFast asserts that with no WithCompletion the first
// item failure completes the batch with FAILURE_TOLERANCE_EXCEEDED, the
// items not yet started are omitted, and a *BatchError is returned. The
// former behaviour ran every item and reported success.
func TestBatchDefaultIsFailFast(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []BatchOption
	}{
		{"no WithCompletion", nil},
		{"zero CompletionConfig", []BatchOption{WithCompletion(CompletionConfig{})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := runVerdict(t, &fakeLambda{}, func(ctx Context, _ any) (batchVerdict, error) {
				opts := append([]BatchOption{WithMaxConcurrency(1)}, tc.opts...)
				return verdictOf(Map(ctx, "default", []string{"ok", "fail", "never"}, failOn("fail"), opts...))
			})
			if !v.IsBatchError || v.Reason != "FAILURE_TOLERANCE_EXCEEDED" {
				t.Fatalf("verdict = %+v, want *BatchError with FAILURE_TOLERANCE_EXCEEDED", v)
			}
			if v.Total != 2 || v.Success != 1 || v.Failure != 1 {
				t.Errorf("counts = total %d success %d failure %d, want 2/1/1 (item 'never' omitted)", v.Total, v.Success, v.Failure)
			}
			if v.Name != "default" || v.ErrCount != 1 || v.FirstErr == "" {
				t.Errorf("BatchError = name %q errors %d first %q", v.Name, v.ErrCount, v.FirstErr)
			}
		})
	}
}

// TestParallelDefaultIsFailFast asserts the same default for Parallel.
func TestParallelDefaultIsFailFast(t *testing.T) {
	v := runVerdict(t, &fakeLambda{}, func(ctx Context, _ any) (batchVerdict, error) {
		return verdictOf(Parallel(ctx, "default", []Branch[string]{
			{Func: func(Context) (string, error) { return "", errors.New("boom") }},
			{Func: func(Context) (string, error) { return "never", nil }},
		}, WithMaxConcurrency(1)))
	})
	if !v.IsBatchError || v.Reason != "FAILURE_TOLERANCE_EXCEEDED" || v.Total != 1 {
		t.Fatalf("verdict = %+v, want *BatchError, FAILURE_TOLERANCE_EXCEEDED, 1 started item", v)
	}
}

// TestBatchToleratedFailurePercentageZero asserts that an explicit zero
// percentage fails the batch on the first failure, unlike nil which leaves
// the threshold unset.
func TestBatchToleratedFailurePercentageZero(t *testing.T) {
	v := runVerdict(t, &fakeLambda{}, func(ctx Context, _ any) (batchVerdict, error) {
		return verdictOf(Map(ctx, "pct0", []string{"ok", "fail", "never"}, failOn("fail"),
			WithMaxConcurrency(1), WithCompletion(CompletionConfig{ToleratedFailurePercentage: aws.Int(0)})))
	})
	if !v.IsBatchError || v.Reason != "FAILURE_TOLERANCE_EXCEEDED" || v.Total != 2 {
		t.Fatalf("verdict = %+v, want breach after the first failure with item 'never' omitted", v)
	}
}

// TestBatchToleratedFailurePercentageExact asserts the percentage
// comparison is exact: 1 of 3 failures is 33.3%, which exceeds 33.
func TestBatchToleratedFailurePercentageExact(t *testing.T) {
	v := runVerdict(t, &fakeLambda{}, func(ctx Context, _ any) (batchVerdict, error) {
		return verdictOf(Map(ctx, "pct33", []string{"fail", "ok", "ok2"}, failOn("fail"),
			WithMaxConcurrency(1), WithCompletion(CompletionConfig{ToleratedFailurePercentage: aws.Int(33)})))
	})
	if !v.IsBatchError || v.Reason != "FAILURE_TOLERANCE_EXCEEDED" || v.Total != 1 {
		t.Fatalf("verdict = %+v, want breach: 1 of 3 (33.3%%) exceeds 33", v)
	}

	// The boundary itself does not breach: 1 of 4 is exactly 25.
	v = runVerdict(t, &fakeLambda{}, func(ctx Context, _ any) (batchVerdict, error) {
		return verdictOf(Map(ctx, "pct25", []string{"fail", "ok", "ok2", "ok3"}, failOn("fail"),
			WithMaxConcurrency(1), WithCompletion(CompletionConfig{ToleratedFailurePercentage: aws.Int(25)})))
	})
	if v.Reason != "ALL_COMPLETED" || v.Total != 4 {
		t.Fatalf("verdict = %+v, want ALL_COMPLETED over 4 items: 1 of 4 (25%%) does not exceed 25", v)
	}
}

func TestShouldStopFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     CompletionConfig
		failed  int
		total   int
		wantHit bool
	}{
		{"default first failure", CompletionConfig{}, 1, 3, true},
		{"default no failure", CompletionConfig{}, 0, 3, false},
		{"min only tolerates all", CompletionConfig{MinSuccessful: 1}, 3, 3, false},
		{"count 0 first failure", CompletionConfig{ToleratedFailureCount: aws.Int(0)}, 1, 3, true},
		{"count 1 within", CompletionConfig{ToleratedFailureCount: aws.Int(1)}, 1, 3, false},
		{"count 1 exceeded", CompletionConfig{ToleratedFailureCount: aws.Int(1)}, 2, 3, true},
		{"pct 0 first failure", CompletionConfig{ToleratedFailurePercentage: aws.Int(0)}, 1, 3, true},
		{"pct 33 one of three", CompletionConfig{ToleratedFailurePercentage: aws.Int(33)}, 1, 3, true},
		{"pct 34 one of three", CompletionConfig{ToleratedFailurePercentage: aws.Int(34)}, 1, 3, false},
		{"pct 25 one of four", CompletionConfig{ToleratedFailurePercentage: aws.Int(25)}, 1, 4, false},
		{"pct 25 two of four", CompletionConfig{ToleratedFailurePercentage: aws.Int(25)}, 2, 4, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldStopFailure(tc.cfg, tc.failed, tc.total); got != tc.wantHit {
				t.Errorf("shouldStopFailure(%+v, %d, %d) = %v, want %v", tc.cfg, tc.failed, tc.total, got, tc.wantHit)
			}
		})
	}
}

// TestBatchErrorAlongsideResult asserts that a breach returns both the
// *BatchError and the populated result, and that the error matches
// *OperationError.
func TestBatchErrorAlongsideResult(t *testing.T) {
	type out struct {
		Failed  []string `json:"failed"`
		Results []string `json:"results"`
		OpName  string   `json:"opName"`
	}
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (out, error) {
		br, err := Map(ctx, "reserve", []string{"ok", "fail"}, failOn("fail"), WithMaxConcurrency(1))
		var berr *BatchError
		if !errors.As(err, &berr) {
			return out{}, errors.New("want *BatchError, got " + err.Error())
		}
		var opErr *OperationError
		if !errors.As(err, &opErr) {
			return out{}, errors.New("*BatchError does not match *OperationError")
		}
		o := out{Results: br.Results(), OpName: opErr.Name}
		for _, item := range br.Failed() {
			o.Failed = append(o.Failed, item.Err.Error())
		}
		return o, nil
	})
	assertSucceeded(t, resp)
	var o out
	if err := json.Unmarshal([]byte(resp.Result), &o); err != nil {
		t.Fatal(err)
	}
	if len(o.Results) != 1 || o.Results[0] != "ok" {
		t.Errorf("Results() = %v, want [ok]: the result must be populated alongside the error", o.Results)
	}
	if len(o.Failed) != 1 || !strings.Contains(o.Failed[0], "item fail failed") {
		t.Errorf("Failed() = %v, want the failed item with its error", o.Failed)
	}
	if o.OpName != "reserve" {
		t.Errorf("OperationError.Name = %q, want %q", o.OpName, "reserve")
	}
}

// TestBatchErrorUnwrapReachesItemErrors asserts errors.Is and errors.As
// reach the per-item errors through Unwrap() []error.
func TestBatchErrorUnwrapReachesItemErrors(t *testing.T) {
	sentinel := errors.New("sentinel")

	t.Run("flat items keep the item error", func(t *testing.T) {
		v := runVerdict(t, &fakeLambda{}, func(ctx Context, _ any) (batchVerdict, error) {
			_, err := Map(ctx, "flat", []string{"a", "b"}, func(_ Context, item string, _ int) (string, error) {
				if item == "b" {
					return "", sentinel
				}
				return item, nil
			}, WithMaxConcurrency(1), WithNesting(NestingFlat), WithCompletion(CompletionConfig{ToleratedFailureCount: aws.Int(1)}))
			if !errors.Is(err, sentinel) {
				return batchVerdict{}, errors.New("errors.Is(err, sentinel) = false")
			}
			return batchVerdict{Err: true}, nil
		})
		if !v.Err {
			t.Fatal("unexpected verdict")
		}
	})

	t.Run("nested items expose ChildContextError", func(t *testing.T) {
		v := runVerdict(t, &fakeLambda{}, func(ctx Context, _ any) (batchVerdict, error) {
			_, err := Map(ctx, "nested", []string{"a", "b"}, failOn("b"), WithMaxConcurrency(1))
			var childErr *ChildContextError
			if !errors.As(err, &childErr) {
				return batchVerdict{}, errors.New("errors.As(err, *ChildContextError) = false")
			}
			if childErr.Message != "item b failed" {
				return batchVerdict{}, errors.New("unexpected item error: " + childErr.Message)
			}
			return batchVerdict{Err: true}, nil
		})
		if !v.Err {
			t.Fatal("unexpected verdict")
		}
	})

	t.Run("direct value", func(t *testing.T) {
		berr := &BatchError{Name: "b", Reason: CompletionAllCompleted, Errors: []error{errors.New("x"), sentinel}}
		if !errors.Is(berr, sentinel) {
			t.Error("errors.Is through Unwrap() []error = false")
		}
		if got := berr.Error(); !strings.Contains(got, `batch "b" failed`) || !strings.Contains(got, "ALL_COMPLETED") || !strings.Contains(got, "2 items failed") || !strings.HasSuffix(got, "first: x") {
			t.Errorf("Error() = %q", got)
		}
		one := &BatchError{Name: "b", Reason: CompletionFailureToleranceExceeded, Errors: []error{sentinel}}
		if got := one.Error(); !strings.Contains(got, "1 item failed") {
			t.Errorf("Error() = %q, want singular noun", got)
		}
	})
}

// TestBatchErrorWireType asserts the wire-error mapper stamps the public
// type name and a rebuilt value recovers its reason, for every defined
// completion reason.
func TestBatchErrorWireType(t *testing.T) {
	reasons := []CompletionReason{
		CompletionAllCompleted,
		CompletionMinSuccessfulReached,
		CompletionFailureToleranceExceeded,
		CompletionCustomSucceeded,
		CompletionCustomFailed,
	}
	for _, reason := range reasons {
		t.Run(reason.String(), func(t *testing.T) {
			orig := &BatchError{Name: "b", Reason: reason, Errors: []error{errors.New("x")}}
			we := errorObject(orig)
			if aws.ToString(we.ErrorType) != "BatchError" {
				t.Errorf("ErrorType = %q, want %q", aws.ToString(we.ErrorType), "BatchError")
			}
			rebuilt := ErrorFromObject(we)
			var berr *BatchError
			if !errors.As(rebuilt, &berr) {
				t.Fatalf("rebuilt = %T, want *BatchError", rebuilt)
			}
			if berr.Reason != reason || berr.Error() != orig.Error() {
				t.Errorf("rebuilt = %+v (%q), want reason %s and message preserved", berr, berr.Error(), reason)
			}
		})
	}
}

// TestCompletionReasonOf asserts the recorded-message parser recovers each
// defined reason from a BatchError message and returns zero otherwise.
func TestCompletionReasonOf(t *testing.T) {
	for _, reason := range []CompletionReason{
		CompletionAllCompleted,
		CompletionMinSuccessfulReached,
		CompletionFailureToleranceExceeded,
		CompletionCustomSucceeded,
		CompletionCustomFailed,
	} {
		msg := (&BatchError{Name: "b", Reason: reason}).Error()
		if got := completionReasonOf(msg); got != reason {
			t.Errorf("completionReasonOf(%q) = %v, want %v", msg, got, reason)
		}
	}
	if got := completionReasonOf("durable: batch \"b\" failed: no reason here"); got != 0 {
		t.Errorf("completionReasonOf(no reason) = %v, want 0", got)
	}
	if got := completionReasonOf((&BatchError{Name: "b", Reason: CompletionReason(99)}).Error()); got != 0 {
		t.Errorf("completionReasonOf(UNKNOWN reason) = %v, want 0", got)
	}
}

// TestBatchErrorNestedReasonRebuild asserts that a rebuilt outer BatchError
// keeps its own reason when its first failed item is a BatchError with a
// different reason, and when the batch name itself spells a reason.
func TestBatchErrorNestedReasonRebuild(t *testing.T) {
	inner := &BatchError{Name: "inner", Reason: CompletionFailureToleranceExceeded, Errors: []error{errors.New("x")}}
	tests := []struct {
		name string
		orig *BatchError
	}{
		{"custom-failed outer over tolerance-exceeded inner", &BatchError{Name: "outer", Reason: CompletionCustomFailed, Errors: []error{inner}}},
		{"all-completed outer over tolerance-exceeded inner", &BatchError{Name: "outer", Reason: CompletionAllCompleted, Errors: []error{inner}}},
		{"name spells another reason", &BatchError{Name: "FAILURE_TOLERANCE_EXCEEDED", Reason: CompletionCustomFailed, Errors: []error{errors.New("x")}}},
		{"name holds a quote and a colon", &BatchError{Name: `say "hi": now`, Reason: CompletionMinSuccessfulReached, Errors: []error{inner}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := completionReasonOf(tt.orig.Error()); got != tt.orig.Reason {
				t.Errorf("completionReasonOf(%q) = %v, want %v", tt.orig.Error(), got, tt.orig.Reason)
			}
			rebuilt := ErrorFromObject(errorObject(tt.orig))
			var berr *BatchError
			if !errors.As(rebuilt, &berr) {
				t.Fatalf("rebuilt = %T, want *BatchError", rebuilt)
			}
			if berr.Reason != tt.orig.Reason || berr.Error() != tt.orig.Error() {
				t.Errorf("rebuilt reason = %v (%q), want %v with message preserved", berr.Reason, berr.Error(), tt.orig.Reason)
			}
		})
	}
	// A message of another shape falls back to the earliest reason named.
	if got := completionReasonOf("batch failed: CUSTOM_COMPLETION_FAILED after ALL_COMPLETED"); got != CompletionCustomFailed {
		t.Errorf("fallback reason = %v, want %v", got, CompletionCustomFailed)
	}
}

// TestBatchMinSuccessfulReachedIsNotAnError asserts that reaching
// MinSuccessful early returns err == nil with unstarted items omitted.
func TestBatchMinSuccessfulReachedIsNotAnError(t *testing.T) {
	v := runVerdict(t, &fakeLambda{}, func(ctx Context, _ any) (batchVerdict, error) {
		return verdictOf(Map(ctx, "min", []string{"a", "b", "c", "d"}, failOn(),
			WithMaxConcurrency(1), WithCompletion(CompletionConfig{MinSuccessful: 2})))
	})
	if v.Err || v.Reason != "" || v.Total != 2 || v.Success != 2 {
		t.Fatalf("verdict = %+v, want err == nil with 2 started items", v)
	}
}

// TestBatchMinSuccessfulNotReachedIsBatchError asserts that a batch whose
// MinSuccessful threshold is never met returns *BatchError once every item
// has run.
func TestBatchMinSuccessfulNotReachedIsBatchError(t *testing.T) {
	v := runVerdict(t, &fakeLambda{}, func(ctx Context, _ any) (batchVerdict, error) {
		return verdictOf(Map(ctx, "min-not-reached", []string{"ok0", "fail", "ok2"}, failOn("fail"),
			WithMaxConcurrency(1), WithCompletion(CompletionConfig{MinSuccessful: 3})))
	})
	if !v.IsBatchError || v.Reason != "ALL_COMPLETED" || v.Total != 3 || v.Success != 2 || v.Failure != 1 {
		t.Fatalf("verdict = %+v, want *BatchError with ALL_COMPLETED over all 3 items", v)
	}
}

// TestBatchToleratedFailureStillReturnsBatchError asserts that a failure
// within a configured tolerance runs the batch to completion and still
// returns *BatchError, whose Reason distinguishes it from a breach.
func TestBatchToleratedFailureStillReturnsBatchError(t *testing.T) {
	v := runVerdict(t, &fakeLambda{}, func(ctx Context, _ any) (batchVerdict, error) {
		return verdictOf(Map(ctx, "tolerated", []string{"ok", "fail", "ok2"}, failOn("fail"),
			WithMaxConcurrency(1), WithCompletion(CompletionConfig{ToleratedFailureCount: aws.Int(1)})))
	})
	if !v.IsBatchError || v.Reason != "ALL_COMPLETED" || v.Total != 3 || v.Status != "FAILED" {
		t.Fatalf("verdict = %+v, want *BatchError with ALL_COMPLETED over all 3 items", v)
	}
}

// TestBatchErrorReplayMatchesLive asserts that replaying a breached batch
// from its checkpoint reproduces the same *BatchError and result shape.
func TestBatchErrorReplayMatchesLive(t *testing.T) {
	handler := func(ctx Context, _ any) (batchVerdict, error) {
		return verdictOf(Map(ctx, "replayed", []string{"ok", "fail", "never"}, failOn("fail"), WithMaxConcurrency(1)))
	}

	fake := &fakeLambda{}
	live := runVerdict(t, fake, handler)
	if !live.IsBatchError || live.Reason != "FAILURE_TOLERANCE_EXCEEDED" {
		t.Fatalf("live verdict = %+v, want a breach", live)
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

	replay := runVerdict(t, &fakeLambda{}, handler, wireOperation{
		Id:             hashID("1"),
		Status:         "SUCCEEDED",
		ContextDetails: &wireContextDetails{Result: mapPayload},
	})
	if replay != live {
		t.Errorf("replay verdict = %+v, live = %+v: the *BatchError diverged across replay", replay, live)
	}
}

// TestBatchErrorSurfacesOnResultSerdesReplay asserts that a batch replayed
// through a custom result serdes that drops item errors still returns a
// *BatchError, derived from the items' statuses and the reason.
func TestBatchErrorSurfacesOnResultSerdesReplay(t *testing.T) {
	handler := func(ctx Context, _ any) (batchVerdict, error) {
		return verdictOf(Map(ctx, "proj-serde", []string{"ok", "bad"}, failOn("bad"),
			WithMaxConcurrency(1), WithBatchResultSerdes(jsonProjectionSerdes{})))
	}

	fake := &fakeLambda{}
	live := runVerdict(t, fake, handler)
	if !live.IsBatchError || live.ErrCount != 1 {
		t.Fatalf("live verdict = %+v, want *BatchError carrying the item error", live)
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

	replay := runVerdict(t, &fakeLambda{}, handler, wireOperation{
		Id:             hashID("1"),
		Status:         "SUCCEEDED",
		ContextDetails: &wireContextDetails{Result: mapPayload},
	})
	if !replay.IsBatchError || replay.Reason != live.Reason || replay.Status != "FAILED" {
		t.Fatalf("replay verdict = %+v, want *BatchError with reason %s", replay, live.Reason)
	}
	if replay.ErrCount != 0 || replay.Failed != 1 {
		t.Errorf("replay verdict = %+v, want the failed item without its error (dropped by the serdes)", replay)
	}
}
