package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// batchResp is a simplified response for test assertions.
type batchResp struct {
	Status string
	Result string
	Error  *wireError
}

// invokeBatch runs handler through the full durable invocation path.
func invokeBatch[I, O any](t *testing.T, fake *fakeLambda, payload []byte, handler Handler[I, O]) batchResp {
	t.Helper()
	h := Wrap(handler, withLambdaAPI(fake))
	got, err := h(context.Background(), payload)
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	var resp invocationResponse
	if err := json.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	result := ""
	if resp.Result != nil {
		result = *resp.Result
	}
	return batchResp{Status: resp.Status, Result: result, Error: resp.Error}
}

// batchPayload builds an invocation payload with optional pre-checkpointed ops.
func batchPayload(event string, ops ...wireOperation) []byte {
	all := append([]wireOperation{{
		Id:               "exec-op",
		Status:           "STARTED",
		ExecutionDetails: &wireExecutionDetails{InputPayload: event},
	}}, ops...)
	in := invocationInput{
		DurableExecutionArn:   "arn:test",
		CheckpointToken:       "token-0",
		InitialExecutionState: initialExecutionState{Operations: all},
	}
	b, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}
	return b
}

// noItemSctx is the item serdes-context function for payload round-trip
// tests that do not run inside an execution: every item gets the zero
// [SerdesContext].
func noItemSctx(int) SerdesContext { return SerdesContext{} }

// batchOnly returns nil for a *BatchError and err otherwise. Tests that
// inspect the result of a batch with failed items use it to keep the
// result-populated path while still propagating SDK failures.
func batchOnly(err error) error {
	var berr *BatchError
	if errors.As(err, &berr) {
		return nil
	}
	return err
}

func assertSucceeded(t *testing.T, resp batchResp) {
	t.Helper()
	if resp.Status != "SUCCEEDED" {
		t.Fatalf("expected SUCCEEDED, got %s (error: %v)", resp.Status, resp.Error)
	}
}

func assertFailed(t *testing.T, resp batchResp) {
	t.Helper()
	if resp.Status != "FAILED" {
		t.Fatalf("expected FAILED, got %s (result: %s)", resp.Status, resp.Result)
	}
}

func TestMapBasicSequential(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`["World","Kiro"]`), func(ctx Context, items []string) ([]string, error) {
		result, err := Map(ctx, "map", items, func(childCtx Context, item string, _ int) (string, error) {
			return Step(childCtx, "", func(StepContext) (string, error) {
				return "Hello, " + item + "!", nil
			})
		}, WithMaxConcurrency(1))
		if err != nil {
			return nil, err
		}
		return result.Results(), nil
	})
	assertSucceeded(t, resp)
	if !strings.Contains(resp.Result, "Hello, World!") || !strings.Contains(resp.Result, "Hello, Kiro!") {
		t.Fatalf("expected greetings in result: %s", resp.Result)
	}

	// Verify checkpoint updates include Map context and iterations.
	updates := updateBatch(t, fake)
	found := map[string]int{}
	for _, u := range updates {
		found[aws.ToString(u.SubType)]++
	}
	if found["Map"] < 2 {
		t.Errorf("expected at least 2 Map updates, got %d", found["Map"])
	}
	if found["MapIteration"] < 4 {
		t.Errorf("expected at least 4 MapIteration updates, got %d", found["MapIteration"])
	}
}

func TestMapEmptyItems(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`[]`), func(ctx Context, items []string) ([]string, error) {
		result, err := Map(ctx, "empty", items, func(_ Context, _ string, _ int) (string, error) {
			t.Fatal("should not be called for empty items")
			return "", nil
		})
		if err != nil {
			return nil, err
		}
		return result.Results(), nil
	})
	assertSucceeded(t, resp)
}

func TestMapItemAndIndex(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`[10,20,30]`), func(ctx Context, items []int) ([]int, error) {
		result, err := Map(ctx, "indexed", items, func(_ Context, item int, index int) (int, error) {
			return item + index, nil
		}, WithMaxConcurrency(1))
		if err != nil {
			return nil, err
		}
		return result.Results(), nil
	})
	assertSucceeded(t, resp)
	if !strings.Contains(resp.Result, "10") || !strings.Contains(resp.Result, "21") || !strings.Contains(resp.Result, "32") {
		t.Fatalf("expected indexed results in %s", resp.Result)
	}
}

func TestMapFailFast(t *testing.T) {
	fake := &fakeLambda{}
	type result struct {
		Reason  string `json:"completionReason"`
		Status  string `json:"status"`
		Success int    `json:"successCount"`
		Failure int    `json:"failureCount"`
		Total   int    `json:"totalCount"`
	}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (result, error) {
		items := []string{"ok", "fail", "never"}
		br, err := Map(ctx, "failfast", items, func(_ Context, item string, _ int) (string, error) {
			if item == "fail" {
				return "", errors.New("item failed")
			}
			return item, nil
		}, WithMaxConcurrency(1), WithCompletion(CompletionConfig{ToleratedFailureCount: aws.Int(0)}))
		if err := batchOnly(err); err != nil {
			return result{}, err
		}
		return result{
			Reason:  br.Reason.String(),
			Status:  br.Status().String(),
			Success: br.SuccessCount(),
			Failure: br.FailureCount(),
			Total:   br.TotalCount(),
		}, nil
	})
	assertSucceeded(t, resp)
	if !strings.Contains(resp.Result, "FAILURE_TOLERANCE_EXCEEDED") {
		t.Fatalf("expected FAILURE_TOLERANCE_EXCEEDED in %s", resp.Result)
	}
}

func TestMapMinSuccessful(t *testing.T) {
	fake := &fakeLambda{}
	type result struct {
		Reason  string `json:"completionReason"`
		Success int    `json:"successCount"`
		Total   int    `json:"totalCount"`
	}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (result, error) {
		items := []string{"a", "b", "c", "d"}
		br, err := Map(ctx, "min-successful", items, func(_ Context, item string, _ int) (string, error) {
			return item, nil
		}, WithMaxConcurrency(1), WithCompletion(CompletionConfig{MinSuccessful: 2}))
		if err != nil {
			return result{}, err
		}
		return result{
			Reason:  br.Reason.String(),
			Success: br.SuccessCount(),
			Total:   br.TotalCount(),
		}, nil
	})
	assertSucceeded(t, resp)
	if !strings.Contains(resp.Result, "MIN_SUCCESSFUL_REACHED") {
		t.Fatalf("expected MIN_SUCCESSFUL_REACHED in %s", resp.Result)
	}
}

func TestMapToleratedPercentage(t *testing.T) {
	fake := &fakeLambda{}
	type result struct {
		Reason string `json:"completionReason"`
	}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (result, error) {
		items := []int{0, 1, 2, 3}
		br, err := Map(ctx, "pct", items, func(_ Context, item int, _ int) (int, error) {
			if item < 2 {
				return 0, errors.New("fail")
			}
			return item, nil
		}, WithMaxConcurrency(1), WithCompletion(CompletionConfig{ToleratedFailurePercentage: aws.Int(25)}))
		if err := batchOnly(err); err != nil {
			return result{}, err
		}
		return result{Reason: br.Reason.String()}, nil
	})
	assertSucceeded(t, resp)
	if !strings.Contains(resp.Result, "FAILURE_TOLERANCE_EXCEEDED") {
		t.Fatalf("expected FAILURE_TOLERANCE_EXCEEDED in %s", resp.Result)
	}
}

func TestMapConcurrentPreservesOrder(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) ([]string, error) {
		items := []string{"r0", "r1", "r2"}
		br, err := Map(ctx, "concurrent", items, func(_ Context, item string, _ int) (string, error) {
			return item, nil
		}, WithMaxConcurrency(2))
		if err != nil {
			return nil, err
		}
		return br.Results(), nil
	})
	assertSucceeded(t, resp)
	// Results must be in index order.
	var results []string
	if err := json.Unmarshal([]byte(resp.Result), &results); err != nil {
		t.Fatalf("unmarshal results: %v", err)
	}
	if len(results) != 3 || results[0] != "r0" || results[1] != "r1" || results[2] != "r2" {
		t.Fatalf("expected [r0,r1,r2], got %v", results)
	}
}

func TestMapErr(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (string, error) {
		items := []string{"fail", "never"}
		br, err := Map(ctx, "throwing", items, func(_ Context, item string, _ int) (string, error) {
			if item == "fail" {
				return "", errors.New("item failed")
			}
			return item, nil
		}, WithMaxConcurrency(1), WithCompletion(CompletionConfig{ToleratedFailureCount: aws.Int(0)}))
		if err != nil {
			return "", err
		}
		return "should not reach: " + br.Reason.String(), nil
	})
	assertFailed(t, resp)
}

func TestMapFlatNesting(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) ([]string, error) {
		items := []string{"fa", "fb"}
		br, err := Map(ctx, "flat", items, func(childCtx Context, item string, _ int) (string, error) {
			return Step(childCtx, "", func(StepContext) (string, error) {
				return item, nil
			})
		}, WithMaxConcurrency(1), WithNesting(NestingFlat))
		if err != nil {
			return nil, err
		}
		return br.Results(), nil
	})
	assertSucceeded(t, resp)
	if !strings.Contains(resp.Result, "fa") || !strings.Contains(resp.Result, "fb") {
		t.Fatalf("expected flat results in %s", resp.Result)
	}

	// Verify NO MapIteration context events in FLAT mode.
	updates := updateBatch(t, fake)
	for _, u := range updates {
		if aws.ToString(u.SubType) == "MapIteration" {
			t.Fatal("FLAT mode should not produce MapIteration context events")
		}
	}
}

func TestMapItemNamer(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`[1,2]`), func(ctx Context, items []int) ([]int, error) {
		br, err := Map(ctx, "named-items", items, func(_ Context, item int, _ int) (int, error) {
			return item * 10, nil
		}, WithMaxConcurrency(1), WithItemNamer(func(i int) string {
			return fmt.Sprintf("item-%d", items[i])
		}))
		if err != nil {
			return nil, err
		}
		return br.Results(), nil
	})
	assertSucceeded(t, resp)
	if !strings.Contains(resp.Result, "10") || !strings.Contains(resp.Result, "20") {
		t.Fatalf("expected namer results in %s", resp.Result)
	}
}

// TestMapItemNamerIndexPosition verifies the index the namer receives
// corresponds to the named item's position in the input slice: each
// reported item's Name embeds both its Index and the input value at that
// index.
func TestMapItemNamerIndexPosition(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`[10,20,30]`), func(ctx Context, items []int) ([]string, error) {
		br, err := Map(ctx, "indexed-names", items, func(_ Context, item int, _ int) (int, error) {
			return item, nil
		}, WithMaxConcurrency(1), WithItemNamer(func(i int) string {
			return fmt.Sprintf("name-%d-%d", i, items[i])
		}))
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(br.Items))
		for _, it := range br.Items {
			want := fmt.Sprintf("name-%d-%d", it.Index, items[it.Index])
			if it.Name != want {
				return nil, fmt.Errorf("item at index %d named %q, want %q", it.Index, it.Name, want)
			}
			names = append(names, it.Name)
		}
		return names, nil
	})
	assertSucceeded(t, resp)
	for _, want := range []string{"name-0-10", "name-1-20", "name-2-30"} {
		if !strings.Contains(resp.Result, want) {
			t.Fatalf("expected %s in %s", want, resp.Result)
		}
	}
}

// TestMapFuncClosesOverItems covers the documented idiom for reaching the
// source collection from fn: fn receives the item and its index only, and
// closes over the input slice to read the rest of the collection. Each
// item's result is its difference from the previous item, so every result
// after the first depends on a neighbour reached through the closure. The
// batch is then replayed from its checkpoint and must return the same
// results without running fn again.
func TestMapFuncClosesOverItems(t *testing.T) {
	var calls atomic.Int32
	handler := func(ctx Context, readings []int) ([]int, error) {
		br, err := Map(ctx, "diffs", readings, func(_ Context, r int, i int) (int, error) {
			calls.Add(1)
			if i == 0 {
				return 0, nil
			}
			return r - readings[i-1], nil
		}, WithItemNamer(func(i int) string {
			return fmt.Sprintf("reading-%d", readings[i])
		}))
		if err != nil {
			return nil, err
		}
		for _, it := range br.Items {
			if want := fmt.Sprintf("reading-%d", readings[it.Index]); it.Name != want {
				return nil, fmt.Errorf("item %d named %q, want %q", it.Index, it.Name, want)
			}
		}
		return br.Results(), nil
	}

	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`[10,25,45,50]`), handler)
	assertSucceeded(t, resp)
	if want := "[0,15,20,5]"; resp.Result != want {
		t.Fatalf("live result = %s, want %s", resp.Result, want)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("fn ran %d times live, want 4", got)
	}
	calls.Store(0)

	var mapPayload string
	for _, u := range updateBatch(t, fake) {
		if aws.ToString(u.SubType) == "Map" && u.Action == OperationActionSucceed {
			mapPayload = aws.ToString(u.Payload)
		}
	}
	if mapPayload == "" {
		t.Fatal("no Map SUCCEED payload found in checkpoint updates")
	}

	replayResp := invokeBatch(t, &fakeLambda{}, batchPayload(`[10,25,45,50]`, wireOperation{
		Id:             hashID("1"),
		Status:         "SUCCEEDED",
		ContextDetails: &wireContextDetails{Result: mapPayload},
	}), handler)
	assertSucceeded(t, replayResp)
	if replayResp.Result != resp.Result {
		t.Fatalf("replay result = %s, live = %s", replayResp.Result, resp.Result)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("fn ran %d times on replay, want 0", got)
	}
}

func TestMapWithBatchSerdes(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) ([]string, error) {
		items := []string{"x", "y"}
		br, err := Map(ctx, "serdes", items, func(_ Context, item string, _ int) (string, error) {
			return strings.ToUpper(item), nil
		}, WithMaxConcurrency(1), WithBatchSerdes(wrapSerdes{}))
		if err != nil {
			return nil, err
		}
		return br.Results(), nil
	})
	assertSucceeded(t, resp)

	// Verify the checkpointed iteration payloads carry the wrapped form.
	updates := updateBatch(t, fake)
	wrapCount := 0
	for _, u := range updates {
		if p := aws.ToString(u.Payload); strings.Contains(p, "wrapped:") {
			wrapCount++
		}
	}
	if wrapCount < 2 {
		t.Errorf("expected at least 2 wrapped payloads, got %d", wrapCount)
	}
}

func TestMapInvalidMaxConcurrency(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (string, error) {
		items := []string{"a", "b"}
		_, err := Map(ctx, "bad", items, func(_ Context, _ string, _ int) (string, error) {
			return "", nil
		}, WithMaxConcurrency(0))
		if err != nil {
			return "", err
		}
		return "ok", nil
	})
	assertFailed(t, resp)
}

// --- Parallel tests ---

func TestParallelBasicSequential(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) ([]string, error) {
		br, err := Parallel(ctx, "parallel", []Branch[string]{
			{Name: "b0", Func: func(childCtx Context) (string, error) {
				return Step(childCtx, "", func(StepContext) (string, error) { return "task-1", nil })
			}},
			{Name: "b1", Func: func(childCtx Context) (string, error) {
				return Step(childCtx, "", func(StepContext) (string, error) { return "task-2", nil })
			}},
		}, WithMaxConcurrency(1))
		if err != nil {
			return nil, err
		}
		return br.Results(), nil
	})
	assertSucceeded(t, resp)
	if !strings.Contains(resp.Result, "task-1") || !strings.Contains(resp.Result, "task-2") {
		t.Fatalf("expected branch results in %s", resp.Result)
	}

	updates := updateBatch(t, fake)
	found := map[string]int{}
	for _, u := range updates {
		found[aws.ToString(u.SubType)]++
	}
	if found["Parallel"] < 2 {
		t.Errorf("expected at least 2 Parallel updates, got %d", found["Parallel"])
	}
	if found["ParallelBranch"] < 4 {
		t.Errorf("expected at least 4 ParallelBranch updates, got %d", found["ParallelBranch"])
	}
}

func TestParallelEmpty(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) ([]string, error) {
		br, err := Parallel(ctx, "empty", []Branch[string]{})
		if err != nil {
			return nil, err
		}
		return br.Results(), nil
	})
	assertSucceeded(t, resp)
}

func TestParallelFailFast(t *testing.T) {
	fake := &fakeLambda{}
	type result struct {
		Reason  string `json:"completionReason"`
		Status  string `json:"status"`
		Success int    `json:"successCount"`
		Failure int    `json:"failureCount"`
		Total   int    `json:"totalCount"`
	}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (result, error) {
		br, err := Parallel(ctx, "failfast", []Branch[string]{
			{Func: func(_ Context) (string, error) { return "ok", nil }},
			{Func: func(_ Context) (string, error) { return "", errors.New("fail") }},
			{Func: func(_ Context) (string, error) { return "never", nil }},
		}, WithMaxConcurrency(1), WithCompletion(CompletionConfig{ToleratedFailureCount: aws.Int(0)}))
		if err := batchOnly(err); err != nil {
			return result{}, err
		}
		return result{
			Reason:  br.Reason.String(),
			Status:  br.Status().String(),
			Success: br.SuccessCount(),
			Failure: br.FailureCount(),
			Total:   br.TotalCount(),
		}, nil
	})
	assertSucceeded(t, resp)
	if !strings.Contains(resp.Result, "FAILURE_TOLERANCE_EXCEEDED") {
		t.Fatalf("expected FAILURE_TOLERANCE_EXCEEDED in %s", resp.Result)
	}
}

func TestParallelFlatNesting(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) ([]string, error) {
		br, err := Parallel(ctx, "flat", []Branch[string]{
			{Func: func(childCtx Context) (string, error) {
				return Step(childCtx, "", func(StepContext) (string, error) { return "fa", nil })
			}},
			{Func: func(childCtx Context) (string, error) {
				return Step(childCtx, "", func(StepContext) (string, error) { return "fb", nil })
			}},
		}, WithMaxConcurrency(1), WithNesting(NestingFlat))
		if err != nil {
			return nil, err
		}
		return br.Results(), nil
	})
	assertSucceeded(t, resp)

	// Verify NO ParallelBranch context events in FLAT mode.
	updates := updateBatch(t, fake)
	for _, u := range updates {
		if aws.ToString(u.SubType) == "ParallelBranch" {
			t.Fatal("FLAT mode should not produce ParallelBranch context events")
		}
	}
}

func TestParallelConcurrentPreservesOrder(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) ([]string, error) {
		br, err := Parallel(ctx, "concurrent", []Branch[string]{
			{Func: func(_ Context) (string, error) { return "r0", nil }},
			{Func: func(_ Context) (string, error) { return "r1", nil }},
			{Func: func(_ Context) (string, error) { return "r2", nil }},
		}, WithMaxConcurrency(2))
		if err != nil {
			return nil, err
		}
		return br.Results(), nil
	})
	assertSucceeded(t, resp)
	var results []string
	if err := json.Unmarshal([]byte(resp.Result), &results); err != nil {
		t.Fatalf("unmarshal results: %v", err)
	}
	if len(results) != 3 || results[0] != "r0" || results[1] != "r1" || results[2] != "r2" {
		t.Fatalf("expected [r0,r1,r2], got %v", results)
	}
}

func TestParallelInvalidMaxConcurrency(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (string, error) {
		_, err := Parallel(ctx, "bad", []Branch[string]{
			{Func: func(_ Context) (string, error) { return "a", nil }},
		}, WithMaxConcurrency(0))
		if err != nil {
			return "", err
		}
		return "ok", nil
	})
	assertFailed(t, resp)
}

func TestParallelErr(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (string, error) {
		br, err := Parallel(ctx, "throwing", []Branch[string]{
			{Func: func(_ Context) (string, error) { return "", errors.New("branch failed") }},
			{Func: func(_ Context) (string, error) { return "never", nil }},
		}, WithMaxConcurrency(1), WithCompletion(CompletionConfig{ToleratedFailureCount: aws.Int(0)}))
		if err != nil {
			return "", err
		}
		return "should not reach: " + br.Reason.String(), nil
	})
	assertFailed(t, resp)
}

// TestBatchOutcome covers the three outcomes of a completed batch: items
// failed (a *BatchError carrying the item errors in input order), a
// batch-level failure without item errors (a *BatchError with no Errors),
// and success (nil). Results are built through public fields only, as an
// external consumer would.
func TestBatchOutcome(t *testing.T) {
	err1 := errors.New("first failure")
	err2 := errors.New("second failure")

	t.Run("failed items", func(t *testing.T) {
		result := BatchResult[string]{
			Items: []BatchItem[string]{
				{Index: 0, Status: BatchItemSucceeded, Result: "ok"},
				{Index: 1, Status: BatchItemFailed, Err: err1},
				{Index: 2, Status: BatchItemFailed, Err: err2},
			},
			Reason: CompletionFailureToleranceExceeded,
		}
		got, err := batchOutcome("b", result)
		var berr *BatchError
		if !errors.As(err, &berr) {
			t.Fatalf("err = %v (%T), want *BatchError", err, err)
		}
		if berr.Name != "b" || berr.Reason != CompletionFailureToleranceExceeded {
			t.Errorf("BatchError = %+v, want name b and FAILURE_TOLERANCE_EXCEEDED", berr)
		}
		if len(berr.Errors) != 2 || berr.Errors[0] != err1 || berr.Errors[1] != err2 {
			t.Errorf("Errors = %v, want [%v %v] in input order", berr.Errors, err1, err2)
		}
		if len(got.Items) != 3 {
			t.Errorf("result returned alongside the error has %d items, want 3", len(got.Items))
		}
	})

	t.Run("batch-level failure without item errors", func(t *testing.T) {
		result := BatchResult[string]{
			Items: []BatchItem[string]{
				{Index: 0, Status: BatchItemSucceeded, Result: "ok"},
				{Index: 1, Status: BatchItemStarted},
			},
			Reason: CompletionFailureToleranceExceeded,
		}
		_, err := batchOutcome("b", result)
		var berr *BatchError
		if !errors.As(err, &berr) {
			t.Fatalf("err = %v (%T), want *BatchError", err, err)
		}
		if len(berr.Errors) != 0 {
			t.Errorf("Errors = %v, want none", berr.Errors)
		}
		if !strings.Contains(err.Error(), "FAILURE_TOLERANCE_EXCEEDED") {
			t.Errorf("Error() = %q, want reason in message", err.Error())
		}
	})

	t.Run("success", func(t *testing.T) {
		result := BatchResult[string]{
			Items: []BatchItem[string]{
				{Index: 0, Status: BatchItemSucceeded, Result: "a"},
				{Index: 1, Status: BatchItemSucceeded, Result: "b"},
			},
			Reason: CompletionAllCompleted,
		}
		if _, err := batchOutcome("b", result); err != nil {
			t.Fatalf("err = %v, want nil for success", err)
		}
	})

	t.Run("custom failed without item errors", func(t *testing.T) {
		result := BatchResult[string]{
			Items: []BatchItem[string]{
				{Index: 0, Status: BatchItemSucceeded, Result: "ok"},
				{Index: 1, Status: BatchItemStarted},
			},
			Reason: CompletionCustomFailed,
		}
		_, err := batchOutcome("b", result)
		var berr *BatchError
		if !errors.As(err, &berr) {
			t.Fatalf("err = %v (%T), want *BatchError", err, err)
		}
		if berr.Reason != CompletionCustomFailed || len(berr.Errors) != 0 {
			t.Errorf("BatchError = %+v, want CUSTOM_COMPLETION_FAILED with no item errors", berr)
		}
		if !strings.Contains(err.Error(), "CUSTOM_COMPLETION_FAILED") {
			t.Errorf("Error() = %q, want reason in message", err.Error())
		}
	})

	t.Run("custom succeeded despite failed item", func(t *testing.T) {
		result := BatchResult[string]{
			Items: []BatchItem[string]{
				{Index: 0, Status: BatchItemSucceeded, Result: "ok"},
				{Index: 1, Status: BatchItemFailed, Err: err1},
			},
			Reason: CompletionCustomSucceeded,
		}
		got, err := batchOutcome("b", result)
		if err != nil {
			t.Fatalf("err = %v, want nil: a custom success decision is authoritative", err)
		}
		if got.FailureCount() != 1 {
			t.Errorf("FailureCount = %d, want 1: the failed item is still reported", got.FailureCount())
		}
	})
}

// TestStatusReflectsCompletionReason verifies that Status() follows the
// completion reason: a failure reason yields BatchItemFailed even without
// failed items, and a custom success decision yields BatchItemSucceeded even
// with a failed item.
func TestStatusReflectsCompletionReason(t *testing.T) {
	cases := []struct {
		name   string
		items  []BatchItem[string]
		reason CompletionReason
		want   BatchItemStatus
	}{
		{"tolerance exceeded, no failed item", []BatchItem[string]{{Status: BatchItemSucceeded, Result: "ok"}}, CompletionFailureToleranceExceeded, BatchItemFailed},
		{"custom failed, no failed item", []BatchItem[string]{{Status: BatchItemSucceeded, Result: "ok"}, {Status: BatchItemStarted}}, CompletionCustomFailed, BatchItemFailed},
		{"custom failed, no items", nil, CompletionCustomFailed, BatchItemFailed},
		{"custom succeeded, failed item", []BatchItem[string]{{Status: BatchItemFailed, Err: errors.New("x")}}, CompletionCustomSucceeded, BatchItemSucceeded},
		{"custom succeeded, no items", nil, CompletionCustomSucceeded, BatchItemSucceeded},
		{"all completed, failed item", []BatchItem[string]{{Status: BatchItemFailed, Err: errors.New("x")}}, CompletionAllCompleted, BatchItemFailed},
		{"min successful, failed item", []BatchItem[string]{{Status: BatchItemSucceeded}, {Status: BatchItemFailed, Err: errors.New("x")}}, CompletionMinSuccessfulReached, BatchItemFailed},
		{"min successful, no failed item", []BatchItem[string]{{Status: BatchItemSucceeded}, {Status: BatchItemStarted}}, CompletionMinSuccessfulReached, BatchItemSucceeded},
		{"unknown reason, no failed item", []BatchItem[string]{{Status: BatchItemSucceeded}}, CompletionReason(0), BatchItemSucceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := BatchResult[string]{Items: tc.items, Reason: tc.reason}
			if got := result.Status(); got != tc.want {
				t.Errorf("Status() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestStatusDerivedAfterPublicJSONRoundTrip verifies that Status() and
// the batch outcome stay authoritative for a BatchResult reconstructed purely from its
// exported fields — the shape an ordinary JSON/Serdes round trip or an
// external construction produces. Item Err values do not survive
// serialization, so the outcome is a *BatchError without item errors.
func TestStatusDerivedAfterPublicJSONRoundTrip(t *testing.T) {
	raw := `{"Items":[{"Index":0,"Status":1,"Result":"ok"},{"Index":1,"Name":"boom","Status":2}],"Reason":1}`
	var rt BatchResult[string]
	if err := json.Unmarshal([]byte(raw), &rt); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if rt.Status() != BatchItemFailed {
		t.Errorf("Status() = %v, want BatchItemFailed (derived from Items)", rt.Status())
	}
	_, got := batchOutcome("b", rt)
	if got == nil {
		t.Fatal("outcome = nil, want non-nil for public state that indicates failure")
	}
	if !strings.Contains(got.Error(), `batch "b" failed`) {
		t.Errorf("outcome = %q, want batch-level error message", got.Error())
	}

	// Mutating the public Items after construction must be reflected too.
	rt.Items = rt.Items[:1]
	rt.Reason = CompletionAllCompleted
	if rt.Status() != BatchItemSucceeded {
		t.Errorf("Status() after mutation = %v, want BatchItemSucceeded", rt.Status())
	}
	if _, err := batchOutcome("b", rt); err != nil {
		t.Errorf("outcome after mutation = %v, want nil", err)
	}
}

// jsonProjectionSerdes is an operation-level result serdes that serializes
// only the public, JSON-safe projection of a BatchResult — as a user serdes
// plausibly would. Item errors are dropped; status must survive.
type jsonProjectionSerdes struct{}

type jsonProjectionItem struct {
	Index  int             `json:"index"`
	Name   string          `json:"name,omitempty"`
	Status BatchItemStatus `json:"status"`
	Result string          `json:"result,omitempty"`
}

type jsonProjectionPayload struct {
	Items  []jsonProjectionItem `json:"items"`
	Reason CompletionReason     `json:"reason"`
}

func (jsonProjectionSerdes) Marshal(_ context.Context, _ SerdesContext, v any) ([]byte, error) {
	br, ok := v.(BatchResult[string])
	if !ok {
		return nil, fmt.Errorf("jsonProjectionSerdes.Marshal: unexpected type %T", v)
	}
	p := jsonProjectionPayload{Reason: br.Reason}
	for _, item := range br.Items {
		p.Items = append(p.Items, jsonProjectionItem{
			Index: item.Index, Name: item.Name, Status: item.Status, Result: item.Result,
		})
	}
	return json.Marshal(p)
}

func (jsonProjectionSerdes) Unmarshal(_ context.Context, _ SerdesContext, data []byte, v any) error {
	var p jsonProjectionPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	ptr, ok := v.(*BatchResult[string])
	if !ok {
		return fmt.Errorf("jsonProjectionSerdes.Unmarshal: target not *BatchResult[string]")
	}
	out := BatchResult[string]{Reason: p.Reason}
	for _, item := range p.Items {
		out.Items = append(out.Items, BatchItem[string]{
			Index: item.Index, Name: item.Name, Status: item.Status, Result: item.Result,
		})
	}
	*ptr = out
	return nil
}

// TestMapResultSerdesReplayStatusAndErr runs a Map with a failing item and
// a custom operation-level result serdes through the real live and replay
// invocation paths, asserting that Status() and the returned *BatchError
// report the failure identically on both. This is the public serialization path: the replayed
// BatchResult is reconstructed entirely by the user serdes from exported
// state, with no access to unexported fields.
func TestMapResultSerdesReplayStatusAndErr(t *testing.T) {
	type verdict struct {
		Status string `json:"status"`
		HasErr bool   `json:"hasErr"`
		Reason string `json:"reason"`
	}

	handler := func(ctx Context, _ any) (verdict, error) {
		items := []string{"ok", "bad"}
		br, err := Map(ctx, "proj-serde", items, func(_ Context, item string, _ int) (string, error) {
			if item == "bad" {
				return "", errors.New("item exploded")
			}
			return item, nil
		}, WithMaxConcurrency(1), WithBatchResultSerdes(jsonProjectionSerdes{}))
		var berr *BatchError
		if err != nil && !errors.As(err, &berr) {
			return verdict{}, err
		}
		return verdict{Status: br.Status().String(), HasErr: berr != nil, Reason: br.Reason.String()}, nil
	}

	// Phase 1: LIVE.
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), handler)
	assertSucceeded(t, resp)
	var live verdict
	if err := json.Unmarshal([]byte(resp.Result), &live); err != nil {
		t.Fatalf("unmarshal live result: %v", err)
	}
	if live.Status != "FAILED" || !live.HasErr {
		t.Fatalf("live verdict = %+v, want FAILED with error", live)
	}

	// Extract the Map parent SUCCEED payload written by the custom serdes.
	var mapPayload string
	for _, u := range updateBatch(t, fake) {
		if aws.ToString(u.SubType) == "Map" && u.Action == OperationActionSucceed {
			mapPayload = aws.ToString(u.Payload)
		}
	}
	if mapPayload == "" {
		t.Fatal("no Map SUCCEED payload found in checkpoint updates")
	}

	// Phase 2: REPLAY from the checkpointed payload.
	replayFake := &fakeLambda{}
	replayResp := invokeBatch(t, replayFake, batchPayload(`null`,
		wireOperation{
			Id:             hashID("1"),
			Status:         "SUCCEEDED",
			ContextDetails: &wireContextDetails{Result: mapPayload},
		},
	), handler)
	assertSucceeded(t, replayResp)
	var replay verdict
	if err := json.Unmarshal([]byte(replayResp.Result), &replay); err != nil {
		t.Fatalf("unmarshal replay result: %v", err)
	}
	if replay != live {
		t.Errorf("replay verdict = %+v, live = %+v — Status()/Err() diverged across replay", replay, live)
	}
}

// TestParallelBranchNamePropagates verifies that Branch.Name flows into
// the checkpoint as the branch's display name without requiring
// WithItemNamer.
func TestParallelBranchNamePropagates(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) ([]string, error) {
		br, err := Parallel(ctx, "named", []Branch[string]{
			{Name: "alpha", Func: func(_ Context) (string, error) { return "a", nil }},
			{Name: "beta", Func: func(_ Context) (string, error) { return "b", nil }},
		}, WithMaxConcurrency(1))
		if err != nil {
			return nil, err
		}
		// Verify item names in the result match Branch.Name.
		for _, item := range br.Items {
			switch item.Index {
			case 0:
				if item.Name != "alpha" {
					return nil, fmt.Errorf("item 0 name = %q, want %q", item.Name, "alpha")
				}
			case 1:
				if item.Name != "beta" {
					return nil, fmt.Errorf("item 1 name = %q, want %q", item.Name, "beta")
				}
			}
		}
		return br.Results(), nil
	})
	assertSucceeded(t, resp)

	// Verify checkpoint updates carry the branch names.
	updates := updateBatch(t, fake)
	names := map[string]bool{}
	for _, u := range updates {
		if aws.ToString(u.SubType) == "ParallelBranch" && u.Name != nil {
			names[aws.ToString(u.Name)] = true
		}
	}
	if !names["alpha"] || !names["beta"] {
		t.Errorf("expected branch names alpha,beta in checkpoints, got %v", names)
	}
}

func TestParallelAccessors(t *testing.T) {
	fake := &fakeLambda{}
	type result struct {
		HasFailure   bool `json:"hasFailure"`
		SuccessCount int  `json:"successCount"`
		FailureCount int  `json:"failureCount"`
		ErrorCount   int  `json:"errorCount"`
	}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (result, error) {
		br, err := Parallel(ctx, "accessors", []Branch[string]{
			{Func: func(_ Context) (string, error) { return "ok0", nil }},
			{Func: func(_ Context) (string, error) { return "", errors.New("branch failed") }},
			{Func: func(_ Context) (string, error) { return "ok2", nil }},
		}, WithMaxConcurrency(1), WithCompletion(CompletionConfig{
			ToleratedFailureCount: aws.Int(1),
		}))
		if err := batchOnly(err); err != nil {
			return result{}, err
		}
		return result{
			HasFailure:   br.HasFailure(),
			SuccessCount: len(br.Succeeded()),
			FailureCount: len(br.Failed()),
			ErrorCount:   len(br.Errors()),
		}, nil
	})
	assertSucceeded(t, resp)
	var r struct {
		HasFailure   bool `json:"hasFailure"`
		SuccessCount int  `json:"successCount"`
		FailureCount int  `json:"failureCount"`
		ErrorCount   int  `json:"errorCount"`
	}
	if err := json.Unmarshal([]byte(resp.Result), &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !r.HasFailure {
		t.Error("expected hasFailure=true")
	}
	if r.SuccessCount != 2 {
		t.Errorf("expected successCount=2, got %d", r.SuccessCount)
	}
	if r.FailureCount != 1 {
		t.Errorf("expected failureCount=1, got %d", r.FailureCount)
	}
	if r.ErrorCount != 1 {
		t.Errorf("expected errorCount=1, got %d", r.ErrorCount)
	}
}

// --- Critical cross-SDK lesson: live == replay shape for early completion ---

func TestMapLiveEqualsReplayShapeEarlyCompletion(t *testing.T) {
	// Two-phase test: asserts that the REPLAY path produces the exact same
	// BatchResult shape as the LIVE path for an early-completion scenario.
	// This guards against the sibling-SDK divergence where live returned 2
	// items but replay returned all 4 (total_count mismatch).

	type result struct {
		Total   int    `json:"totalCount"`
		Success int    `json:"successCount"`
		Reason  string `json:"completionReason"`
		Items   int    `json:"itemCount"`
	}

	handler := func(ctx Context, _ any) (result, error) {
		items := []string{"a", "b", "c", "d"}
		br, err := Map(ctx, "early", items, func(_ Context, item string, _ int) (string, error) {
			return item, nil
		}, WithMaxConcurrency(1), WithCompletion(CompletionConfig{MinSuccessful: 2}))
		if err != nil {
			return result{}, err
		}
		return result{
			Total:   br.TotalCount(),
			Success: br.SuccessCount(),
			Reason:  br.Reason.String(),
			Items:   len(br.Items),
		}, nil
	}

	// Phase 1: LIVE execution — capture result shape and checkpoint payload.
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), handler)
	assertSucceeded(t, resp)

	var liveResult result
	if err := json.Unmarshal([]byte(resp.Result), &liveResult); err != nil {
		t.Fatalf("unmarshal live result: %v", err)
	}
	if liveResult.Total != 2 {
		t.Fatalf("live totalCount = %d, want 2", liveResult.Total)
	}
	if liveResult.Items != 2 {
		t.Fatalf("live itemCount = %d, want 2 (never-started excluded)", liveResult.Items)
	}
	if liveResult.Reason != "MIN_SUCCESSFUL_REACHED" {
		t.Fatalf("live reason = %s, want MIN_SUCCESSFUL_REACHED", liveResult.Reason)
	}

	// Extract the Map parent's SUCCEED checkpoint payload from the fake.
	updates := updateBatch(t, fake)
	var mapPayload string
	for _, u := range updates {
		if aws.ToString(u.SubType) == "Map" && u.Action == OperationActionSucceed {
			mapPayload = aws.ToString(u.Payload)
		}
	}
	if mapPayload == "" {
		t.Fatal("no Map SUCCEED payload found in checkpoint updates")
	}

	// Phase 2: REPLAY — construct invocation with the Map pre-checkpointed
	// as SUCCEEDED carrying the live payload, then re-invoke the same handler.
	replayFake := &fakeLambda{}
	replayInput := batchPayload(`null`,
		wireOperation{
			Id:             hashID("1"), // Map is the first operation claimed
			Status:         "SUCCEEDED",
			ContextDetails: &wireContextDetails{Result: mapPayload},
		},
	)
	replayResp := invokeBatch(t, replayFake, replayInput, handler)
	assertSucceeded(t, replayResp)

	var replayResult result
	if err := json.Unmarshal([]byte(replayResp.Result), &replayResult); err != nil {
		t.Fatalf("unmarshal replay result: %v", err)
	}

	// Assert LIVE == REPLAY shape identity.
	if replayResult.Total != liveResult.Total {
		t.Errorf("replay totalCount = %d, live = %d", replayResult.Total, liveResult.Total)
	}
	if replayResult.Items != liveResult.Items {
		t.Errorf("replay itemCount = %d, live = %d", replayResult.Items, liveResult.Items)
	}
	if replayResult.Success != liveResult.Success {
		t.Errorf("replay successCount = %d, live = %d", replayResult.Success, liveResult.Success)
	}
	if replayResult.Reason != liveResult.Reason {
		t.Errorf("replay reason = %q, live = %q", replayResult.Reason, liveResult.Reason)
	}

	// Verify the replay path issued NO checkpoint updates (pure replay).
	if len(replayFake.gotUpdateBatches) != 0 {
		t.Errorf("replay issued %d checkpoint batches, want 0 (pure replay)", len(replayFake.gotUpdateBatches))
	}
}

// TestConcurrentMinSuccessfulAbandonsInFlight verifies that a concurrent
// batch completing early on MinSuccessful stops awaiting the branches still
// in flight: they are reported STARTED and are NOT counted as successes,
// even though (absent abandonment) their remaining steps would have
// succeeded. This is the regression guard for the divergence where the Go
// concurrent path awaited every branch to a terminal state before returning.
func TestConcurrentMinSuccessfulAbandonsInFlight(t *testing.T) {
	fake := &fakeLambda{}
	type result struct {
		Success      int    `json:"successCount"`
		Failure      int    `json:"failureCount"`
		Started      int    `json:"startedCount"`
		StartedIndex []int  `json:"startedIndex"`
		Total        int    `json:"totalCount"`
		Reason       string `json:"reason"`
	}
	slowBranch := func(a, b string) Branch[string] {
		return Branch[string]{Func: func(c Context) (string, error) {
			// First step sleeps so the two fast branches reach the
			// MinSuccessful threshold before this branch would run its
			// second step.
			if _, err := Step(c, a, func(StepContext) (string, error) {
				time.Sleep(80 * time.Millisecond)
				return a, nil
			}); err != nil {
				return "", err
			}
			// Reached only if the branch was not abandoned: claiming this
			// operation unwinds once the batch has completed.
			return Step(c, b, func(StepContext) (string, error) { return b, nil })
		}}
	}
	fastBranch := func(v string) Branch[string] {
		return Branch[string]{Func: func(c Context) (string, error) {
			return Step(c, v, func(StepContext) (string, error) { return v, nil })
		}}
	}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (result, error) {
		// Unlimited concurrency: all four branches start immediately.
		br, err := Parallel(ctx, "abandon", []Branch[string]{
			fastBranch("f0"),
			fastBranch("f1"),
			slowBranch("s2a", "s2b"),
			slowBranch("s3a", "s3b"),
		}, WithCompletion(CompletionConfig{MinSuccessful: 2}))
		if err != nil {
			return result{}, err
		}
		var startedIndex []int
		for _, item := range br.Started() {
			startedIndex = append(startedIndex, item.Index)
		}
		return result{
			Success:      br.SuccessCount(),
			Failure:      br.FailureCount(),
			Started:      br.StartedCount(),
			StartedIndex: startedIndex,
			Total:        br.TotalCount(),
			Reason:       br.Reason.String(),
		}, nil
	})
	assertSucceeded(t, resp)
	var r result
	if err := json.Unmarshal([]byte(resp.Result), &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.Reason != "MIN_SUCCESSFUL_REACHED" {
		t.Errorf("reason = %s, want MIN_SUCCESSFUL_REACHED", r.Reason)
	}
	// The two fast branches succeed; the two slow branches are abandoned
	// in flight (reported STARTED), never reaching their second step.
	if r.Success != 2 {
		t.Errorf("successCount = %d, want 2 (in-flight branches must be abandoned, not awaited)", r.Success)
	}
	if r.Started != 2 {
		t.Errorf("startedCount = %d, want 2 (abandoned branches)", r.Started)
	}
	// Started() reports the abandoned branches in input order.
	if want := []int{2, 3}; !reflect.DeepEqual(r.StartedIndex, want) {
		t.Errorf("Started() indexes = %v, want %v", r.StartedIndex, want)
	}
	if r.Failure != 0 {
		t.Errorf("failureCount = %d, want 0", r.Failure)
	}
	if r.Total != 4 {
		t.Errorf("totalCount = %d, want 4 (2 succeeded + 2 abandoned)", r.Total)
	}
}

// TestConcurrentAbandonedBranchLateCheckpointDoesNotFail verifies that a
// branch whose in-flight step completes and checkpoints AFTER the batch
// completion decision fired does not turn into an execution failure: the
// late checkpoint is absorbed and the execution succeeds.
func TestConcurrentAbandonedBranchLateCheckpointDoesNotFail(t *testing.T) {
	fake := &fakeLambda{}
	type result struct {
		Success int    `json:"successCount"`
		Failure int    `json:"failureCount"`
		Total   int    `json:"totalCount"`
		Reason  string `json:"reason"`
	}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (result, error) {
		br, err := Parallel(ctx, "late-checkpoint", []Branch[string]{
			{Func: func(c Context) (string, error) {
				return Step(c, "fast", func(StepContext) (string, error) { return "fast", nil })
			}},
			{Func: func(c Context) (string, error) {
				// Completes and checkpoints after MinSuccessful=1 is met
				// by the fast branch. The late child checkpoint must be
				// harmless.
				return Step(c, "late", func(StepContext) (string, error) {
					time.Sleep(60 * time.Millisecond)
					return "late", nil
				})
			}},
		}, WithCompletion(CompletionConfig{MinSuccessful: 1}))
		if err != nil {
			return result{}, err
		}
		return result{
			Success: br.SuccessCount(),
			Failure: br.FailureCount(),
			Total:   br.TotalCount(),
			Reason:  br.Reason.String(),
		}, nil
	})
	assertSucceeded(t, resp)
	var r result
	if err := json.Unmarshal([]byte(resp.Result), &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.Failure != 0 {
		t.Errorf("failureCount = %d, want 0 (late checkpoint must not fail the execution)", r.Failure)
	}
	if r.Success < 1 {
		t.Errorf("successCount = %d, want >= 1", r.Success)
	}
	if r.Reason != "MIN_SUCCESSFUL_REACHED" {
		t.Errorf("reason = %s, want MIN_SUCCESSFUL_REACHED", r.Reason)
	}
}

func TestMapPanicInItemDoesNotHangCoordinator(t *testing.T) {
	fake := &fakeLambda{}
	type result struct {
		Success int `json:"successCount"`
		Failure int `json:"failureCount"`
		Total   int `json:"totalCount"`
	}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (result, error) {
		items := []string{"ok", "panic", "after"}
		br, err := Map(ctx, "panic-test", items, func(_ Context, item string, _ int) (string, error) {
			if item == "panic" {
				panic("test panic")
			}
			return item, nil
		}, WithMaxConcurrency(1), WithCompletion(CompletionConfig{
			ToleratedFailureCount: aws.Int(1),
		}))
		if err := batchOnly(err); err != nil {
			return result{}, err
		}
		return result{
			Success: br.SuccessCount(),
			Failure: br.FailureCount(),
			Total:   br.TotalCount(),
		}, nil
	})
	assertSucceeded(t, resp)
	var r result
	if err := json.Unmarshal([]byte(resp.Result), &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.Failure != 1 {
		t.Errorf("failureCount = %d, want 1 (from panic)", r.Failure)
	}
}

func TestParallelNestedParallel(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) ([][]string, error) {
		br, err := Parallel(ctx, "outer", []Branch[[]string]{
			{Func: func(outerCtx Context) ([]string, error) {
				inner, err := Parallel(outerCtx, "inner", []Branch[string]{
					{Func: func(innerCtx Context) (string, error) {
						return Step(innerCtx, "", func(StepContext) (string, error) { return "i1", nil })
					}},
					{Func: func(innerCtx Context) (string, error) {
						return Step(innerCtx, "", func(StepContext) (string, error) { return "i2", nil })
					}},
				}, WithMaxConcurrency(1))
				if err != nil {
					return nil, err
				}
				return inner.Results(), nil
			}},
		}, WithMaxConcurrency(1))
		if err != nil {
			return nil, err
		}
		return br.Results(), nil
	})
	assertSucceeded(t, resp)
	if !strings.Contains(resp.Result, "i1") || !strings.Contains(resp.Result, "i2") {
		t.Fatalf("expected nested results in %s", resp.Result)
	}
}

// --- Batch result serdes tests ---

// wrapSerdes is a test custom serializer that wraps values with "wrapped:" prefix.
type wrapSerdes struct{}

func (wrapSerdes) Marshal(_ context.Context, _ SerdesContext, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	s := string(b)
	if len(s) > 1 && s[0] == '"' {
		inner := s[1 : len(s)-1]
		return []byte("wrapped:" + inner), nil
	}
	return []byte("wrapped:" + s), nil
}

func (wrapSerdes) Unmarshal(_ context.Context, _ SerdesContext, data []byte, v any) error {
	s := string(data)
	unwrapped := strings.TrimPrefix(s, "wrapped:")
	ptr, ok := v.(*string)
	if ok {
		*ptr = unwrapped
		return nil
	}
	return json.Unmarshal([]byte(`"`+unwrapped+`"`), v)
}

func TestMapOperationLevelSerdes(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) ([]string, error) {
		items := []string{"x", "y"}
		br, err := Map(ctx, "op-serde", items, func(_ Context, item string, _ int) (string, error) {
			return strings.ToUpper(item), nil
		}, WithMaxConcurrency(1), WithBatchResultSerdes(opSerdes{}))
		if err != nil {
			return nil, err
		}
		return br.Results(), nil
	})
	assertSucceeded(t, resp)

	// Verify the parent Map ContextSucceeded payload uses the custom serializer.
	updates := updateBatch(t, fake)
	var mapSucceedPayload string
	for _, u := range updates {
		if aws.ToString(u.SubType) == "Map" && u.Action == OperationActionSucceed {
			mapSucceedPayload = aws.ToString(u.Payload)
		}
	}
	if !strings.HasPrefix(mapSucceedPayload, "OPSERDE:") {
		t.Fatalf("expected Map success payload to start with OPSERDE:, got %q", mapSucceedPayload)
	}
}

// opSerdes is the operation-level serdes for testing: "OPSERDE:X,Y" format.
type opSerdes struct{}

func (opSerdes) Marshal(_ context.Context, _ SerdesContext, v any) ([]byte, error) {
	br, ok := v.(BatchResult[string])
	if !ok {
		return nil, fmt.Errorf("opSerdes.Marshal: unexpected type %T", v)
	}
	results := br.Results()
	return []byte("OPSERDE:" + strings.Join(results, ",")), nil
}

func (opSerdes) Unmarshal(_ context.Context, _ SerdesContext, data []byte, v any) error {
	s := string(data)
	if !strings.HasPrefix(s, "OPSERDE:") {
		return fmt.Errorf("opSerdes.Unmarshal: unexpected format %q", s)
	}
	vals := strings.Split(strings.TrimPrefix(s, "OPSERDE:"), ",")
	items := make([]BatchItem[string], len(vals))
	for i, val := range vals {
		items[i] = BatchItem[string]{
			Index:  i,
			Status: BatchItemSucceeded,
			Result: val,
		}
	}
	ptr, ok := v.(*BatchResult[string])
	if !ok {
		return fmt.Errorf("opSerdes.Unmarshal: target not *BatchResult[string]")
	}
	*ptr = BatchResult[string]{Items: items, Reason: CompletionAllCompleted}
	return nil
}

func TestMapToleratedWithinAllComplete(t *testing.T) {
	fake := &fakeLambda{}
	type result struct {
		Reason  string `json:"completionReason"`
		Status  string `json:"status"`
		Success int    `json:"successCount"`
		Failure int    `json:"failureCount"`
		Total   int    `json:"totalCount"`
	}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (result, error) {
		items := []string{"ok", "fail", "ok2"}
		br, err := Map(ctx, "tolerated", items, func(_ Context, item string, _ int) (string, error) {
			if item == "fail" {
				return "", errors.New("item failed")
			}
			return item, nil
		}, WithMaxConcurrency(1), WithCompletion(CompletionConfig{
			ToleratedFailureCount: aws.Int(1),
		}))
		if err := batchOnly(err); err != nil {
			return result{}, err
		}
		return result{
			Reason:  br.Reason.String(),
			Status:  br.Status().String(),
			Success: br.SuccessCount(),
			Failure: br.FailureCount(),
			Total:   br.TotalCount(),
		}, nil
	})
	assertSucceeded(t, resp)
	var r result
	if err := json.Unmarshal([]byte(resp.Result), &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.Reason != "ALL_COMPLETED" {
		t.Errorf("reason = %s, want ALL_COMPLETED", r.Reason)
	}
	if r.Status != "FAILED" {
		t.Errorf("status = %s, want FAILED", r.Status)
	}
	if r.Total != 3 {
		t.Errorf("total = %d, want 3", r.Total)
	}
}

// --- Round-trip tests for checkpoint serialization format ---

func TestBatchCheckpointPayloadRoundTrip(t *testing.T) {
	// Table-driven tests exercising fromBatchResult → marshal → unmarshal →
	// toBatchResult with various edge cases. Guards against regressions in
	// the checkpoint serialization format.
	serdes := jsonSerdes{}

	tests := []struct {
		name   string
		result BatchResult[string]
	}{
		{
			name: "mixed success and failure",
			result: BatchResult[string]{
				Items: []BatchItem[string]{
					{Index: 0, Name: "item-0", Status: BatchItemSucceeded, Result: "hello"},
					{Index: 1, Name: "item-1", Status: BatchItemFailed, Err: &ChildContextError{
						Name: "item-1",
						Err:  &replayedError{errType: "RuntimeError", message: "something failed"},
					}},
					{Index: 2, Name: "item-2", Status: BatchItemSucceeded, Result: "world"},
				},
				Reason: CompletionAllCompleted,
			},
		},
		{
			name: "early completion with not-started items excluded",
			result: BatchResult[string]{
				Items: []BatchItem[string]{
					{Index: 0, Name: "0", Status: BatchItemSucceeded, Result: "a"},
					{Index: 1, Name: "1", Status: BatchItemSucceeded, Result: "b"},
				},
				Reason: CompletionMinSuccessfulReached,
			},
		},
		{
			name: "empty items",
			result: BatchResult[string]{
				Items:  nil,
				Reason: CompletionAllCompleted,
			},
		},
		{
			name: "item with empty result string",
			result: BatchResult[string]{
				Items: []BatchItem[string]{
					{Index: 0, Name: "empty", Status: BatchItemSucceeded, Result: ""},
				},
				Reason: CompletionAllCompleted,
			},
		},
		{
			name: "failure tolerance exceeded",
			result: BatchResult[string]{
				Items: []BatchItem[string]{
					{Index: 0, Name: "0", Status: BatchItemSucceeded, Result: "ok"},
					{Index: 1, Name: "1", Status: BatchItemFailed, Err: &ChildContextError{
						Name: "1",
						Err:  &replayedError{errType: "Error", message: "fail-1"},
					}},
					{Index: 2, Name: "2", Status: BatchItemFailed, Err: &ChildContextError{
						Name: "2",
						Err:  &replayedError{errType: "Error", message: "fail-2"},
					}},
				},
				Reason: CompletionFailureToleranceExceeded,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// fromBatchResult → marshal
			payload, err := fromBatchResult(context.Background(), tt.result, serdes, noItemSctx)
			if err != nil {
				t.Fatalf("fromBatchResult: %v", err)
			}
			raw, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal payload: %v", err)
			}

			// unmarshal → toBatchResult
			var decoded batchCheckpointPayload
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatalf("unmarshal payload: %v", err)
			}
			got, err := toBatchResult[string](context.Background(), decoded, serdes, noItemSctx)
			if err != nil {
				t.Fatalf("toBatchResult: %v", err)
			}

			// Assert shape identity.
			if len(got.Items) != len(tt.result.Items) {
				t.Fatalf("items len = %d, want %d", len(got.Items), len(tt.result.Items))
			}
			if got.Reason != tt.result.Reason {
				t.Errorf("reason = %v, want %v", got.Reason, tt.result.Reason)
			}
			for i, want := range tt.result.Items {
				item := got.Items[i]
				if item.Index != want.Index {
					t.Errorf("item[%d].Index = %d, want %d", i, item.Index, want.Index)
				}
				if item.Name != want.Name {
					t.Errorf("item[%d].Name = %q, want %q", i, item.Name, want.Name)
				}
				if item.Status != want.Status {
					t.Errorf("item[%d].Status = %d, want %d", i, item.Status, want.Status)
				}
				if item.Status == BatchItemSucceeded && item.Result != want.Result {
					t.Errorf("item[%d].Result = %q, want %q", i, item.Result, want.Result)
				}
				if item.Status == BatchItemFailed {
					if item.Err == nil {
						t.Errorf("item[%d].Err = nil, want non-nil", i)
					} else if want.Err != nil {
						// Verify the error is a ChildContextError wrapping
						// a replayedError with the inner type/message
						// preserved (error formatting differs after round-
						// trip since replayedError prepends the type).
						var gotChild *ChildContextError
						if !errors.As(item.Err, &gotChild) {
							t.Errorf("item[%d].Err is not a ChildContextError: %T", i, item.Err)
						}
					}
				}
			}
		})
	}
}

// TestBatchCheckpointPreservesInnerErrorType verifies that a failed batch
// item's inner SDK wrapper type (e.g. StepError) survives the checkpoint
// round-trip through fromBatchResult → JSON → toBatchResult. Before the fix,
// errors.As(err, &StepError{}) succeeded on the live error but failed after
// replay because toBatchResult flattened the chain to ChildContextError →
// replayedError, losing the concrete wrapper.
func TestBatchCheckpointPreservesInnerErrorType(t *testing.T) {
	serdes := jsonSerdes{}

	// Simulate a live batch result where item 1 failed with a StepError
	// inside a ChildContextError (the common case: a Step inside a Map
	// iteration fails).
	liveResult := BatchResult[string]{
		Items: []BatchItem[string]{
			{Index: 0, Name: "item-0", Status: BatchItemSucceeded, Result: "ok"},
			{Index: 1, Name: "item-1", Status: BatchItemFailed, Err: &ChildContextError{
				Name: "item-1",
				Err: &StepError{
					Name:     "fetch-data",
					Attempts: 3,
					Err:      fmt.Errorf("connection refused"),
				},
			}},
		},
		Reason: CompletionAllCompleted,
	}

	// Verify the live error supports errors.As for StepError.
	var liveStepErr *StepError
	if !errors.As(liveResult.Items[1].Err, &liveStepErr) {
		t.Fatal("live error: errors.As(*StepError) should succeed")
	}
	if liveStepErr.Name != "fetch-data" {
		t.Errorf("live StepError.Name = %q, want %q", liveStepErr.Name, "fetch-data")
	}
	if liveStepErr.Attempts != 3 {
		t.Errorf("live StepError.Attempts = %d, want %d", liveStepErr.Attempts, 3)
	}

	// Round-trip through checkpoint serialization.
	payload, err := fromBatchResult(context.Background(), liveResult, serdes, noItemSctx)
	if err != nil {
		t.Fatalf("fromBatchResult: %v", err)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	var decoded batchCheckpointPayload
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	got, err := toBatchResult[string](context.Background(), decoded, serdes, noItemSctx)
	if err != nil {
		t.Fatalf("toBatchResult: %v", err)
	}

	// After replay, errors.As for ChildContextError must still succeed.
	var replayedChild *ChildContextError
	if !errors.As(got.Items[1].Err, &replayedChild) {
		t.Fatal("replayed error: errors.As(*ChildContextError) failed")
	}
	if replayedChild.Name != "item-1" {
		t.Errorf("replayed ChildContextError.Name = %q, want %q", replayedChild.Name, "item-1")
	}

	// KEY ASSERTION: errors.As for the inner StepError must succeed after
	// replay, matching the live behavior.
	var replayedStep *StepError
	if !errors.As(got.Items[1].Err, &replayedStep) {
		t.Fatal("replayed error: errors.As(*StepError) failed — inner wrapper type lost across replay")
	}
	if replayedStep.Name != "fetch-data" {
		t.Errorf("replayed StepError.Name = %q, want %q", replayedStep.Name, "fetch-data")
	}
	if replayedStep.Attempts != 3 {
		t.Errorf("replayed StepError.Attempts = %d, want %d", replayedStep.Attempts, 3)
	}

	// The leaf error (user-defined) should be represented as a
	// replayedError carrying the original type name and message.
	if replayedStep.Err == nil {
		t.Fatal("replayed StepError.Err is nil, want non-nil leaf error")
	}
	leafErr, ok := replayedStep.Err.(*replayedError)
	if !ok {
		t.Fatalf("replayed StepError.Err type = %T, want *replayedError", replayedStep.Err)
	}
	if leafErr.errType != "errorString" && leafErr.errType != "Error" {
		// fmt.Errorf produces *errors.errorString; wireErrorType normalizes
		// it to "Error".
		t.Errorf("replayed leaf errType = %q, want %q", leafErr.errType, "Error")
	}
	if leafErr.message != "connection refused" {
		t.Errorf("replayed leaf message = %q, want %q", leafErr.message, "connection refused")
	}
}

// TestBatchCheckpointBackwardCompat verifies that a checkpoint payload
// serialized WITHOUT the new inner wrapper fields (pre-fix format) still
// deserializes correctly. The ErrType is preserved in replayedError since
// there is no inner metadata to reconstruct from.
func TestBatchCheckpointBackwardCompat(t *testing.T) {
	// Simulate an old-format checkpoint without StepName/StepAttempts/InnerErr fields.
	oldPayload := `{
		"results": [
			{"index": 0, "name": "item-0", "status": 1, "result": "\"ok\""},
			{"index": 1, "name": "item-1", "status": 2, "errType": "CustomError", "errMessage": "something broke"}
		],
		"reason": 0
	}`
	var decoded batchCheckpointPayload
	if err := json.Unmarshal([]byte(oldPayload), &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got, err := toBatchResult[string](context.Background(), decoded, jsonSerdes{}, noItemSctx)
	if err != nil {
		t.Fatalf("toBatchResult: %v", err)
	}
	if len(got.Items) != 2 {
		t.Fatalf("items = %d, want 2", len(got.Items))
	}
	// Failed item should have ChildContextError wrapping replayedError
	// since "CustomError" is not a known SDK wrapper type.
	var childErr *ChildContextError
	if !errors.As(got.Items[1].Err, &childErr) {
		t.Fatal("errors.As(*ChildContextError) failed")
	}
	re, ok := childErr.Err.(*replayedError)
	if !ok {
		t.Fatalf("inner error type = %T, want *replayedError", childErr.Err)
	}
	if re.errType != "CustomError" {
		t.Errorf("replayedError.errType = %q, want %q", re.errType, "CustomError")
	}
	if re.message != "something broke" {
		t.Errorf("replayedError.message = %q, want %q", re.message, "something broke")
	}
}

// TestBatchCheckpointBackwardCompatStepError verifies that a legacy
// checkpoint with ErrType="StepError" but NO inner metadata still
// reconstructs a StepError (with zero-value fields) so errors.As succeeds.
func TestBatchCheckpointBackwardCompatStepError(t *testing.T) {
	oldPayload := `{
		"results": [
			{"index": 0, "name": "item-0", "status": 2, "errType": "StepError", "errMessage": "durable: step \"x\" failed after 2 attempts: timeout"}
		],
		"reason": 0
	}`
	var decoded batchCheckpointPayload
	if err := json.Unmarshal([]byte(oldPayload), &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got, err := toBatchResult[string](context.Background(), decoded, jsonSerdes{}, noItemSctx)
	if err != nil {
		t.Fatalf("toBatchResult: %v", err)
	}

	// errors.As for StepError must succeed even without inner metadata.
	var stepErr *StepError
	if !errors.As(got.Items[0].Err, &stepErr) {
		t.Fatal("errors.As(*StepError) failed for legacy checkpoint")
	}
	// Without persisted metadata, Name and Attempts are zero-valued.
	if stepErr.Name != "" {
		t.Errorf("StepError.Name = %q, want empty (no metadata in legacy checkpoint)", stepErr.Name)
	}
	if stepErr.Attempts != 0 {
		t.Errorf("StepError.Attempts = %d, want 0 (no metadata in legacy checkpoint)", stepErr.Attempts)
	}
	// The leaf error should carry the full message since no inner details
	// are available.
	if stepErr.Err == nil {
		t.Fatal("StepError.Err is nil")
	}
	re, ok := stepErr.Err.(*replayedError)
	if !ok {
		t.Fatalf("StepError.Err type = %T, want *replayedError", stepErr.Err)
	}
	if re.errType != "Error" {
		t.Errorf("leaf errType = %q, want %q", re.errType, "Error")
	}
}

// TestRouteB_ErrorDataReconstructsRealValues verifies that the wire-only
// replay path (Route B) produces real StepError.Name and StepError.Attempts
// values when ErrorData carries the childErrorData JSON blob. This is the
// common mid-batch cross-invocation resume case where only
// op.childCtx.errType, errMessage, and errData are available.
func TestRouteB_ErrorDataReconstructsRealValues(t *testing.T) {
	// Simulate the ErrorData that encodeChildErrorData would write for a
	// StepError with Name="fetch-data", Attempts=3, leaf="connection refused".
	errData := `{"stepName":"fetch-data","stepAttempts":3,"innerErrType":"Error","innerErrMessage":"connection refused"}`

	// Route B: reconstructInnerError with direct fields empty, errData present.
	inner := reconstructInnerError("StepError", "durable: step \"fetch-data\" failed after 3 attempts: connection refused", childErrorData{}, errData)

	// errors.As for StepError must succeed.
	var stepErr *StepError
	if !errors.As(inner, &stepErr) {
		t.Fatal("Route B: errors.As(*StepError) failed — ErrorData not used for reconstruction")
	}
	if stepErr.Name != "fetch-data" {
		t.Errorf("Route B: StepError.Name = %q, want %q", stepErr.Name, "fetch-data")
	}
	if stepErr.Attempts != 3 {
		t.Errorf("Route B: StepError.Attempts = %d, want %d", stepErr.Attempts, 3)
	}
	if stepErr.Err == nil {
		t.Fatal("Route B: StepError.Err is nil")
	}
	leaf, ok := stepErr.Err.(*replayedError)
	if !ok {
		t.Fatalf("Route B: StepError.Err type = %T, want *replayedError", stepErr.Err)
	}
	if leaf.errType != "Error" {
		t.Errorf("Route B: leaf errType = %q, want %q", leaf.errType, "Error")
	}
	if leaf.message != "connection refused" {
		t.Errorf("Route B: leaf message = %q, want %q", leaf.message, "connection refused")
	}
}

// TestRouteB_ErrorDataAbsent verifies graceful fallback when ErrorData is
// empty (old checkpoints pre-dating the ErrorData enhancement). errors.As
// for StepError must still succeed; fields are zero-valued.
func TestRouteB_ErrorDataAbsent(t *testing.T) {
	inner := reconstructInnerError("StepError", "durable: step \"x\" failed after 2 attempts: timeout", childErrorData{}, "")

	var stepErr *StepError
	if !errors.As(inner, &stepErr) {
		t.Fatal("ErrorData absent: errors.As(*StepError) failed")
	}
	// Without ErrorData, Name and Attempts remain zero-valued.
	if stepErr.Name != "" {
		t.Errorf("ErrorData absent: StepError.Name = %q, want empty", stepErr.Name)
	}
	if stepErr.Attempts != 0 {
		t.Errorf("ErrorData absent: StepError.Attempts = %d, want 0", stepErr.Attempts)
	}
	// Leaf falls back to errMessage since no inner details are available.
	leaf, ok := stepErr.Err.(*replayedError)
	if !ok {
		t.Fatalf("ErrorData absent: StepError.Err type = %T, want *replayedError", stepErr.Err)
	}
	if leaf.errType != "Error" {
		t.Errorf("ErrorData absent: leaf errType = %q, want %q", leaf.errType, "Error")
	}
	if leaf.message != "durable: step \"x\" failed after 2 attempts: timeout" {
		t.Errorf("ErrorData absent: leaf message = %q, want full errMessage", leaf.message)
	}
}

// TestRouteB_ErrorDataMalformed verifies graceful fallback when ErrorData
// contains unparseable content. Must never fail an execution — degrades to
// the same behavior as absent ErrorData.
func TestRouteB_ErrorDataMalformed(t *testing.T) {
	malformedCases := []struct {
		name    string
		errData string
	}{
		{"not JSON", "this is not json at all"},
		{"empty object", "{}"},
		{"wrong structure", `{"foo":"bar","baz":42}`},
		{"truncated", `{"stepName":"fetch`},
	}
	for _, tc := range malformedCases {
		t.Run(tc.name, func(t *testing.T) {
			inner := reconstructInnerError("StepError", "step failed", childErrorData{}, tc.errData)

			// Must not panic or return nil.
			if inner == nil {
				t.Fatal("reconstructInnerError returned nil")
			}
			// errors.As for StepError must still succeed.
			var stepErr *StepError
			if !errors.As(inner, &stepErr) {
				t.Fatal("malformed ErrorData: errors.As(*StepError) failed")
			}
			// Fields degrade to whatever the parse extracted (empty for
			// wrong-structure/truncated, or zero for empty-object).
			// The key invariant: no panic, no error propagation.
		})
	}
}

// TestRouteB_UserDefinedLeafStaysStringTyped verifies that a user-defined
// error type that is NOT an SDK wrapper remains a string-typed replayedError
// after Route B replay. Only SDK wrapper types (StepError) get concrete
// reconstruction; user types stay opaque.
func TestRouteB_UserDefinedLeafStaysStringTyped(t *testing.T) {
	// ErrorData with a user-defined inner error type.
	errData := `{"stepName":"process","stepAttempts":1,"innerErrType":"MyCustomError","innerErrMessage":"custom failure"}`

	inner := reconstructInnerError("StepError", "durable: step \"process\" failed after 1 attempts: custom failure", childErrorData{}, errData)

	var stepErr *StepError
	if !errors.As(inner, &stepErr) {
		t.Fatal("errors.As(*StepError) failed")
	}
	if stepErr.Name != "process" {
		t.Errorf("StepError.Name = %q, want %q", stepErr.Name, "process")
	}
	if stepErr.Attempts != 1 {
		t.Errorf("StepError.Attempts = %d, want %d", stepErr.Attempts, 1)
	}
	// The leaf MUST be a replayedError with the user type name as string.
	leaf, ok := stepErr.Err.(*replayedError)
	if !ok {
		t.Fatalf("leaf type = %T, want *replayedError", stepErr.Err)
	}
	if leaf.errType != "MyCustomError" {
		t.Errorf("leaf errType = %q, want %q", leaf.errType, "MyCustomError")
	}
	if leaf.message != "custom failure" {
		t.Errorf("leaf message = %q, want %q", leaf.message, "custom failure")
	}
}

// TestTruncateInnerErrMessage verifies that innerErrMessage is bounded at
// maxInnerErrMessageBytes on both the ErrorData (wire) and aggregate
// checkpoint paths, that the result is valid UTF-8, and that short messages
// pass through unchanged.
func TestTruncateInnerErrMessage(t *testing.T) {
	// Build a message that exceeds the limit. Use a multi-byte rune near the
	// boundary to verify rune-safe truncation.
	base := strings.Repeat("a", maxInnerErrMessageBytes-2) + "é" // é = 2 bytes → total = 1024
	long := base + strings.Repeat("x", 100)                      // well over the limit

	t.Run("long message truncated in ErrorData", func(t *testing.T) {
		leaf := fmt.Errorf("%s", long)
		stepErr := &StepError{Name: "op", Attempts: 1, Err: leaf}

		encoded := encodeChildErrorData(stepErr)
		if encoded == nil {
			t.Fatal("encodeChildErrorData returned nil")
		}
		var d childErrorData
		if err := json.Unmarshal([]byte(*encoded), &d); err != nil {
			t.Fatalf("unmarshal ErrorData: %v", err)
		}
		if len(d.InnerErrMessage) > maxInnerErrMessageBytes {
			t.Errorf("ErrorData innerErrMessage length = %d, want <= %d", len(d.InnerErrMessage), maxInnerErrMessageBytes)
		}
		if !utf8.ValidString(d.InnerErrMessage) {
			t.Error("ErrorData innerErrMessage is not valid UTF-8")
		}
	})

	t.Run("long message truncated in aggregate payload", func(t *testing.T) {
		leaf := fmt.Errorf("%s", long)
		stepErr := &StepError{Name: "op", Attempts: 1, Err: leaf}
		batchResult := BatchResult[string]{
			Items: []BatchItem[string]{
				{Index: 0, Name: "item", Status: BatchItemFailed, Err: &ChildContextError{Name: "item", Err: stepErr}},
			},
			Reason: CompletionAllCompleted,
		}
		payload, err := fromBatchResult(context.Background(), batchResult, jsonSerdes{}, noItemSctx)
		if err != nil {
			t.Fatalf("fromBatchResult: %v", err)
		}
		cp := payload.Results[0]
		if len(cp.InnerErrMessage) > maxInnerErrMessageBytes {
			t.Errorf("aggregate InnerErrMessage length = %d, want <= %d", len(cp.InnerErrMessage), maxInnerErrMessageBytes)
		}
		if !utf8.ValidString(cp.InnerErrMessage) {
			t.Error("aggregate InnerErrMessage is not valid UTF-8")
		}
	})

	t.Run("short message unchanged", func(t *testing.T) {
		short := "connection refused"
		leaf := fmt.Errorf("%s", short)
		stepErr := &StepError{Name: "op", Attempts: 1, Err: leaf}

		encoded := encodeChildErrorData(stepErr)
		if encoded == nil {
			t.Fatal("encodeChildErrorData returned nil")
		}
		var d childErrorData
		if err := json.Unmarshal([]byte(*encoded), &d); err != nil {
			t.Fatalf("unmarshal ErrorData: %v", err)
		}
		if d.InnerErrMessage != short {
			t.Errorf("short message changed: got %q, want %q", d.InnerErrMessage, short)
		}
	})

	t.Run("truncation at rune boundary", func(t *testing.T) {
		// Place a 3-byte rune (€ = 0xE2 0x82 0xAC) exactly at the boundary.
		// 1022 bytes of 'a' + '€' (3 bytes) = 1025 bytes total → must truncate
		// to 1022 (cutting the '€' which would be split).
		prefix := strings.Repeat("a", maxInnerErrMessageBytes-2) // 1022 bytes
		msg := prefix + "€"                                      // 1025 bytes

		result := truncateUTF8(msg, maxInnerErrMessageBytes)
		if len(result) > maxInnerErrMessageBytes {
			t.Errorf("truncated length = %d, want <= %d", len(result), maxInnerErrMessageBytes)
		}
		if !utf8.ValidString(result) {
			t.Error("truncated result is not valid UTF-8")
		}
		// The € should be removed because it can't fit within the limit.
		if strings.Contains(result, "€") {
			t.Error("€ should be dropped since it crosses the byte boundary")
		}
		if result != prefix {
			t.Errorf("expected prefix of %d 'a's, got length %d", len(prefix), len(result))
		}
	})
}

// TestConcurrentAbandonedWaitDoesNotForcePending verifies that a branch
// abandoned after early completion does not force the whole invocation to
// PENDING. One branch runs a long wait (which suspends immediately, without
// a real timer) and the other succeeds at once under MinSuccessful=1. The
// wait branch is abandoned; its pending commitment must be retired so the
// invocation returns SUCCEEDED instead of PENDING, and it must not wait for
// the timer.
func TestConcurrentAbandonedWaitDoesNotForcePending(t *testing.T) {
	fake := &fakeLambda{}
	type result struct {
		Success int    `json:"successCount"`
		Started int    `json:"startedCount"`
		Total   int    `json:"totalCount"`
		Reason  string `json:"reason"`
	}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (result, error) {
		br, err := Map(ctx, "abandon-wait", []int{0, 1},
			func(c Context, _ int, index int) (string, error) {
				if index == 0 {
					// Long wait: suspends the branch immediately (the
					// duration is only recorded, never slept on) and is
					// abandonable.
					if werr := Wait(c, "long", time.Hour); werr != nil {
						return "", werr
					}
					return "waited", nil
				}
				return "fast", nil
			}, WithCompletion(CompletionConfig{MinSuccessful: 1}))
		if err != nil {
			return result{}, err
		}
		return result{
			Success: br.SuccessCount(),
			Started: br.StartedCount(),
			Total:   br.TotalCount(),
			Reason:  br.Reason.String(),
		}, nil
	})
	assertSucceeded(t, resp)
	var r result
	if err := json.Unmarshal([]byte(resp.Result), &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.Reason != "MIN_SUCCESSFUL_REACHED" {
		t.Errorf("reason = %s, want MIN_SUCCESSFUL_REACHED", r.Reason)
	}
	if r.Success != 1 {
		t.Errorf("successCount = %d, want 1", r.Success)
	}
	if r.Started != 1 {
		t.Errorf("startedCount = %d, want 1 (the abandoned wait branch)", r.Started)
	}
	if r.Total != 2 {
		t.Errorf("totalCount = %d, want 2", r.Total)
	}
}

// TestNestedAbandonedWaitDoesNotForcePending verifies that a pending
// commitment made inside a NESTED concurrent batch is retired when the outer
// batch abandons the branch that contains it. The nested batch mints its own
// abandon handle, so retirement must cascade from the outer handle to it.
//
// The interleaving is forced rather than left to the scheduler: the fast
// branch does not succeed until the nested branch has committed, so the
// outer completion decision always happens after the nested commitment
// exists.
func TestNestedAbandonedWaitDoesNotForcePending(t *testing.T) {
	fake := &fakeLambda{}
	type result struct {
		Success int    `json:"successCount"`
		Total   int    `json:"totalCount"`
		Reason  string `json:"reason"`
	}
	nestedCommitted := make(chan struct{})
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (result, error) {
		br, err := Map(ctx, "outer", []int{0, 1},
			func(c Context, _ int, index int) (string, error) {
				if index == 0 {
					// Nested batch with two items so it takes the
					// concurrent path and mints its own abandon handle.
					_, nerr := Map(c, "inner", []int{0, 1},
						func(ic Context, _ int, _ int) (string, error) {
							if werr := Wait(ic, "long", time.Hour); werr != nil {
								return "", werr
							}
							return "waited", nil
						})
					// The nested commitments now exist. Release the fast
					// branch so the completion decision follows them.
					close(nestedCommitted)
					if nerr != nil {
						return "", nerr
					}
					return "nested", nil
				}
				<-nestedCommitted
				return "fast", nil
			}, WithCompletion(CompletionConfig{MinSuccessful: 1}))
		if err != nil {
			return result{}, err
		}
		return result{
			Success: br.SuccessCount(),
			Total:   br.TotalCount(),
			Reason:  br.Reason.String(),
		}, nil
	})
	assertSucceeded(t, resp)
	var r result
	if err := json.Unmarshal([]byte(resp.Result), &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.Reason != "MIN_SUCCESSFUL_REACHED" {
		t.Errorf("reason = %s, want MIN_SUCCESSFUL_REACHED", r.Reason)
	}
	if r.Success != 1 {
		t.Errorf("successCount = %d, want 1", r.Success)
	}
}

// TestGoInsideItemCommitsAfterRetirement forces the interleaving in which a
// durable.Go branch launched inside a batch item commits to PENDING after
// the batch has retired its handle's commitments. The batch joins only its
// item workers, so the Go branch is still running when the batch returns.
// Its commitment must be dropped, not recorded against the retired handle,
// so the invocation returns SUCCEEDED exactly as it does when the branch
// commits before retirement.
//
// The ordering is forced through a callback: CreateCallback claims and
// checkpoints the operation inside the item, but the commitment is deferred
// to the first Result call. The items return only after the callback
// exists, so the completion decision and the retirement follow the claim;
// the Go body calls Result only after the handler has released it, which
// happens after Map has returned, so the commitment follows the
// retirement. The handler returns only after the Go body has unwound, so
// the commitment has landed when the outcome is decided.
func TestGoInsideItemCommitsAfterRetirement(t *testing.T) {
	fake := &fakeLambda{}
	type result struct {
		Success int    `json:"successCount"`
		Total   int    `json:"totalCount"`
		Reason  string `json:"reason"`
	}
	created := make(chan struct{})
	release := make(chan struct{})
	goDone := make(chan struct{})
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (result, error) {
		br, err := Map(ctx, "outer", []int{0, 1},
			func(c Context, _ int, index int) (string, error) {
				if index == 0 {
					Go(c, "bg", func(gc Context) (string, error) {
						defer close(goDone)
						cb, cerr := CreateCallback[string](gc, "cb")
						if cerr != nil {
							return "", cerr
						}
						close(created)
						<-release
						// The first Result call records the commitment.
						return cb.Result()
					})
				}
				<-created
				return "fast", nil
			}, WithCompletion(CompletionConfig{MinSuccessful: 1}))
		if err != nil {
			return result{}, err
		}
		// Map has returned: its handle is retired. Let the Go branch
		// commit now, and wait for it to have done so.
		close(release)
		<-goDone
		return result{
			Success: br.SuccessCount(),
			Total:   br.TotalCount(),
			Reason:  br.Reason.String(),
		}, nil
	})
	assertSucceeded(t, resp)
	var r result
	if err := json.Unmarshal([]byte(resp.Result), &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.Reason != "MIN_SUCCESSFUL_REACHED" {
		t.Errorf("reason = %s, want MIN_SUCCESSFUL_REACHED", r.Reason)
	}
	if r.Success < 1 {
		t.Errorf("successCount = %d, want at least 1", r.Success)
	}
}

// TestGoInsideItemWaitOutcomeIsStable runs, many times, a handler whose
// batch item launches a durable.Go that suspends on a Wait after the item
// body has returned, and asserts the invocation status is the same on every
// run. The Go branch's Wait may commit before or after the batch retires its
// handle, depending on scheduling; neither ordering may change the outcome.
//
// The handler returns only after the Go body has unwound, so whatever the
// branch recorded has landed when the outcome is decided. The item and its
// sibling return as soon as the Go branch has been launched, so the batch's
// early completion races the branch's Wait.
func TestGoInsideItemWaitOutcomeIsStable(t *testing.T) {
	const runs = 50
	statuses := make(map[string]int, 2)
	for i := 0; i < runs; i++ {
		fake := &fakeLambda{}
		launched := make(chan struct{})
		goDone := make(chan struct{})
		resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (string, error) {
			_, err := Map(ctx, "outer", []int{0, 1},
				func(c Context, _ int, index int) (string, error) {
					if index == 0 {
						Go(c, "bg", func(gc Context) (string, error) {
							defer close(goDone)
							if werr := Wait(gc, "long", time.Hour); werr != nil {
								return "", werr
							}
							return "waited", nil
						})
						close(launched)
					}
					<-launched
					return "fast", nil
				}, WithCompletion(CompletionConfig{MinSuccessful: 1}))
			if err != nil {
				return "", err
			}
			<-goDone
			return "done", nil
		})
		statuses[resp.Status]++
	}
	if len(statuses) != 1 {
		t.Fatalf("invocation status varied across %d runs: %v", runs, statuses)
	}
	if statuses["SUCCEEDED"] != runs {
		t.Fatalf("statuses = %v, want %d SUCCEEDED (the Go branch is under the abandoned subtree)", statuses, runs)
	}
}

// TestGoInsideNestedItemCommitsAfterOuterAbandons covers a durable.Go
// launched inside a NESTED batch's item. The nested batch completes normally
// and returns while the Go branch is still running with the nested handle.
// The outer batch then completes early and abandons the item that contained
// the nested batch. The Go branch commits only after the outer batch has
// retired its handle. The nested handle reaches the outer handle through its
// parent chain, so the commitment is dropped and the invocation returns
// SUCCEEDED, the same outcome as when the Go branch commits before
// retirement.
//
// The ordering is forced: the nested items return only after the Go branch
// has been launched, the outer sibling returns only after the nested batch
// has returned, and the Go body commits only after the outer Map has
// returned to the handler. The handler returns only after the Go body has
// unwound.
func TestGoInsideNestedItemCommitsAfterOuterAbandons(t *testing.T) {
	fake := &fakeLambda{}
	type result struct {
		Success int    `json:"successCount"`
		Total   int    `json:"totalCount"`
		Reason  string `json:"reason"`
	}
	launched := make(chan struct{})
	nestedDone := make(chan struct{})
	release := make(chan struct{})
	goDone := make(chan struct{})
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (result, error) {
		br, err := Map(ctx, "outer", []int{0, 1},
			func(c Context, _ int, index int) (string, error) {
				if index == 0 {
					// Two items so the nested batch takes the concurrent
					// path and mints its own handle beneath the outer one.
					_, nerr := Map(c, "inner", []int{0, 1},
						func(ic Context, _ int, innerIndex int) (string, error) {
							if innerIndex == 0 {
								Go(ic, "bg", func(gc Context) (string, error) {
									defer close(goDone)
									<-release
									if werr := Wait(gc, "long", time.Hour); werr != nil {
										return "", werr
									}
									return "waited", nil
								})
								close(launched)
							}
							<-launched
							return "inner", nil
						})
					close(nestedDone)
					if nerr != nil {
						return "", nerr
					}
					return "nested", nil
				}
				<-nestedDone
				return "fast", nil
			}, WithCompletion(CompletionConfig{MinSuccessful: 1}))
		if err != nil {
			return result{}, err
		}
		// The outer Map has returned and retired its handle. Let the Go
		// branch reach its Wait now, and wait for it to have unwound.
		close(release)
		<-goDone
		return result{
			Success: br.SuccessCount(),
			Total:   br.TotalCount(),
			Reason:  br.Reason.String(),
		}, nil
	})
	assertSucceeded(t, resp)
	var r result
	if err := json.Unmarshal([]byte(resp.Result), &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.Reason != "MIN_SUCCESSFUL_REACHED" {
		t.Errorf("reason = %s, want MIN_SUCCESSFUL_REACHED", r.Reason)
	}
	if r.Success < 1 {
		t.Errorf("successCount = %d, want at least 1", r.Success)
	}
}

// TestBatchStartedByGoAfterRetirementIsAbandoned covers a durable.Go
// launched inside a batch item that starts a nested batch only after the
// outer batch has completed early and retired its handle. The nested batch
// is minted beneath an already-abandoned handle, so it starts no work: its
// items never commit, and the invocation returns SUCCEEDED instead of
// depending on whether the nested batch registered before or after the
// retirement.
func TestBatchStartedByGoAfterRetirementIsAbandoned(t *testing.T) {
	fake := &fakeLambda{}
	launched := make(chan struct{})
	release := make(chan struct{})
	goDone := make(chan struct{})
	var innerErr error
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (string, error) {
		_, err := Map(ctx, "outer", []int{0, 1},
			func(c Context, _ int, index int) (string, error) {
				if index == 0 {
					Go(c, "bg", func(gc Context) (string, error) {
						defer close(goDone)
						<-release
						_, innerErr = Map(gc, "inner", []int{0, 1},
							func(ic Context, _ int, _ int) (string, error) {
								if werr := Wait(ic, "long", time.Hour); werr != nil {
									return "", werr
								}
								return "waited", nil
							})
						return "", innerErr
					})
					close(launched)
				}
				<-launched
				return "fast", nil
			}, WithCompletion(CompletionConfig{MinSuccessful: 1}))
		if err != nil {
			return "", err
		}
		close(release)
		<-goDone
		return "done", nil
	})
	assertSucceeded(t, resp)
	if !errors.Is(innerErr, errSuspendExecution) {
		t.Errorf("nested batch under an abandoned handle returned %v, want errSuspendExecution", innerErr)
	}
}

// TestSuspendedBranchHoldsConcurrencySlot verifies that a suspended batch
// worker retains its max-concurrency slot until reaching a terminal state.
// No replacement branch is admitted while the suspended branches hold their
// slots, matching JS and Python behavior.
//
// Setup: Map with maxConcurrency=2 and 3 items. Items 0 and 1 each create
// an unresolved callback (suspend). With the old (incorrect) behavior,
// each suspension releases a slot and item 2 is admitted. With the correct
// behavior, the two suspended branches hold both slots, item 2 is never
// admitted, and the invocation suspends with exactly 2 child starts.
func TestSuspendedBranchHoldsConcurrencySlot(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (string, error) {
		_, err := Map(ctx, "batch", []int{0, 1, 2},
			func(c Context, _ int, index int) (string, error) {
				if index <= 1 {
					// Both items 0 and 1 suspend on unresolved callbacks.
					cb, cerr := CreateCallback[string](c, fmt.Sprintf("cb-%d", index))
					if cerr != nil {
						return "", cerr
					}
					return cb.Result()
				}
				// Item 2 must NOT be reached.
				return "item-2-ran", nil
			}, WithMaxConcurrency(2))
		if err != nil {
			return "", err
		}
		return "done", nil
	})

	// The invocation must suspend because both admitted branches are
	// blocked on callbacks.
	if resp.Status != invocationPending {
		t.Fatalf("expected PENDING, got %s (result: %s)", resp.Status, resp.Result)
	}

	// Count child context START checkpoints (excluding the batch parent).
	// With slot-holding, only items 0 and 1 should have started.
	updates := updateBatch(t, fake)
	childStarts := 0
	for _, u := range updates {
		if u.Type == OperationTypeContext && u.Action == OperationActionStart {
			if aws.ToString(u.SubType) == operationSubTypeMapIteration {
				childStarts++
			}
		}
	}
	if childStarts != 2 {
		t.Fatalf("expected 2 child context starts (items 0,1), got %d", childStarts)
	}
}

// --- Live-to-replay error taxonomy tests ---
//
// These tests run real Map/Parallel operations whose items fail with the
// SDK's typed errors, then replay from the recorded checkpoints, asserting
// that Status(), Err(), errors.As, and errors.Is behave identically on the
// live and replay paths. Route A replays from the parent's aggregate
// payload; Route B replays mid-batch from per-child checkpoints (ErrorData).

// wfcVerdict captures the error-chain observations a handler makes on a
// BatchResult whose item failed with a WaitForConditionError.
type wfcVerdict struct {
	BatchStatus  string `json:"batchStatus"`
	ErrNonNil    bool   `json:"errNonNil"`
	AsChildCtx   bool   `json:"asChildCtx"`
	AsWFC        bool   `json:"asWfc"`
	WFCName      string `json:"wfcName"`
	WFCAttempts  int    `json:"wfcAttempts"`
	LeafContains bool   `json:"leafContains"`
}

// wfcMapHandler runs a Map where one item fails through a real
// WaitForCondition whose check function errors, and reports the observed
// error taxonomy.
func wfcMapHandler(ctx Context, _ any) (wfcVerdict, error) {
	items := []string{"a", "b"}
	br, err := Map(ctx, "wfc-map", items, func(c Context, item string, _ int) (string, error) {
		if item == "b" {
			return WaitForCondition(c, "await-sensor", func(_ StepContext, s string) (string, error) {
				return s, errors.New("sensor offline")
			}, ConditionConfig[string]{InitialState: "PENDING"})
		}
		return item, nil
	}, WithMaxConcurrency(1))
	var berr *BatchError
	if err != nil && !errors.As(err, &berr) {
		return wfcVerdict{}, err
	}
	v := wfcVerdict{BatchStatus: br.Status().String(), ErrNonNil: err != nil}
	var childErr *ChildContextError
	v.AsChildCtx = errors.As(err, &childErr)
	var wfcErr *WaitForConditionError
	if errors.As(err, &wfcErr) {
		v.AsWFC = true
		v.WFCName = wfcErr.Name
		v.WFCAttempts = wfcErr.Attempts
		v.LeafContains = wfcErr.Err != nil && strings.Contains(wfcErr.Err.Error(), "sensor offline")
	}
	return v, nil
}

// wantWFCVerdict is the taxonomy both live and replay paths must report.
var wantWFCVerdict = wfcVerdict{
	BatchStatus:  "FAILED",
	ErrNonNil:    true,
	AsChildCtx:   true,
	AsWFC:        true,
	WFCName:      "await-sensor",
	WFCAttempts:  1,
	LeafContains: true,
}

// runWFCMapLive executes the live phase and returns the verdict plus the
// recorded checkpoint updates.
func runWFCMapLive(t *testing.T) (wfcVerdict, []OperationUpdate) {
	t.Helper()
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), wfcMapHandler)
	assertSucceeded(t, resp)
	var live wfcVerdict
	if err := json.Unmarshal([]byte(resp.Result), &live); err != nil {
		t.Fatalf("unmarshal live result: %v", err)
	}
	if live != wantWFCVerdict {
		t.Fatalf("live verdict = %+v, want %+v", live, wantWFCVerdict)
	}
	return live, updateBatch(t, fake)
}

// TestMapWaitForConditionErrorLiveToReplayAggregate replays a Map whose
// item failed with a real WaitForConditionError from the parent's aggregate
// checkpoint payload (Route A) and asserts the full error taxonomy —
// concrete type, fields, and cause — matches the live run.
func TestMapWaitForConditionErrorLiveToReplayAggregate(t *testing.T) {
	live, updates := runWFCMapLive(t)

	var mapPayload string
	for _, u := range updates {
		if aws.ToString(u.SubType) == "Map" && u.Action == OperationActionSucceed {
			mapPayload = aws.ToString(u.Payload)
		}
	}
	if mapPayload == "" {
		t.Fatal("no Map SUCCEED payload found in checkpoint updates")
	}

	replayResp := invokeBatch(t, &fakeLambda{}, batchPayload(`null`,
		wireOperation{
			Id:             hashID("1"),
			Status:         "SUCCEEDED",
			Type:           "CONTEXT",
			SubType:        "Map",
			Name:           "wfc-map",
			ContextDetails: &wireContextDetails{Result: mapPayload},
		},
	), wfcMapHandler)
	assertSucceeded(t, replayResp)
	var replay wfcVerdict
	if err := json.Unmarshal([]byte(replayResp.Result), &replay); err != nil {
		t.Fatalf("unmarshal replay result: %v", err)
	}
	if replay != live {
		t.Errorf("aggregate replay verdict = %+v, live = %+v — taxonomy degraded across replay", replay, live)
	}
}

// TestMapWaitForConditionErrorLiveToReplayRouteB replays the same Map
// mid-batch: the parent is still STARTED and only the per-item child
// checkpoints (including the failed child's ErrorData) are available. The
// reconstructed error chain must match the live run.
func TestMapWaitForConditionErrorLiveToReplayRouteB(t *testing.T) {
	live, updates := runWFCMapLive(t)

	// Rebuild the checkpointed state from the recorded updates: parent
	// Map STARTED, both MapIteration children terminal.
	var ops []wireOperation
	for _, u := range updates {
		switch aws.ToString(u.SubType) {
		case "Map":
			if u.Action == OperationActionStart {
				ops = append(ops, wireOperation{
					Id:      aws.ToString(u.Id),
					Status:  "STARTED",
					Type:    "CONTEXT",
					SubType: "Map",
					Name:    aws.ToString(u.Name),
				})
			}
		case "MapIteration":
			op := wireOperation{
				Id:       aws.ToString(u.Id),
				ParentId: aws.ToString(u.ParentId),
				Type:     "CONTEXT",
				SubType:  "MapIteration",
				Name:     aws.ToString(u.Name),
			}
			switch u.Action {
			case OperationActionSucceed:
				op.Status = "SUCCEEDED"
				op.ContextDetails = &wireContextDetails{Result: aws.ToString(u.Payload)}
			case OperationActionFail:
				op.Status = "FAILED"
				op.ContextDetails = &wireContextDetails{Error: &wireFullError{
					ErrorType:    aws.ToString(u.Error.ErrorType),
					ErrorMessage: aws.ToString(u.Error.ErrorMessage),
					ErrorData:    aws.ToString(u.Error.ErrorData),
				}}
			default:
				continue // starts are superseded by the terminal update
			}
			ops = append(ops, op)
		}
	}
	if len(ops) != 3 {
		t.Fatalf("rebuilt %d wire operations, want 3 (Map STARTED + 2 terminal iterations)", len(ops))
	}

	replayResp := invokeBatch(t, &fakeLambda{}, batchPayload(`null`, ops...), wfcMapHandler)
	assertSucceeded(t, replayResp)
	var replay wfcVerdict
	if err := json.Unmarshal([]byte(replayResp.Result), &replay); err != nil {
		t.Fatalf("unmarshal replay result: %v", err)
	}
	if replay != live {
		t.Errorf("Route B replay verdict = %+v, live = %+v — taxonomy degraded across replay", replay, live)
	}
}

// taxVerdict captures the error-chain observations for a Parallel whose
// branches failed with SerdesError and a timed-out CallbackError.
type taxVerdict struct {
	BatchStatus string `json:"batchStatus"`
	ErrAsSerdes bool   `json:"errAsSerdes"`
	SerdesAs    bool   `json:"serdesAs"`
	SerdesOp    string `json:"serdesOp"`
	SerdesDir   string `json:"serdesDir"`
	CallbackAs  bool   `json:"callbackAs"`
	CbName      string `json:"cbName"`
	CbID        string `json:"cbId"`
	IsTimedOut  bool   `json:"isTimedOut"`
}

// taxParallelHandler runs a Parallel whose branches fail with the typed
// errors the batch machinery must carry across replay: a SerdesError (as a
// resumed-state decode failure inside the branch would produce) and a
// CallbackError matching ErrCallbackTimedOut (as a timed-out callback
// inside the branch would produce).
func taxParallelHandler(ctx Context, _ any) (taxVerdict, error) {
	br, err := Parallel(ctx, "tax-parallel", []Branch[string]{
		{Name: "decode", Func: func(_ Context) (string, error) {
			return "", &SerdesError{Operation: "decode-state", Direction: "unmarshal", Err: errors.New("bad json")}
		}},
		{Name: "await", Func: func(_ Context) (string, error) {
			return "", &CallbackError{Name: "ext", CallbackID: "cb-123", Err: ErrCallbackTimedOut}
		}},
	}, WithMaxConcurrency(1), WithCompletion(CompletionConfig{ToleratedFailureCount: aws.Int(2)}))
	var berr *BatchError
	if err != nil && !errors.As(err, &berr) {
		return taxVerdict{}, err
	}
	v := taxVerdict{BatchStatus: br.Status().String()}

	// The returned *BatchError unwraps to the item errors, the serdes
	// branch first.
	var errSerdes *SerdesError
	v.ErrAsSerdes = errors.As(err, &errSerdes)

	var serdesErr *SerdesError
	if errors.As(br.Items[0].Err, &serdesErr) {
		v.SerdesAs = true
		v.SerdesOp = serdesErr.Operation
		v.SerdesDir = serdesErr.Direction
	}
	var cbErr *CallbackError
	if errors.As(br.Items[1].Err, &cbErr) {
		v.CallbackAs = true
		v.CbName = cbErr.Name
		v.CbID = cbErr.CallbackID
	}
	v.IsTimedOut = errors.Is(br.Items[1].Err, ErrCallbackTimedOut)
	return v, nil
}

// TestParallelSerdesAndCallbackTaxonomyLiveToReplay asserts that
// SerdesError fields and the CallbackError timeout sentinel survive a
// Parallel replay from the aggregate checkpoint payload.
func TestParallelSerdesAndCallbackTaxonomyLiveToReplay(t *testing.T) {
	want := taxVerdict{
		BatchStatus: "FAILED",
		ErrAsSerdes: true,
		SerdesAs:    true,
		SerdesOp:    "decode-state",
		SerdesDir:   "unmarshal",
		CallbackAs:  true,
		CbName:      "ext",
		CbID:        "cb-123",
		IsTimedOut:  true,
	}

	// Phase 1: LIVE.
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), taxParallelHandler)
	assertSucceeded(t, resp)
	var live taxVerdict
	if err := json.Unmarshal([]byte(resp.Result), &live); err != nil {
		t.Fatalf("unmarshal live result: %v", err)
	}
	if live != want {
		t.Fatalf("live verdict = %+v, want %+v", live, want)
	}

	// Phase 2: REPLAY from the parent's aggregate payload.
	var parallelPayload string
	for _, u := range updateBatch(t, fake) {
		if aws.ToString(u.SubType) == "Parallel" && u.Action == OperationActionSucceed {
			parallelPayload = aws.ToString(u.Payload)
		}
	}
	if parallelPayload == "" {
		t.Fatal("no Parallel SUCCEED payload found in checkpoint updates")
	}

	replayResp := invokeBatch(t, &fakeLambda{}, batchPayload(`null`,
		wireOperation{
			Id:             hashID("1"),
			Status:         "SUCCEEDED",
			Type:           "CONTEXT",
			SubType:        "Parallel",
			Name:           "tax-parallel",
			ContextDetails: &wireContextDetails{Result: parallelPayload},
		},
	), taxParallelHandler)
	assertSucceeded(t, replayResp)
	var replay taxVerdict
	if err := json.Unmarshal([]byte(replayResp.Result), &replay); err != nil {
		t.Fatalf("unmarshal replay result: %v", err)
	}
	if replay != live {
		t.Errorf("replay verdict = %+v, live = %+v — taxonomy degraded across replay", replay, live)
	}
}

// TestCompletionReasonString asserts the wire string of every completion
// reason, the sentinel for the zero and unrecognized values, and that the
// numeric values persisted in checkpoints are unchanged.
func TestCompletionReasonString(t *testing.T) {
	cases := []struct {
		reason CompletionReason
		value  int
		want   string
	}{
		{CompletionAllCompleted, 1, "ALL_COMPLETED"},
		{CompletionMinSuccessfulReached, 2, "MIN_SUCCESSFUL_REACHED"},
		{CompletionFailureToleranceExceeded, 3, "FAILURE_TOLERANCE_EXCEEDED"},
		{CompletionCustomSucceeded, 4, "CUSTOM_COMPLETION_SUCCEEDED"},
		{CompletionCustomFailed, 5, "CUSTOM_COMPLETION_FAILED"},
		{CompletionReason(0), 0, "UNKNOWN"},
		{CompletionReason(99), 99, "UNKNOWN"},
	}
	for _, tc := range cases {
		if int(tc.reason) != tc.value {
			t.Errorf("%s: numeric value = %d, want %d", tc.want, int(tc.reason), tc.value)
		}
		if got := tc.reason.String(); got != tc.want {
			t.Errorf("CompletionReason(%d).String() = %q, want %q", int(tc.reason), got, tc.want)
		}
	}
}

// TestBatchResultLookupByName covers Result and Item on a BatchResult built
// from its exported fields: a hit, a miss, a failed item, a started item,
// and duplicate names resolving to the first in input order.
func TestBatchResultLookupByName(t *testing.T) {
	failure := errors.New("boom")
	br := BatchResult[string]{
		Items: []BatchItem[string]{
			{Index: 0, Name: "label", Status: BatchItemSucceeded, Result: "L"},
			{Index: 1, Name: "tracking", Status: BatchItemFailed, Err: failure},
			{Index: 2, Name: "dup", Status: BatchItemFailed, Err: failure},
			{Index: 3, Name: "dup", Status: BatchItemSucceeded, Result: "second"},
			{Index: 4, Name: "slow", Status: BatchItemStarted},
		},
		Reason: CompletionAllCompleted,
	}

	if v, ok := br.Result("label"); !ok || v != "L" {
		t.Errorf(`Result("label") = %q, %v; want "L", true`, v, ok)
	}
	if v, ok := br.Result("missing"); ok || v != "" {
		t.Errorf(`Result("missing") = %q, %v; want "", false`, v, ok)
	}
	if v, ok := br.Result("tracking"); ok || v != "" {
		t.Errorf(`Result("tracking") on a failed item = %q, %v; want "", false`, v, ok)
	}
	if v, ok := br.Result("slow"); ok || v != "" {
		t.Errorf(`Result("slow") on a started item = %q, %v; want "", false`, v, ok)
	}
	// Duplicate names: the first in input order is the match. Here it
	// failed, so Result reports no success even though a later item with
	// the same name succeeded.
	if v, ok := br.Result("dup"); ok || v != "" {
		t.Errorf(`Result("dup") = %q, %v; want "", false (first match failed)`, v, ok)
	}

	if item := br.Item("tracking"); item == nil || item.Index != 1 || item.Err != failure {
		t.Errorf(`Item("tracking") = %+v, want index 1 with its error`, item)
	}
	if item := br.Item("dup"); item == nil || item.Index != 2 {
		t.Errorf(`Item("dup") = %+v, want the first match (index 2)`, item)
	}
	if item := br.Item("missing"); item != nil {
		t.Errorf(`Item("missing") = %+v, want nil`, item)
	}
	// Item returns a pointer into Items.
	if item := br.Item("label"); item != &br.Items[0] {
		t.Error(`Item("label") does not point into Items`)
	}
}

// TestBatchResultStartedAccessors covers Started and StartedCount on a
// BatchResult built from its exported fields, and their relationship to
// TotalCount.
func TestBatchResultStartedAccessors(t *testing.T) {
	br := BatchResult[string]{
		Items: []BatchItem[string]{
			{Index: 0, Name: "a", Status: BatchItemSucceeded, Result: "A"},
			{Index: 1, Name: "b", Status: BatchItemStarted},
			{Index: 2, Name: "c", Status: BatchItemFailed, Err: errors.New("boom")},
			{Index: 3, Name: "d", Status: BatchItemStarted},
		},
		Reason: CompletionMinSuccessfulReached,
	}

	started := br.Started()
	if len(started) != 2 || started[0].Index != 1 || started[1].Index != 3 {
		t.Errorf("Started() = %+v, want items 1 and 3 in input order", started)
	}
	if got := br.StartedCount(); got != 2 {
		t.Errorf("StartedCount() = %d, want 2", got)
	}
	if got := br.SuccessCount() + br.FailureCount() + br.StartedCount(); got != br.TotalCount() {
		t.Errorf("SuccessCount+FailureCount+StartedCount = %d, TotalCount = %d", got, br.TotalCount())
	}

	empty := BatchResult[string]{Reason: CompletionAllCompleted}
	if got := empty.Started(); len(got) != 0 {
		t.Errorf("Started() on an empty result = %+v, want none", got)
	}
	if got := empty.StartedCount(); got != 0 {
		t.Errorf("StartedCount() on an empty result = %d, want 0", got)
	}
}
