package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
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
	got, err := h.Invoke(context.Background(), payload)
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
		}, WithMaxConcurrency(1), WithCompletion(WithToleratedFailureCount(0)))
		if err != nil {
			return result{}, err
		}
		return result{
			Reason:  br.Reason.String(),
			Status:  br.Status(),
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
		}, WithMaxConcurrency(1), WithCompletion(CompletionConfig{ToleratedFailurePercentage: 25}))
		if err != nil {
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

func TestMapThrowIfError(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (string, error) {
		items := []string{"fail", "never"}
		br, err := Map(ctx, "throwing", items, func(_ Context, item string, _ int) (string, error) {
			if item == "fail" {
				return "", errors.New("item failed")
			}
			return item, nil
		}, WithMaxConcurrency(1), WithCompletion(WithToleratedFailureCount(0)))
		if err != nil {
			return "", err
		}
		if throwErr := br.ThrowIfError(); throwErr != nil {
			return "", throwErr
		}
		return "should not reach", nil
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
		}, WithMaxConcurrency(1), WithItemNamer(func(_ any, i int) string {
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
		}, WithMaxConcurrency(1), WithCompletion(WithToleratedFailureCount(0)))
		if err != nil {
			return result{}, err
		}
		return result{
			Reason:  br.Reason.String(),
			Status:  br.Status(),
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

func TestParallelThrowIfError(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) (string, error) {
		br, err := Parallel(ctx, "throwing", []Branch[string]{
			{Func: func(_ Context) (string, error) { return "", errors.New("branch failed") }},
			{Func: func(_ Context) (string, error) { return "never", nil }},
		}, WithMaxConcurrency(1), WithCompletion(WithToleratedFailureCount(0)))
		if err != nil {
			return "", err
		}
		if throwErr := br.ThrowIfError(); throwErr != nil {
			return "", throwErr
		}
		return "should not reach", nil
	})
	assertFailed(t, resp)
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
			ToleratedFailureCount:    1,
			toleratedFailureCountSet: true,
		}))
		if err != nil {
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
		if aws.ToString(u.SubType) == "Map" && u.Action == types.OperationActionSucceed {
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
			ToleratedFailureCount:    1,
			toleratedFailureCountSet: true,
		}))
		if err != nil {
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

func (wrapSerdes) Marshal(_ SerdesContext, v any) ([]byte, error) {
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

func (wrapSerdes) Unmarshal(_ SerdesContext, data []byte, v any) error {
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
		if aws.ToString(u.SubType) == "Map" && u.Action == types.OperationActionSucceed {
			mapSucceedPayload = aws.ToString(u.Payload)
		}
	}
	if !strings.HasPrefix(mapSucceedPayload, "OPSERDE:") {
		t.Fatalf("expected Map success payload to start with OPSERDE:, got %q", mapSucceedPayload)
	}
}

// opSerdes is the operation-level serdes for testing: "OPSERDE:X,Y" format.
type opSerdes struct{}

func (opSerdes) Marshal(_ SerdesContext, v any) ([]byte, error) {
	br, ok := v.(BatchResult[string])
	if !ok {
		return nil, fmt.Errorf("opSerdes.Marshal: unexpected type %T", v)
	}
	results := br.Results()
	return []byte("OPSERDE:" + strings.Join(results, ",")), nil
}

func (opSerdes) Unmarshal(_ SerdesContext, data []byte, v any) error {
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
			ToleratedFailureCount:    1,
			toleratedFailureCountSet: true,
		}))
		if err != nil {
			return result{}, err
		}
		return result{
			Reason:  br.Reason.String(),
			Status:  br.Status(),
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
			payload, err := fromBatchResult(tt.result, serdes, SerdesContext{})
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
			got, err := toBatchResult[string](decoded, serdes, SerdesContext{})
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
