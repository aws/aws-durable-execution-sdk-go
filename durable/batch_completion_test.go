package durable_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func intPtr(n int) *int { return &n }

func runToCompletion(t *testing.T, h func(durable.Context, any) (int, error)) *durabletest.TestResult {
	t.Helper()
	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// failSecondItem is a Map function whose item at index 1 fails and whose
// other items return the item value.
func failSecondItem(c durable.Context, item, idx int) (int, error) {
	if idx == 1 {
		return durable.Step(c, "s", func(_ durable.StepContext) (int, error) {
			return 0, errors.New("boom")
		}, durable.WithRetry(durable.NoRetry()))
	}
	return durable.Step(c, "s", func(_ durable.StepContext) (int, error) { return item, nil })
}

func succeedItem(c durable.Context, item, _ int) (int, error) {
	return durable.Step(c, "s", func(_ durable.StepContext) (int, error) { return item, nil })
}

// TestToleratedFailureReturnsNilError asserts that a Map whose single failed
// item is within ToleratedFailureCount returns (result, nil).
func TestToleratedFailureReturnsNilError(t *testing.T) {
	var gotErr error
	var failed int
	var reason durable.CompletionReason
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", []int{0, 1, 2}, failSecondItem,
			durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: intPtr(1)}),
			durable.WithMaxConcurrency(1))
		gotErr = err
		failed = len(res.Failed())
		reason = res.Reason
		return res.SuccessCount(), nil
	}
	r := runToCompletion(t, h)
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (handler ignored the batch error)", r.Status)
	}
	if gotErr != nil {
		t.Errorf("Map returned err = %v, want nil for a tolerated failure (reason %s)", gotErr, reason)
	}
	if failed != 1 {
		t.Errorf("result.Failed() has %d items, want 1", failed)
	}
	if reason != durable.CompletionAllCompleted {
		t.Errorf("reason = %s, want ALL_COMPLETED", reason)
	}
}

// TestBreachReturnsBatchError asserts that a real breach still returns a
// *BatchError whose Reason is FAILURE_TOLERANCE_EXCEEDED.
func TestBreachReturnsBatchError(t *testing.T) {
	var gotErr error
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", []int{0, 1, 2}, failSecondItem,
			durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: intPtr(0)}),
			durable.WithMaxConcurrency(1))
		gotErr = err
		return res.SuccessCount(), nil
	}
	runToCompletion(t, h)
	var be *durable.BatchError
	if !errors.As(gotErr, &be) {
		t.Fatalf("Map returned err = %v, want *BatchError", gotErr)
	}
	if be.Reason != durable.CompletionFailureToleranceExceeded {
		t.Errorf("BatchError.Reason = %s, want FAILURE_TOLERANCE_EXCEEDED", be.Reason)
	}
}

// TestBreachVariantsReturnBatchError asserts that a percentage breach and
// the fail-fast default return a *BatchError with
// FAILURE_TOLERANCE_EXCEEDED.
func TestBreachVariantsReturnBatchError(t *testing.T) {
	cases := map[string][]durable.BatchOption{
		"percentage": {durable.WithCompletion(durable.CompletionConfig{ToleratedFailurePercentage: intPtr(10)})},
		"fail-fast":  nil,
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			var gotErr error
			h := func(ctx durable.Context, _ any) (int, error) {
				all := append([]durable.BatchOption{durable.WithMaxConcurrency(1)}, opts...)
				_, err := durable.Map(ctx, "m", []int{0, 1, 2}, failSecondItem, all...)
				gotErr = err
				return 0, nil
			}
			runToCompletion(t, h)
			var be *durable.BatchError
			if !errors.As(gotErr, &be) {
				t.Fatalf("Map returned err = %v, want *BatchError", gotErr)
			}
			if be.Reason != durable.CompletionFailureToleranceExceeded {
				t.Errorf("BatchError.Reason = %s, want FAILURE_TOLERANCE_EXCEEDED", be.Reason)
			}
		})
	}
}

// TestCustomFailedReturnsBatchCompletionError asserts that a batch
// completed by a CompleteBatch(CompletionOutcomeFailed) decision with no
// failed item returns a *BatchCompletionError whose Reason is
// CUSTOM_COMPLETION_FAILED.
func TestCustomFailedReturnsBatchCompletionError(t *testing.T) {
	var gotErr error
	var total int
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", []int{0, 1, 2}, succeedItem,
			durable.WithMaxConcurrency(1),
			durable.WithCompletion(durable.CompletionConfig{
				ShouldComplete: func(p durable.BatchProgress) durable.CompletionDecision {
					if p.SuccessCount >= 1 {
						return durable.CompleteBatch(durable.CompletionOutcomeFailed)
					}
					return durable.ContinueBatch()
				},
			}))
		gotErr = err
		total = res.TotalCount()
		return 0, nil
	}
	runToCompletion(t, h)
	var be *durable.BatchCompletionError
	if !errors.As(gotErr, &be) {
		t.Fatalf("Map returned err = %v, want *BatchCompletionError", gotErr)
	}
	if be.Reason != durable.CompletionCustomFailed {
		t.Errorf("BatchCompletionError.Reason = %s, want CUSTOM_COMPLETION_FAILED", be.Reason)
	}
	if total != 1 {
		t.Errorf("result.TotalCount() = %d, want 1", total)
	}
}

// TestMinSuccessfulEarlyReportsMinSuccessfulReached asserts that a batch
// that meets MinSuccessful with items still unstarted reports
// MIN_SUCCESSFUL_REACHED and returns a nil error, also when an item
// failed before the threshold was met.
func TestMinSuccessfulEarlyReportsMinSuccessfulReached(t *testing.T) {
	var gotErr error
	var reason durable.CompletionReason
	var total, failed int
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", []int{0, 1, 2, 3, 4}, failSecondItem,
			durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 2}),
			durable.WithMaxConcurrency(1))
		gotErr = err
		reason = res.Reason
		total = res.TotalCount()
		failed = len(res.Failed())
		return res.SuccessCount(), nil
	}
	r := runToCompletion(t, h)
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", r.Status)
	}
	if gotErr != nil {
		t.Errorf("Map returned err = %v, want nil", gotErr)
	}
	if reason != durable.CompletionMinSuccessfulReached {
		t.Errorf("reason = %s, want MIN_SUCCESSFUL_REACHED", reason)
	}
	if total != 3 || failed != 1 {
		t.Errorf("TotalCount() = %d, Failed() = %d; want 3 and 1", total, failed)
	}
}

// TestMinSuccessfulOnLastItemReportsAllCompleted asserts that a batch whose
// last item satisfies MinSuccessful (so every item ran) reports
// ALL_COMPLETED, not MIN_SUCCESSFUL_REACHED.
func TestMinSuccessfulOnLastItemReportsAllCompleted(t *testing.T) {
	var reason durable.CompletionReason
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", []int{0, 1}, succeedItem,
			durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 2}),
			durable.WithMaxConcurrency(1))
		if err != nil {
			return 0, err
		}
		reason = res.Reason
		return res.SuccessCount(), nil
	}
	r := runToCompletion(t, h)
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", r.Status)
	}
	if reason != durable.CompletionAllCompleted {
		t.Errorf("reason = %s, want ALL_COMPLETED (every item ran)", reason)
	}
}

// TestShouldCompleteConsultedAtZeroProgress asserts that a ShouldComplete
// callback that completes the batch at once runs zero item bodies and returns
// an empty result.
func TestShouldCompleteConsultedAtZeroProgress(t *testing.T) {
	for _, concurrency := range []int{1, 3} {
		var bodies atomic.Int64
		var total, consultedProgress, consultedTotal int
		var consultedStatuses []durable.BatchItemStatus
		var reason durable.CompletionReason
		h := func(ctx durable.Context, _ any) (int, error) {
			res, err := durable.Map(ctx, "m", []int{0, 1, 2},
				func(c durable.Context, item, i int) (int, error) {
					bodies.Add(1)
					return succeedItem(c, item, i)
				},
				durable.WithMaxConcurrency(concurrency),
				durable.WithCompletion(durable.CompletionConfig{
					ShouldComplete: func(p durable.BatchProgress) durable.CompletionDecision {
						consultedProgress = p.CompletedCount
						consultedTotal = p.TotalCount
						consultedStatuses = nil
						for _, it := range p.Items {
							consultedStatuses = append(consultedStatuses, it.Status)
						}
						return durable.CompleteBatch(durable.CompletionOutcomeSucceeded)
					},
				}))
			if err != nil {
				return 0, err
			}
			total = res.TotalCount()
			reason = res.Reason
			return res.SuccessCount(), nil
		}
		r := runToCompletion(t, h)
		if r.Status != durabletest.Succeeded {
			t.Fatalf("concurrency %d: status = %s, want SUCCEEDED", concurrency, r.Status)
		}
		if bodies.Load() != 0 {
			t.Errorf("concurrency %d: item bodies run = %d, want 0 (completed at zero progress)", concurrency, bodies.Load())
		}
		if total != 0 {
			t.Errorf("concurrency %d: result.TotalCount() = %d, want 0", concurrency, total)
		}
		if consultedProgress != 0 || consultedTotal != 3 {
			t.Errorf("concurrency %d: first ShouldComplete saw CompletedCount = %d, TotalCount = %d; want 0 and 3", concurrency, consultedProgress, consultedTotal)
		}
		for i, s := range consultedStatuses {
			if s != durable.BatchItemNotStarted {
				t.Errorf("concurrency %d: item %d status = %s, want NOT_STARTED", concurrency, i, s)
			}
		}
		if reason != durable.CompletionCustomSucceeded {
			t.Errorf("concurrency %d: reason = %s, want CUSTOM_COMPLETION_SUCCEEDED", concurrency, reason)
		}
	}
}

// TestShouldCompleteZeroProgressFailed asserts that a zero-progress
// CompleteBatch(CompletionOutcomeFailed) runs no item and returns a
// *BatchCompletionError with CUSTOM_COMPLETION_FAILED and an empty result.
func TestShouldCompleteZeroProgressFailed(t *testing.T) {
	var gotErr error
	var total int
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Parallel(ctx, "p", []durable.Branch[int]{
			{Func: func(c durable.Context) (int, error) { return succeedItem(c, 1, 0) }},
			{Func: func(c durable.Context) (int, error) { return succeedItem(c, 2, 1) }},
		}, durable.WithCompletion(durable.CompletionConfig{
			ShouldComplete: func(durable.BatchProgress) durable.CompletionDecision {
				return durable.CompleteBatch(durable.CompletionOutcomeFailed)
			},
		}))
		gotErr = err
		total = res.TotalCount()
		return 0, nil
	}
	runToCompletion(t, h)
	var be *durable.BatchCompletionError
	if !errors.As(gotErr, &be) {
		t.Fatalf("Parallel returned err = %v, want *BatchCompletionError", gotErr)
	}
	if be.Reason != durable.CompletionCustomFailed {
		t.Errorf("BatchCompletionError.Reason = %s, want CUSTOM_COMPLETION_FAILED", be.Reason)
	}
	if total != 0 {
		t.Errorf("result.TotalCount() = %d, want 0", total)
	}
}

// TestCountBasedShouldCompleteRunsUntilThreshold asserts that a
// ShouldComplete that completes once SuccessCount reaches 2 continues at
// zero progress, runs items until two succeed, and then stops.
func TestCountBasedShouldCompleteRunsUntilThreshold(t *testing.T) {
	var bodies atomic.Int64
	var calls int
	var total, succeeded int
	var reason durable.CompletionReason
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", []int{0, 1, 2, 3, 4},
			func(c durable.Context, item, i int) (int, error) {
				bodies.Add(1)
				return failSecondItem(c, item, i)
			},
			durable.WithMaxConcurrency(1),
			durable.WithCompletion(durable.CompletionConfig{
				ShouldComplete: func(p durable.BatchProgress) durable.CompletionDecision {
					calls++
					if p.SuccessCount >= 2 {
						return durable.CompleteBatch(durable.CompletionOutcomeSucceeded)
					}
					return durable.ContinueBatch()
				},
			}))
		if err != nil {
			return 0, err
		}
		total = res.TotalCount()
		succeeded = res.SuccessCount()
		reason = res.Reason
		return succeeded, nil
	}
	r := runToCompletion(t, h)
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", r.Status)
	}
	// Items 0 (success), 1 (failure), and 2 (success) run; 3 and 4 do not.
	if bodies.Load() != 3 || total != 3 || succeeded != 2 {
		t.Errorf("bodies = %d, TotalCount() = %d, SuccessCount() = %d; want 3, 3, 2", bodies.Load(), total, succeeded)
	}
	// One zero-progress consult plus one per terminal item.
	if calls != 4 {
		t.Errorf("ShouldComplete calls = %d, want 4", calls)
	}
	if reason != durable.CompletionCustomSucceeded {
		t.Errorf("reason = %s, want CUSTOM_COMPLETION_SUCCEEDED", reason)
	}
}

// TestZeroProgressCompletionReplays asserts that a replay of a batch
// completed at zero progress returns the same empty result and reason
// without calling ShouldComplete again. It also asserts that the
// operations after the batch keep their IDs on replay, under every
// concurrency setting: the Wait is recorded once and the Step after it
// runs once.
func TestZeroProgressCompletionReplays(t *testing.T) {
	cases := []struct {
		name string
		opts []durable.BatchOption
	}{
		{name: "default"},
		{name: "concurrency-2", opts: []durable.BatchOption{durable.WithMaxConcurrency(2)}},
		{name: "sequential", opts: []durable.BatchOption{durable.WithMaxConcurrency(1)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls, stepBodies atomic.Int64
			var totals []int
			var reasons []durable.CompletionReason
			h := func(ctx durable.Context, _ any) (int, error) {
				opts := append([]durable.BatchOption{
					durable.WithCompletion(durable.CompletionConfig{
						ShouldComplete: func(durable.BatchProgress) durable.CompletionDecision {
							calls.Add(1)
							return durable.CompleteBatch(durable.CompletionOutcomeSucceeded)
						},
					}),
				}, tc.opts...)
				res, err := durable.Map(ctx, "m", []int{0, 1, 2}, succeedItem, opts...)
				if err != nil {
					return 0, err
				}
				totals = append(totals, res.TotalCount())
				reasons = append(reasons, res.Reason)
				if err := durable.Wait(ctx, "w", time.Second); err != nil {
					return 0, err
				}
				return durable.Step(ctx, "s", func(durable.StepContext) (int, error) {
					stepBodies.Add(1)
					return res.TotalCount(), nil
				})
			}
			r := runToCompletion(t, h)
			if r.Status != durabletest.Succeeded {
				t.Fatalf("status = %s, want SUCCEEDED", r.Status)
			}
			// One invocation suspends on the Wait; the second replays the
			// batch and the Wait and runs the Step.
			if len(totals) != 2 {
				t.Fatalf("handler ran %d times, want 2", len(totals))
			}
			for i := range totals {
				if totals[i] != 0 || reasons[i] != durable.CompletionCustomSucceeded {
					t.Errorf("run %d: TotalCount() = %d, reason = %s; want 0 and CUSTOM_COMPLETION_SUCCEEDED", i, totals[i], reasons[i])
				}
			}
			if calls.Load() != 1 {
				t.Errorf("ShouldComplete calls = %d, want 1", calls.Load())
			}
			if stepBodies.Load() != 1 {
				t.Errorf("step body ran %d times, want 1", stepBodies.Load())
			}
			counts := map[string]int{}
			for _, op := range r.Operations {
				counts[op.Name]++
			}
			if counts["w"] != 1 || counts["s"] != 1 {
				t.Errorf("recorded operations named w = %d, s = %d; want 1 each", counts["w"], counts["s"])
			}
		})
	}
}

// TestToleratedFailureReplaysWithNilError asserts that a replay of a
// batch with a tolerated failure returns a nil error again.
func TestToleratedFailureReplaysWithNilError(t *testing.T) {
	var errs []error
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Parallel(ctx, "p", []durable.Branch[int]{
			{Func: func(c durable.Context) (int, error) { return failSecondItem(c, 0, 0) }},
			{Func: func(c durable.Context) (int, error) { return failSecondItem(c, 1, 1) }},
		}, durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: intPtr(1)}))
		errs = append(errs, err)
		if err != nil {
			return 0, err
		}
		if err := durable.Wait(ctx, "w", time.Second); err != nil {
			return 0, err
		}
		return res.FailureCount(), nil
	}
	r := runToCompletion(t, h)
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", r.Status)
	}
	if len(errs) < 2 {
		t.Fatalf("handler ran %d times, want a replay", len(errs))
	}
	for i, err := range errs {
		if err != nil {
			t.Errorf("run %d: Parallel returned err = %v, want nil", i, err)
		}
	}
}
