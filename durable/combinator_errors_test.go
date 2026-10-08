// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// failing returns a future whose step fails at once with no retry.
func failing(ctx durable.Context, name string) *durable.Future[int] {
	return durable.StepAsync(ctx, name, func(durable.StepContext) (int, error) {
		return 0, errors.New("boom")
	}, durable.WithRetry(durable.NoRetry()))
}

// capture runs run inside a handler, returns the error run produced, and
// does not fail the execution: the handler returns nil, so the combinator's
// own error is observed on its own.
func capture(t *testing.T, run func(ctx durable.Context) error) error {
	t.Helper()
	var got error
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		got = run(ctx)
		return "done", nil
	}
	if _, err := durabletest.NewLocalRunner(h).RunUntilComplete(struct{}{}); err != nil {
		t.Fatal(err)
	}
	return got
}

// TestCombinatorFailureIsCombinatorError asserts that a failure of All,
// Any, Race, Join, or Select surfaces as a *ChildContextError whose
// ErrorType is "PromiseCombinatorError" and whose cause is a
// *CombinatorError.
func TestCombinatorFailureIsCombinatorError(t *testing.T) {
	cases := []struct {
		name    string
		wantMsg string // the failure message must contain this
		run     func(ctx durable.Context) error
	}{
		{"All", "boom", func(ctx durable.Context) error {
			_, err := durable.All(ctx, "all", []*durable.Future[int]{failing(ctx, "a"), failing(ctx, "b")})
			return err
		}},
		{"Any", "All promises were rejected", func(ctx durable.Context) error {
			_, err := durable.Any(ctx, "any", []*durable.Future[int]{failing(ctx, "a"), failing(ctx, "b")})
			return err
		}},
		{"Race", "boom", func(ctx durable.Context) error {
			_, err := durable.Race(ctx, "race", []*durable.Future[int]{failing(ctx, "a")})
			return err
		}},
		{"Join", "boom", func(ctx durable.Context) error {
			return durable.Join(ctx, "join", []durable.Awaitable{failing(ctx, "a"), failing(ctx, "b")})
		}},
		{"Select", "boom", func(ctx durable.Context) error {
			_, _, err := durable.Select(ctx, "sel", []durable.Branch[int]{
				{Name: "x", Func: func(c durable.Context) (int, error) {
					return durable.Step(c, "s", func(durable.StepContext) (int, error) {
						return 0, errors.New("boom")
					}, durable.WithRetry(durable.NoRetry()))
				}},
			})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := capture(t, tc.run)
			if got == nil {
				t.Fatalf("%s: want a non-nil error, got nil", tc.name)
			}
			var childErr *durable.ChildContextError
			if !errors.As(got, &childErr) {
				t.Fatalf("%s: want a *ChildContextError, got %T: %v", tc.name, got, got)
			}
			if childErr.ErrorType != "PromiseCombinatorError" {
				t.Errorf("%s: ChildContextError.ErrorType = %q, want %q", tc.name, childErr.ErrorType, "PromiseCombinatorError")
			}
			var combErr *durable.CombinatorError
			if !errors.As(got, &combErr) {
				t.Fatalf("%s: errors.As(err, &*CombinatorError) = false, want true", tc.name)
			}
			if !strings.Contains(got.Error(), tc.wantMsg) {
				t.Errorf("%s: message = %q, want it to contain %q", tc.name, got.Error(), tc.wantMsg)
			}
		})
	}
}

// TestAnyMessageIsAllPromisesWereRejected asserts that the error from a
// fully failed Any reads "All promises were rejected", matching the
// JavaScript SDK.
func TestAnyMessageIsAllPromisesWereRejected(t *testing.T) {
	got := capture(t, func(ctx durable.Context) error {
		_, err := durable.Any(ctx, "any", []*durable.Future[int]{failing(ctx, "a"), failing(ctx, "b")})
		return err
	})
	if got == nil {
		t.Fatal("want a non-nil error, got nil")
	}
	if !strings.Contains(got.Error(), "All promises were rejected") {
		t.Errorf("Any error = %q, want it to contain %q", got.Error(), "All promises were rejected")
	}
}

// TestRaceEmptyReturnsImmediately asserts that Race with no futures returns
// an error at once rather than suspending, so the execution reaches a
// terminal status.
func TestRaceEmptyReturnsImmediately(t *testing.T) {
	h := func(ctx durable.Context, _ struct{}) (int, error) {
		_, err := durable.Race(ctx, "race", []*durable.Future[int](nil))
		return 0, err
	}
	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Failed {
		t.Errorf("Race(nil): execution status = %s, want %s (an immediate error, not a suspension)", r.Status, durabletest.Failed)
	}
}

// TestAnyEmptyReturnsImmediately asserts that Any with no futures fails at
// once with the message "All promises were rejected".
func TestAnyEmptyReturnsImmediately(t *testing.T) {
	got := capture(t, func(ctx durable.Context) error {
		_, err := durable.Any(ctx, "any", []*durable.Future[int](nil))
		return err
	})
	if got == nil {
		t.Fatal("want a non-nil error, got nil")
	}
	var combErr *durable.CombinatorError
	if !errors.As(got, &combErr) {
		t.Fatalf("Any(nil): errors.As(err, &*CombinatorError) = false, want true (got %T)", got)
	}
	if !strings.Contains(got.Error(), "All promises were rejected") {
		t.Errorf("Any(nil) error = %q, want it to contain %q", got.Error(), "All promises were rejected")
	}
}

// TestAllSettledItemsCarryErr asserts that AllSettled never fails for an
// item failure: it resolves, and the rejected member's error is read from
// Settled[i].Err.
func TestAllSettledItemsCarryErr(t *testing.T) {
	var settledErr error
	var topErr error
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		settled, err := durable.AllSettled(ctx, "collect", []*durable.Future[int]{failing(ctx, "a")})
		if err != nil {
			topErr = err
			return "", err
		}
		settledErr = settled[0].Err
		return "done", nil
	}
	if _, err := durabletest.NewLocalRunner(h).RunUntilComplete(struct{}{}); err != nil {
		t.Fatal(err)
	}
	if topErr != nil {
		t.Fatalf("AllSettled returned a top-level error for an item failure: %v", topErr)
	}
	if settledErr == nil {
		t.Fatal("AllSettled: Settled[0].Err = nil, want the item's error")
	}
	var stepErr *durable.StepError
	if !errors.As(settledErr, &stepErr) {
		t.Errorf("AllSettled: Settled[0].Err = %T, want it to match *StepError", settledErr)
	}
}

// TestNestedCombinatorErrorIsWrapped asserts that All and Join wrap a
// future's *CombinatorError like any other failure. The future here
// returns a *CombinatorError through WithChildErrorMapper. The outer
// combinator's *CombinatorError must hold exactly that one error in
// Errors, so the inner error keeps its own place in the chain. The
// recorded failure is rebuilt the same way on the first run and on
// replay, so the inner error is identified by its recorded message.
func TestNestedCombinatorErrorIsWrapped(t *testing.T) {
	inner := func(ctx durable.Context) *durable.Future[int] {
		return durable.RunInChildContextAsync(ctx, "inner", func(c durable.Context) (int, error) {
			return durable.Step(c, "s", func(durable.StepContext) (int, error) {
				return 0, errors.New("boom")
			}, durable.WithRetry(durable.NoRetry()))
		}, durable.WithChildErrorMapper(func(err *durable.ChildContextError) error {
			return &durable.CombinatorError{Name: "inner", Errors: []error{err}}
		}))
	}
	cases := []struct {
		name string
		run  func(ctx durable.Context) error
	}{
		{"All", func(ctx durable.Context) error {
			_, err := durable.All(ctx, "outer", []*durable.Future[int]{inner(ctx)})
			return err
		}},
		{"Join", func(ctx durable.Context) error {
			return durable.Join(ctx, "outer", []durable.Awaitable{inner(ctx)})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := capture(t, tc.run)
			var childErr *durable.ChildContextError
			if !errors.As(got, &childErr) || childErr.Name != "outer" || childErr.ErrorType != "PromiseCombinatorError" {
				t.Fatalf("err = %T %v, want a *ChildContextError named outer with ErrorType PromiseCombinatorError", got, got)
			}
			outer, ok := childErr.Err.(*durable.CombinatorError)
			if !ok {
				t.Fatalf("cause = %T, want *CombinatorError", childErr.Err)
			}
			if outer.Name != "outer" {
				t.Errorf("outer CombinatorError.Name = %q, want %q", outer.Name, "outer")
			}
			if len(outer.Errors) != 1 {
				t.Fatalf("outer CombinatorError.Errors has %d entries, want 1: %v", len(outer.Errors), outer.Errors)
			}
			const innerMsg = `durable: combinator "inner": all futures failed (1 errors)`
			if msg := outer.Errors[0].Error(); msg != "PromiseCombinatorError: "+innerMsg {
				t.Errorf("outer CombinatorError.Errors[0] = %q, want the rebuilt inner error %q", msg, "PromiseCombinatorError: "+innerMsg)
			}
			if outer.Error() != innerMsg {
				t.Errorf("outer message = %q, want the first error's message %q", outer.Error(), innerMsg)
			}
		})
	}
}
