// Command future-combinators-mixed demonstrates all four combinators in a
// single workflow: All, Race, AllSettled, and Any, each operating on
// different sets of futures.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result collects outcomes from each combinator stage.
type Result struct {
	AllResults   []string `json:"allResults"`
	RaceResult   string   `json:"raceResult"`
	SettledCount int      `json:"settledCount"`
	AnyResult    string   `json:"anyResult"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// Stage 1: All — all succeed.
	a1 := durable.StepAsync(ctx, "all-1", func(_ durable.StepContext) (string, error) {
		return "Result from step 1", nil
	})
	a2 := durable.StepAsync(ctx, "all-2", func(_ durable.StepContext) (string, error) {
		return "Result from step 2", nil
	})
	a3 := durable.StepAsync(ctx, "all-3", func(_ durable.StepContext) (string, error) {
		return "Result from step 3", nil
	})
	allResults, err := durable.All(ctx, "all-steps", []*durable.Future[string]{a1, a2, a3})
	if err != nil {
		return Result{}, err
	}

	// Stage 2: Race — fast wins.
	r1 := durable.StepAsync(ctx, "race-slow", func(_ durable.StepContext) (string, error) {
		return "Slow result", nil
	})
	r2 := durable.StepAsync(ctx, "race-fast", func(_ durable.StepContext) (string, error) {
		return "Fast result", nil
	})
	raceResult, err := durable.Race(ctx, "race", []*durable.Future[string]{r1, r2})
	if err != nil {
		return Result{}, err
	}

	// Stage 3: AllSettled — mix of success and failure.
	s1 := durable.StepAsync(ctx, "settled-ok", func(_ durable.StepContext) (string, error) {
		return "Success!", nil
	})
	s2 := durable.StepAsync(ctx, "settled-fail", func(_ durable.StepContext) (string, error) {
		return "", errors.New("this step failed")
	}, durable.WithRetry(durable.NoRetry()))
	settled, err := durable.AllSettled(ctx, "settled-steps", []*durable.Future[string]{s1, s2})
	if err != nil {
		return Result{}, err
	}

	// Stage 4: Any — first success wins despite some failures.
	y1 := durable.StepAsync(ctx, "any-fail-1", func(_ durable.StepContext) (string, error) {
		return "", errors.New("first failure")
	}, durable.WithRetry(durable.NoRetry()))
	y2 := durable.StepAsync(ctx, "any-fail-2", func(_ durable.StepContext) (string, error) {
		return "", errors.New("second failure")
	}, durable.WithRetry(durable.NoRetry()))
	y3 := durable.StepAsync(ctx, "any-ok", func(_ durable.StepContext) (string, error) {
		return "First success!", nil
	})
	anyResult, err := durable.Any(ctx, "any", []*durable.Future[string]{y1, y2, y3})
	if err != nil {
		var combErr *durable.CombinatorError
		if errors.As(err, &combErr) {
			anyResult = fmt.Sprintf("all failed (%d)", len(combErr.Errors))
		} else {
			return Result{}, err
		}
	}

	return Result{
		AllResults:   allResults,
		RaceResult:   raceResult,
		SettledCount: len(settled),
		AnyResult:    anyResult,
	}, nil
}

func main() { durable.Start(handler) }
