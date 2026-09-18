package durable_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// selectRun is what the handler below returns: the winner Select produced
// on the invocation that completed the execution, and how many times each
// branch body ran across all invocations.
type selectRun struct {
	Winner   string `json:"winner"`
	Value    string `json:"value"`
	Err      string `json:"err,omitempty"`
	PrimRuns int32  `json:"primaryRuns"`
	FallRuns int32  `json:"fallbackRuns"`
}

// TestSelectReplayKeepsWinnerWhenTimingReverses runs a Select across two
// invocations. On the first, the fallback branch settles at once while the
// primary branch is held back, so the fallback wins. A wait then suspends
// the execution. On the second invocation the delays are reversed: were
// the branches to run again, the primary would win. Select replays the
// checkpointed winner instead, so the fallback is still reported and no
// branch body runs a second time.
func TestSelectReplayKeepsWinnerWhenTimingReverses(t *testing.T) {
	var invocation, primaryRuns, fallbackRuns atomic.Int32

	handler := func(ctx durable.Context, _ string) (selectRun, error) {
		n := invocation.Add(1)
		// hold gates the branch that must lose on this invocation. It is
		// released once Select has returned, so the held branch still
		// completes within the invocation.
		hold := make(chan struct{})
		winner, value, err := durable.Select(ctx, "quote", []durable.Branch[string]{
			{Name: "primary", Func: func(durable.Context) (string, error) {
				primaryRuns.Add(1)
				if n == 1 {
					<-hold
				}
				return "primary-quote", nil
			}},
			{Name: "fallback", Func: func(durable.Context) (string, error) {
				fallbackRuns.Add(1)
				if n != 1 {
					<-hold
				}
				return "fallback-quote", nil
			}},
		})
		close(hold)
		if err != nil {
			return selectRun{}, err
		}
		if werr := durable.Wait(ctx, "settle", time.Second); werr != nil {
			return selectRun{}, werr
		}
		return selectRun{
			Winner: winner, Value: value,
			PrimRuns: primaryRuns.Load(), FallRuns: fallbackRuns.Load(),
		}, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	first := runner.Run(t, "x")
	if first.Status != durabletest.Pending {
		t.Fatalf("first invocation status = %s, want PENDING; error = %+v", first.Status, first.Error)
	}
	if !runner.CompletePendingTimers() {
		t.Fatal("no pending wait to complete")
	}
	second := runner.Run(t, "x")
	if second.Status != durabletest.Succeeded {
		t.Fatalf("second invocation status = %s, want SUCCEEDED; error = %+v", second.Status, second.Error)
	}
	out, err := durabletest.ResultAs[selectRun](second)
	if err != nil {
		t.Fatal(err)
	}
	if out.Winner != "fallback" || out.Value != "fallback-quote" {
		t.Errorf("replayed Select = %+v, want the first invocation's winner fallback", out)
	}
	if out.PrimRuns != 1 || out.FallRuns != 1 {
		t.Errorf("branch bodies ran primary=%d fallback=%d times, want 1 each: bodies must not run on replay",
			out.PrimRuns, out.FallRuns)
	}
}

// TestSelectReplayKeepsFailedWinner is the failure counterpart: the winning
// branch fails on the first invocation, and the replay returns the same
// winner name and an error that still names that branch.
func TestSelectReplayKeepsFailedWinner(t *testing.T) {
	type observation struct {
		Winner    string `json:"winner"`
		ErrName   string `json:"errName"`
		ErrMsg    string `json:"errMsg"`
		ErrString string `json:"errString"`
	}
	var observations []observation

	handler := func(ctx durable.Context, _ string) (string, error) {
		hold := make(chan struct{})
		winner, _, err := durable.Select(ctx, "quote", []durable.Branch[string]{
			{Name: "broken", Func: func(durable.Context) (string, error) {
				return "", errors.New("upstream unavailable")
			}},
			{Name: "slow", Func: func(durable.Context) (string, error) {
				<-hold
				return "slow-quote", nil
			}},
		})
		close(hold)
		obs := observation{Winner: winner}
		var childErr *durable.ChildContextError
		if errors.As(err, &childErr) {
			obs.ErrName = childErr.Name
			obs.ErrMsg = childErr.Message
		}
		if err != nil {
			obs.ErrString = err.Error()
		}
		observations = append(observations, obs)
		if werr := durable.Wait(ctx, "settle", time.Second); werr != nil {
			return "", werr
		}
		return winner, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	first := runner.Run(t, "x")
	if first.Status != durabletest.Pending {
		t.Fatalf("first invocation status = %s, want PENDING; error = %+v", first.Status, first.Error)
	}
	if !runner.CompletePendingTimers() {
		t.Fatal("no pending wait to complete")
	}
	second := runner.Run(t, "x")
	if second.Status != durabletest.Succeeded {
		t.Fatalf("second invocation status = %s, want SUCCEEDED; error = %+v", second.Status, second.Error)
	}
	if len(observations) != 2 {
		t.Fatalf("recorded %d observations, want 2", len(observations))
	}
	want := observation{
		Winner: "broken", ErrName: "broken", ErrMsg: "upstream unavailable",
		ErrString: `durable: child context "broken" failed: Error: upstream unavailable`,
	}
	for i, obs := range observations {
		if obs != want {
			t.Errorf("invocation %d observed %+v, want %+v", i+1, obs, want)
		}
	}
}
