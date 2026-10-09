// SPDX-License-Identifier: Apache-2.0

package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// TestAsyncAfterTerminationReturnsSuspension asserts that StepAsync,
// WaitAsync, and InvokeAsync called after the checkpointer has terminated
// return a future already settled with the suspension signal. The call
// records nothing, and the StepAsync body does not run.
func TestAsyncAfterTerminationReturnsSuspension(t *testing.T) {
	cases := []struct {
		name  string
		start func(c Context, bodyRan *atomic.Bool) interface {
			settled() bool
			result() (string, error)
		}
	}{
		{
			name: "step",
			start: func(c Context, bodyRan *atomic.Bool) interface {
				settled() bool
				result() (string, error)
			} {
				return StepAsync(c, "late", func(_ StepContext) (string, error) {
					bodyRan.Store(true)
					return "x", nil
				})
			},
		},
		{
			name: "step-at-most-once",
			start: func(c Context, bodyRan *atomic.Bool) interface {
				settled() bool
				result() (string, error)
			} {
				return StepAsync(c, "late", func(_ StepContext) (string, error) {
					bodyRan.Store(true)
					return "x", nil
				}, WithSemantics(AtMostOncePerRetry))
			},
		},
		{
			name: "wait",
			start: func(c Context, _ *atomic.Bool) interface {
				settled() bool
				result() (string, error)
			} {
				return voidAsString{WaitAsync(c, "late", time.Hour)}
			},
		},
		{
			name: "invoke",
			start: func(c Context, _ *atomic.Bool) interface {
				settled() bool
				result() (string, error)
			} {
				return InvokeAsync[string](c, "late", "target-function:$LATEST", "in")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeLambda{}
			gate := make(chan struct{})
			done := make(chan struct{})
			var bodyRan atomic.Bool
			var settledAtCall bool
			var lateErr error

			h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
				_ = Go(ctx, "orphan", func(c Context) (string, error) {
					defer close(done)
					<-gate
					fut := tc.start(c, &bodyRan)
					settledAtCall = fut.settled()
					_, lateErr = fut.result()
					return "orphan", nil
				})
				return "handler-done", nil
			}, withLambdaAPI(fake))

			raw, err := h(context.Background(), stepPayload(`""`))
			if err != nil {
				t.Fatalf("Invoke error: %v", err)
			}
			var resp invocationResponse
			if err := json.Unmarshal(raw, &resp); err != nil {
				t.Fatalf("unmarshal response: %v", err)
			}
			if resp.Status != invocationSucceeded {
				t.Fatalf("status = %q, want %q", resp.Status, invocationSucceeded)
			}

			// The invocation has responded and the checkpointer is
			// terminated. Let the orphan start the operation.
			close(gate)
			select {
			case <-done:
			case <-time.After(orphanExitTiming):
				t.Fatalf("orphan branch did not exit within %v", orphanExitTiming)
			}

			if !settledAtCall {
				t.Error("future was not settled when the call returned")
			}
			if !errors.Is(lateErr, errSuspendExecution) {
				t.Errorf("future error = %v, want errSuspendExecution", lateErr)
			}
			if bodyRan.Load() {
				t.Error("StepAsync body ran after the checkpointer terminated")
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			for _, batch := range fake.gotUpdateBatches {
				for _, u := range batch {
					if aws.ToString(u.Name) == "late" {
						t.Errorf("late operation checkpoint was sent (action=%s)", u.Action)
					}
				}
			}
		})
	}
}

// voidAsString adapts a Future[Void] to the string result the test reads.
type voidAsString struct{ f *Future[Void] }

func (v voidAsString) settled() bool { return v.f.settled() }

func (v voidAsString) result() (string, error) {
	_, err := v.f.result()
	return "", err
}

// TestAwaitGateAttachesWaiters asserts that a goroutine parked through an
// await gate does not commit the invocation to PENDING until the gated
// future is awaited, that the first await attaches a waiter already parked,
// and that a goroutine parked after the await is attached.
func TestAwaitGateAttachesWaiters(t *testing.T) {
	h := newPollHarness()
	gate := &awaitGate{}
	parked := func(n int) bool {
		h.s.mu.Lock()
		defer h.s.mu.Unlock()
		count := 0
		for _, w := range h.s.watches {
			count += len(w.waiters)
		}
		return count == n
	}
	waitParked := func(n int) {
		t.Helper()
		deadline := time.Now().Add(orphanExitTiming)
		for !parked(n) {
			if time.Now().After(deadline) {
				t.Fatalf("%d waiters did not park", n)
			}
			time.Sleep(time.Millisecond)
		}
	}
	await := func(id string) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := h.s.awaitOperation(h.state, id, nil, false, gate, terminalRecord, noEndTime)
			done <- err
		}()
		return done
	}

	first := await("1")
	waitParked(1)
	if h.s.committed() {
		t.Fatal("a waiter parked before the await commits the invocation")
	}
	h.s.markAwaited(gate)
	if !h.s.committed() {
		t.Fatal("the await did not attach the waiter parked before it")
	}
	second := await("2")
	waitParked(2)

	h.s.onStateMerged([]*operation{{id: hashID("1"), status: statusSucceeded}})
	if err := <-first; err != nil {
		t.Fatalf("awaitOperation error = %v", err)
	}
	if !h.s.committed() {
		t.Fatal("a waiter parked after the await is detached")
	}
	h.s.onStateMerged([]*operation{{id: hashID("2"), status: statusSucceeded}})
	if err := <-second; err != nil {
		t.Fatalf("awaitOperation error = %v", err)
	}
	if h.s.committed() {
		t.Fatal("invocation still committed after every waiter woke")
	}
}

// TestCombinatorAttachesLosingFuture asserts that a combinator marks every
// future it reads as awaited. Race reads a pending InvokeAsync future and
// a step future. The step body returns only after the goroutine that runs
// the invoke has parked, so that goroutine parks before anything awaits
// the invoke future. The handler never calls Result on the invoke future,
// so only the read by Race can attach that waiter. Race returns the step's
// value while the invoke is still pending, and the handler returns. An
// attached waiter holds the invocation, so the invocation answers PENDING.
func TestCombinatorAttachesLosingFuture(t *testing.T) {
	fake := &fakeLambda{}
	h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
		slow := InvokeAsync[string](ctx, "slow", "target-function:$LATEST", "in")
		quick := StepAsync(ctx, "quick", func(_ StepContext) (string, error) {
			deadline := time.Now().Add(orphanExitTiming)
			for !invokeParked(slow) {
				if time.Now().After(deadline) {
					return "", errors.New("the invoke goroutine did not park")
				}
				time.Sleep(time.Millisecond)
			}
			return "quick", nil
		}, WithRetry(NoRetry()))
		return Race(ctx, "race", []*Future[string]{slow, quick})
	}, withLambdaAPI(fake))

	raw, err := h(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatalf("Invoke error: %v", err)
	}
	var resp invocationResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Status != invocationPending {
		t.Fatalf("status = %q, want %q (response %s)", resp.Status, invocationPending, raw)
	}
}

// invokeParked reports whether a goroutine is parked on an operation of
// the suspension signal f is registered with.
func invokeParked(f *Future[string]) bool {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	for _, w := range f.s.watches {
		if len(w.waiters) > 0 {
			return true
		}
	}
	return false
}
