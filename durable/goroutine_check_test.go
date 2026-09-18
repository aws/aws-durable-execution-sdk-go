//go:build !durablenocheck

package durable

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestGoidParsesCurrentGoroutine(t *testing.T) {
	id, ok := goid()
	if !ok {
		t.Fatal("goid() ok = false, want true")
	}
	if id == 0 {
		t.Error("goid() = 0, want a positive goroutine ID")
	}
}

func TestGoidDiffersAcrossGoroutines(t *testing.T) {
	mainID, ok := goid()
	if !ok {
		t.Fatal("goid() ok = false on test goroutine")
	}

	type result struct {
		id uint64
		ok bool
	}
	ch := make(chan result, 1)
	go func() {
		id, ok := goid()
		ch <- result{id: id, ok: ok}
	}()
	got := <-ch
	if !got.ok {
		t.Fatal("goid() ok = false on spawned goroutine")
	}
	if got.id == mainID {
		t.Errorf("spawned goroutine ID %d equals owner ID, want distinct IDs", got.id)
	}
}

func TestGoroutineOwnerCheck(t *testing.T) {
	owner := currentGoroutineOwner()

	if err := owner.check(); err != nil {
		t.Errorf("check() on owning goroutine = %v, want nil", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- owner.check()
	}()
	err := <-errCh
	if !errors.Is(err, ErrWrongGoroutine) {
		t.Errorf("check() from foreign goroutine = %v, want ErrWrongGoroutine", err)
	}
}

func TestGoroutineOwnerDisabledFailsOpen(t *testing.T) {
	owner := disabledGoroutineOwner()
	errCh := make(chan error, 1)
	go func() {
		errCh <- owner.check()
	}()
	if err := <-errCh; err != nil {
		t.Errorf("check() with detection disabled = %v, want nil", err)
	}
}

// TestStepRejectsForeignGoroutine drives the check through the public
// Step entry point using a plain `go` statement, the misuse the check
// exists to catch.
func TestStepRejectsForeignGoroutine(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		errCh := make(chan error, 1)
		go func() {
			_, err := Step(ctx, "foreign", func(StepContext) (string, error) {
				return "must not run", nil
			})
			errCh <- err
		}()
		if err := <-errCh; !errors.Is(err, ErrWrongGoroutine) {
			return "", err
		}
		// The rejected call must not have consumed an operation ID or
		// recorded a checkpoint, so the handler's own step is still "1".
		return Step(ctx, "owned", func(StepContext) (string, error) {
			return "ok", nil
		})
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"ok\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestGoParentContextFromChildGoroutineFails(t *testing.T) {
	// Using the PARENT context from a child goroutine must fail fast with
	// ErrWrongGoroutine, not silently corrupt replay order. The failure
	// escapes the child, so the parent sees a ChildContextError whose
	// recorded message is the ErrWrongGoroutine text. The original error
	// value is not preserved across the child boundary; only its type and
	// message are, the same on first execution and on replay.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fut := Go(ctx, "misuse", func(_ Context) (string, error) {
			// Attempt to use the parent ctx from the child goroutine.
			_, err := Step[string](ctx, "bad", func(StepContext) (string, error) {
				return "should not run", nil
			})
			if !errors.Is(err, ErrWrongGoroutine) {
				return "", fmt.Errorf("Step on parent ctx from child goroutine = %v, want ErrWrongGoroutine", err)
			}
			return "", err
		})
		_, err := fut.Result()
		var childErr *ChildContextError
		if !errors.As(err, &childErr) {
			return "", fmt.Errorf("expected ChildContextError, got: %v", err)
		}
		if !strings.Contains(childErr.Message, ErrWrongGoroutine.Error()) {
			return "", fmt.Errorf("child error message = %q, want it to contain %q", childErr.Message, ErrWrongGoroutine.Error())
		}
		return "ownership-checked", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"ownership-checked\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// BenchmarkGoid measures the goroutine-identity read on its own.
func BenchmarkGoid(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := goid(); !ok {
			b.Fatal("goid() ok = false")
		}
	}
}
