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

func TestConfigureSerdesWrongGoroutine(t *testing.T) {
	// ConfigureSerdes is documented to change the defaults only from the
	// owning goroutine, so a call from another goroutine is rejected and
	// leaves the defaults unchanged.
	ec := newTestContext(t, []*operation{execOp()})
	errCh := make(chan error, 1)
	go func() {
		errCh <- ConfigureSerdes(ec, SerdesConfig{Serdes: maskedReceiptSerdes("RCPT:")})
	}()
	err := <-errCh
	if !errors.Is(err, ErrWrongGoroutine) {
		t.Fatalf("ConfigureSerdes from another goroutine error = %v, want ErrWrongGoroutine", err)
	}
	if !strings.Contains(err.Error(), "ConfigureSerdes") {
		t.Errorf("error = %q, want it to name ConfigureSerdes", err)
	}
	if ec.serdesDefaults().serdes != JSONSerdes {
		t.Errorf("serdes = %T after a rejected call, want the default unchanged", ec.serdesDefaults().serdes)
	}
}

// TestConfigureSerdesConcurrentWithForeignOperations runs ConfigureSerdes on
// the owning goroutine while another goroutine keeps calling operations on
// the same context. Each operation reads the serializer defaults before its
// owner check rejects it, so this test only passes under the race detector
// when those reads are synchronized with the write. Every foreign call must
// still fail with ErrWrongGoroutine, and the rejected calls must not
// consume an operation ID.
func TestConfigureSerdesConcurrentWithForeignOperations(t *testing.T) {
	ec := newTestContext(t, []*operation{execOp()})
	serdesA := &ctxRecordingSerdes{}
	serdesB := &ctxRecordingSerdes{}

	const rounds = 200
	start := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		<-start
		for i := 0; i < rounds; i++ {
			for _, err := range foreignOperations(ec) {
				if !errors.Is(err, ErrWrongGoroutine) {
					errCh <- fmt.Errorf("round %d: error = %v, want ErrWrongGoroutine", i, err)
					return
				}
			}
		}
		errCh <- nil
	}()

	close(start)
	for i := 0; i < rounds; i++ {
		cfg := SerdesConfig{Serdes: serdesA, CallbackDeserializer: upperDeserializer{}}
		if i%2 == 1 {
			cfg = SerdesConfig{Serdes: serdesB}
		}
		if err := ConfigureSerdes(ec, cfg); err != nil {
			t.Fatalf("ConfigureSerdes round %d: %v", i, err)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}

	// The owner's final configuration is in effect, and no rejected
	// foreign call consumed an operation ID.
	d := ec.serdesDefaults()
	if d.serdes != serdesB {
		t.Errorf("serdes after the last ConfigureSerdes = %T, want serdesB", d.serdes)
	}
	if _, ok := d.callbackDeserializer.(upperDeserializer); !ok {
		t.Errorf("callbackDeserializer = %T, want upperDeserializer kept from the earlier call", d.callbackDeserializer)
	}
	if id, err := ec.claimOperation(); err != nil || id != "1" {
		t.Errorf("first claimed operation = %q, %v; want \"1\", nil", id, err)
	}
}

// foreignOperations calls, from the current goroutine, every operation that
// reads the handler-level serializer defaults before its owner check, and
// returns their errors in order.
func foreignOperations(ec *execContext) []error {
	var errs []error
	_, err := Step(ec, "step", func(StepContext) (string, error) { return "", nil })
	errs = append(errs, err)
	_, err = StepAsync(ec, "step-async", func(StepContext) (string, error) { return "", nil }).Result()
	errs = append(errs, err)
	_, err = Invoke[string](ec, "invoke", "fn", "in")
	errs = append(errs, err)
	_, err = InvokeAsync[string](ec, "invoke-async", "fn", "in").Result()
	errs = append(errs, err)
	_, err = RunInChildContext(ec, "child", func(Context) (string, error) { return "", nil })
	errs = append(errs, err)
	_, err = RunInChildContextAsync(ec, "child-async", func(Context) (string, error) { return "", nil }).Result()
	errs = append(errs, err)
	_, err = WaitForCondition(ec, "condition",
		func(StepContext, string) (string, error) { return "", nil },
		ConditionConfig[string]{WaitStrategy: func(string, int) WaitDecision { return WaitDecision{} }})
	errs = append(errs, err)
	_, err = Map(ec, "map", []int{1}, func(Context, int, int) (string, error) { return "", nil })
	errs = append(errs, err)
	_, err = CreateCallback[string](ec, "callback")
	errs = append(errs, err)
	return errs
}
