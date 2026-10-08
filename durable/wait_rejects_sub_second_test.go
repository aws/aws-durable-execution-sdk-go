package durable_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestWaitRejectsSubSecondDuration asserts that Wait and WaitAsync reject a
// duration under one second at the call, before any checkpoint.
func TestWaitRejectsSubSecondDuration(t *testing.T) {
	durations := []time.Duration{0, time.Nanosecond, 500 * time.Millisecond, 999 * time.Millisecond}

	check := func(t *testing.T, r *durabletest.TestResult, err error) {
		if err != nil {
			t.Fatal(err)
		}
		var errMsg string
		if r.Error != nil {
			errMsg = r.Error.Message
		}
		if r.Status != durabletest.Failed {
			t.Fatalf("status = %s, want FAILED: a sub-second duration must be rejected at the call", r.Status)
		}
		if !strings.Contains(errMsg, "duration must be at least 1 second") {
			t.Fatalf("error message = %q, want it to contain %q", errMsg, "duration must be at least 1 second")
		}
		for _, op := range r.Operations {
			if op.Type == "WAIT" {
				t.Fatalf("recorded a WAIT operation, want none: the duration must be rejected before any checkpoint")
			}
		}
	}

	for _, d := range durations {
		t.Run("Wait_"+d.String(), func(t *testing.T) {
			h := func(ctx durable.Context, _ any) (string, error) {
				if err := durable.Wait(ctx, "w", d); err != nil {
					return "", err
				}
				return "ok", nil
			}
			r, err := durabletest.NewLocalRunner(h).RunUntilComplete(nil)
			check(t, r, err)
		})

		t.Run("WaitAsync_"+d.String(), func(t *testing.T) {
			h := func(ctx durable.Context, _ any) (string, error) {
				f := durable.WaitAsync(ctx, "w", d)
				if _, err := f.Result(ctx); err != nil {
					return "", err
				}
				return "ok", nil
			}
			r, err := durabletest.NewLocalRunner(h).RunUntilComplete(nil)
			check(t, r, err)
		})
	}
}

// TestWaitSubSecondErrorText asserts the exact error each call returns.
func TestWaitSubSecondErrorText(t *testing.T) {
	h := func(ctx durable.Context, _ any) (string, error) {
		werr := durable.Wait(ctx, "a", 999*time.Millisecond)
		_, aerr := durable.WaitAsync(ctx, "b", -time.Second).Result(ctx)
		var b strings.Builder
		if werr != nil {
			b.WriteString(werr.Error())
		}
		b.WriteString("|")
		if aerr != nil {
			b.WriteString(aerr.Error())
		}
		return b.String(), nil
	}
	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := durabletest.ResultAs[string](r)
	if err != nil {
		t.Fatal(err)
	}
	want := `durable: Wait "a": duration must be at least 1 second|durable: WaitAsync "b": duration must be at least 1 second`
	if got != want {
		t.Fatalf("errors = %q, want %q", got, want)
	}
	if len(r.Operations) != 0 {
		t.Fatalf("recorded %d operations, want 0", len(r.Operations))
	}
}

// TestWaitOneSecondRecordsWait asserts that a wait of exactly one second is
// accepted: Wait and WaitAsync each record a WAIT operation of one second
// and the first invocation suspends.
func TestWaitOneSecondRecordsWait(t *testing.T) {
	cases := map[string]func(durable.Context) error{
		"Wait": func(ctx durable.Context) error { return durable.Wait(ctx, "w", time.Second) },
		"WaitAsync": func(ctx durable.Context) error {
			_, err := durable.WaitAsync(ctx, "w", time.Second).Result(ctx)
			return err
		},
	}
	for name, wait := range cases {
		t.Run(name, func(t *testing.T) {
			h := func(ctx durable.Context, _ any) (string, error) {
				if err := wait(ctx); err != nil {
					return "", err
				}
				return "ok", nil
			}
			runner := durabletest.NewLocalRunner(h)
			r, err := runner.Run(nil)
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != durabletest.Pending {
				t.Fatalf("first run status = %s (error %+v), want PENDING", r.Status, r.Error)
			}
			var waits int
			for _, op := range r.Operations {
				if op.Type == "WAIT" {
					waits++
				}
			}
			if waits != 1 {
				t.Fatalf("WAIT operations = %d, want 1", waits)
			}
			r, err = runner.RunUntilComplete(nil)
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != durabletest.Succeeded {
				t.Fatalf("status = %s (error %+v), want SUCCEEDED", r.Status, r.Error)
			}
		})
	}
}
