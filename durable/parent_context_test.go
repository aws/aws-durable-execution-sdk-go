//go:build !durablenocheck

package durable_test

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// recordsOp reports whether an operation with the given name was recorded.
func recordsOp(r *durabletest.TestResult, name string) bool {
	for _, op := range r.Operations {
		if op.Name == name {
			return true
		}
	}
	return false
}

// TestParentContextOpRejected asserts the required behavior: an operation
// called on the parent context inside a step body or a child-context body
// is rejected at the call and records nothing.
func TestParentContextOpRejected(t *testing.T) {
	cases := []struct {
		name    string
		handler durable.Handler[struct{}, string]
	}{
		{"step body", func(ctx durable.Context, _ struct{}) (string, error) {
			return durable.Step(ctx, "outer", func(_ durable.StepContext) (string, error) {
				_, ierr := durable.Step(ctx, "inner-on-parent", func(_ durable.StepContext) (string, error) {
					return "x", nil
				})
				if ierr != nil {
					return "", ierr
				}
				return "ok", nil
			})
		}},
		{"child body", func(ctx durable.Context, _ struct{}) (string, error) {
			return durable.RunInChildContext(ctx, "child", func(_ durable.Context) (string, error) {
				_, ierr := durable.Step(ctx, "inner-on-parent", func(_ durable.StepContext) (string, error) {
					return "x", nil
				})
				if ierr != nil {
					return "", ierr
				}
				return "ok", nil
			})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := durabletest.NewLocalRunner(tc.handler).RunUntilComplete(struct{}{})
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != durabletest.Failed {
				t.Fatalf("status = %s, want FAILED: the misuse must be rejected at the call", r.Status)
			}
			if r.Error == nil || !strings.Contains(r.Error.Message, "context") {
				t.Fatalf("error = %+v, want a context-usage error", r.Error)
			}
			if recordsOp(r, "inner-on-parent") {
				t.Fatal("inner-on-parent was recorded; a rejected operation must record nothing")
			}
		})
	}
}

// wireID returns the wire form of a positional operation ID.
func wireID(positionalID string) string {
	sum := md5.Sum([]byte(positionalID))
	return hex.EncodeToString(sum[:])[:16]
}

// assertNothingRecorded fails the test when the operation name appears in
// the recorded operations or in any history event.
func assertNothingRecorded(t *testing.T, r *durabletest.TestResult, name string) {
	t.Helper()
	if recordsOp(r, name) {
		t.Errorf("%s is in the recorded operations; a rejected operation must record nothing", name)
	}
	for _, ev := range r.Events {
		if ev.Name != nil && *ev.Name == name {
			t.Errorf("%s is in the history (event %s); a rejected operation must record nothing", name, ev.EventType)
		}
	}
}

// wrongContext reports, as the handler's result, whether err wraps
// ErrWrongContext, and the error text.
func wrongContext(err error) string {
	return fmt.Sprintf("is=%v %v", errors.Is(err, durable.ErrWrongContext), err)
}

// TestParentContextErrorIs checks that the error a misused operation
// returns matches ErrWrongContext through errors.Is, for a synchronous
// operation and through the Future of an asynchronous one, and that it
// names the operation and the two contexts.
func TestParentContextErrorIs(t *testing.T) {
	noRetry := durable.WithRetry(durable.NoRetry())
	cases := []struct {
		name    string
		handler durable.Handler[struct{}, string]
		want    string
	}{
		{"sync in step body", func(ctx durable.Context, _ struct{}) (string, error) {
			return durable.Step(ctx, "outer", func(_ durable.StepContext) (string, error) {
				return wrongContext(durable.Wait(ctx, "inner-on-parent", time.Second)), nil
			}, noRetry)
		}, `is=true durable: operation called on a context that is not the innermost active context; inside a RunInChildContext, Go, Map, or Parallel body use the context that body received, and claim no operation on an enclosing context from inside a step body (operation "inner-on-parent" on context "root", active context "1")`},
		{"async in step body", func(ctx durable.Context, _ struct{}) (string, error) {
			return durable.Step(ctx, "outer", func(_ durable.StepContext) (string, error) {
				fut := durable.StepAsync(ctx, "inner-on-parent", func(_ durable.StepContext) (string, error) {
					return "x", nil
				})
				_, err := fut.Result(ctx)
				return wrongContext(err), nil
			}, noRetry)
		}, `(operation "inner-on-parent" on context "root", active context "1")`},
		{"async in child body", func(ctx durable.Context, _ struct{}) (string, error) {
			return durable.RunInChildContext(ctx, "child", func(child durable.Context) (string, error) {
				fut := durable.InvokeAsync[string](ctx, "inner-on-parent", "fn:$LATEST", "in")
				_, err := fut.Result(child)
				return wrongContext(err), nil
			})
		}, `(operation "inner-on-parent" on context "root", active context "1")`},
		{"root from a step body in a child", func(ctx durable.Context, _ struct{}) (string, error) {
			return durable.RunInChildContext(ctx, "child", func(child durable.Context) (string, error) {
				return durable.Step(child, "outer", func(_ durable.StepContext) (string, error) {
					_, err := durable.Step(ctx, "inner-on-parent", func(_ durable.StepContext) (string, error) {
						return "x", nil
					})
					return wrongContext(err), nil
				}, noRetry)
			})
		}, `(operation "inner-on-parent" on context "root", active context "1-1")`},
		{"child from its own step body", func(ctx durable.Context, _ struct{}) (string, error) {
			return durable.RunInChildContext(ctx, "child", func(child durable.Context) (string, error) {
				return durable.Step(child, "outer", func(_ durable.StepContext) (string, error) {
					_, err := durable.RunInChildContext(child, "inner-on-parent", func(durable.Context) (string, error) {
						return "x", nil
					})
					return wrongContext(err), nil
				}, noRetry)
			})
		}, `(operation "inner-on-parent" on context "1", active context "1-1")`},
		{"condition check", func(ctx durable.Context, _ struct{}) (string, error) {
			var got string
			_, err := durable.WaitForCondition(ctx, "cond", func(_ durable.StepContext, s int) (int, error) {
				_, serr := durable.Step(ctx, "inner-on-parent", func(_ durable.StepContext) (string, error) {
					return "x", nil
				})
				got = wrongContext(serr)
				return s, nil
			}, durable.ConditionConfig[int]{WaitStrategy: func(int, int) durable.WaitDecision {
				return durable.WaitDecision{Continue: false}
			}})
			return got, err
		}, `(operation "inner-on-parent" on context "root", active context "1")`},
		{"callback submitter", func(ctx durable.Context, _ struct{}) (string, error) {
			var got string
			_, err := durable.WaitForCallback[string](ctx, "approval", func(_ durable.StepContext, _ string) error {
				_, serr := durable.Step(ctx, "inner-on-parent", func(_ durable.StepContext) (string, error) {
					return "x", nil
				})
				got = wrongContext(serr)
				return errors.New("stop")
			}, durable.WithSubmitterRetry(durable.NoRetry()))
			if err == nil {
				return "", errors.New("WaitForCallback succeeded, want the submitter failure")
			}
			return got, nil
		}, `(operation "inner-on-parent" on context "root", active context "1-2")`},
		{"captured sibling child", func(ctx durable.Context, _ struct{}) (string, error) {
			var first durable.Context
			if _, err := durable.RunInChildContext(ctx, "first", func(c durable.Context) (string, error) {
				first = c
				return "", nil
			}); err != nil {
				return "", err
			}
			return durable.RunInChildContext(ctx, "second", func(durable.Context) (string, error) {
				return wrongContext(durable.Wait(first, "inner-on-parent", time.Second)), nil
			})
		}, `(operation "inner-on-parent" on context "1", active context "2")`},
		{"stale child from the root body", func(ctx durable.Context, _ struct{}) (string, error) {
			var stale durable.Context
			if _, err := durable.RunInChildContext(ctx, "child", func(c durable.Context) (string, error) {
				stale = c
				return "", nil
			}); err != nil {
				return "", err
			}
			return wrongContext(durable.Wait(stale, "inner-on-parent", time.Second)), nil
		}, `(operation "inner-on-parent" on context "1", active context "root")`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := durabletest.NewLocalRunner(tc.handler).RunUntilComplete(struct{}{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := durabletest.ResultAs[string](r)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(got, "is=true ") || !strings.HasSuffix(got, tc.want) {
				t.Fatalf("result = %q, want errors.Is true and a message ending %q", got, tc.want)
			}
			assertNothingRecorded(t, r, "inner-on-parent")
		})
	}
}

// TestParentContextClaimsNoID checks that a rejected operation consumes no
// operation ID. The handler ignores the rejection, so the next operation on
// the active context takes the ID the rejected one would have taken. The
// execution suspends on a wait and replays, which fails with a
// NonDeterministicReplayError if the IDs do not line up.
func TestParentContextClaimsNoID(t *testing.T) {
	cases := []struct {
		name    string
		handler durable.Handler[struct{}, string]
		want    []string // recorded operation names, in order
		ids     map[string]string
	}{
		{"step body", func(ctx durable.Context, _ struct{}) (string, error) {
			if _, err := durable.Step(ctx, "outer", func(_ durable.StepContext) (string, error) {
				_, err := durable.Step(ctx, "inner-on-parent", func(_ durable.StepContext) (string, error) {
					return "x", nil
				})
				if !errors.Is(err, durable.ErrWrongContext) {
					return "", fmt.Errorf("inner error = %v, want ErrWrongContext", err)
				}
				return "ok", nil
			}); err != nil {
				return "", err
			}
			if err := durable.Wait(ctx, "gap", time.Second); err != nil {
				return "", err
			}
			return durable.Step(ctx, "after", func(_ durable.StepContext) (string, error) {
				return "done", nil
			})
		}, []string{"outer", "gap", "after"}, map[string]string{"outer": "1", "gap": "2", "after": "3"}},
		{"child body", func(ctx durable.Context, _ struct{}) (string, error) {
			if _, err := durable.RunInChildContext(ctx, "child", func(child durable.Context) (string, error) {
				_, err := durable.Step(ctx, "inner-on-parent", func(_ durable.StepContext) (string, error) {
					return "x", nil
				})
				if !errors.Is(err, durable.ErrWrongContext) {
					return "", fmt.Errorf("inner error = %v, want ErrWrongContext", err)
				}
				return durable.Step(child, "in-child", func(_ durable.StepContext) (string, error) {
					return "ok", nil
				})
			}); err != nil {
				return "", err
			}
			if err := durable.Wait(ctx, "gap", time.Second); err != nil {
				return "", err
			}
			return durable.Step(ctx, "after", func(_ durable.StepContext) (string, error) {
				return "done", nil
			})
		}, []string{"child", "in-child", "gap", "after"}, map[string]string{"child": "1", "in-child": "1-1", "gap": "2", "after": "3"}},
		// A virtual child claims its ID without a checkpoint. The claim on
		// the inactive parent must still be rejected before the ID is
		// taken, and the virtual body must not run.
		{"virtual child in step body", func(ctx durable.Context, _ struct{}) (string, error) {
			if _, err := durable.Step(ctx, "outer", func(_ durable.StepContext) (string, error) {
				_, err := durable.RunInChildContext(ctx, "inner-on-parent", func(child durable.Context) (string, error) {
					return durable.Step(child, "in-virtual", func(_ durable.StepContext) (string, error) {
						return "x", nil
					})
				}, durable.WithChildVirtual())
				if !errors.Is(err, durable.ErrWrongContext) {
					return "", fmt.Errorf("inner error = %v, want ErrWrongContext", err)
				}
				return "ok", nil
			}); err != nil {
				return "", err
			}
			if err := durable.Wait(ctx, "gap", time.Second); err != nil {
				return "", err
			}
			return durable.Step(ctx, "after", func(_ durable.StepContext) (string, error) {
				return "done", nil
			})
		}, []string{"outer", "gap", "after"}, map[string]string{"outer": "1", "gap": "2", "after": "3"}},
		{"virtual child in child body", func(ctx durable.Context, _ struct{}) (string, error) {
			if _, err := durable.RunInChildContext(ctx, "child", func(child durable.Context) (string, error) {
				_, err := durable.RunInChildContext(ctx, "inner-on-parent", func(v durable.Context) (string, error) {
					return durable.Step(v, "in-virtual", func(_ durable.StepContext) (string, error) {
						return "x", nil
					})
				}, durable.WithChildVirtual())
				if !errors.Is(err, durable.ErrWrongContext) {
					return "", fmt.Errorf("inner error = %v, want ErrWrongContext", err)
				}
				return durable.Step(child, "in-child", func(_ durable.StepContext) (string, error) {
					return "ok", nil
				})
			}); err != nil {
				return "", err
			}
			if err := durable.Wait(ctx, "gap", time.Second); err != nil {
				return "", err
			}
			return durable.Step(ctx, "after", func(_ durable.StepContext) (string, error) {
				return "done", nil
			})
		}, []string{"child", "in-child", "gap", "after"}, map[string]string{"child": "1", "in-child": "1-1", "gap": "2", "after": "3"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := durabletest.NewLocalRunner(tc.handler).RunUntilComplete(struct{}{})
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != durabletest.Succeeded {
				t.Fatalf("status = %s, error = %+v; want SUCCEEDED", r.Status, r.Error)
			}
			var names []string
			for _, op := range r.Operations {
				names = append(names, op.Name)
				if want, ok := tc.ids[op.Name]; ok && op.ID != wireID(want) {
					t.Errorf("operation %s has ID %s, want the ID of %q", op.Name, op.ID, want)
				}
			}
			if strings.Join(names, ",") != strings.Join(tc.want, ",") {
				t.Errorf("operations = %v, want %v", names, tc.want)
			}
			assertNothingRecorded(t, r, "inner-on-parent")
			assertNothingRecorded(t, r, "in-virtual")
		})
	}
}

// TestChildContextUseStillWorks checks that an operation on the context a
// RunInChildContext, Go, Map, or Parallel body received is accepted and
// recorded under that child.
func TestChildContextUseStillWorks(t *testing.T) {
	inner := func(c durable.Context) (string, error) {
		return durable.Step(c, "inner", func(_ durable.StepContext) (string, error) {
			return "ok", nil
		})
	}
	cases := []struct {
		name    string
		handler durable.Handler[struct{}, string]
		parent  string // name of the operation inner must be recorded under
	}{
		{"RunInChildContext", func(ctx durable.Context, _ struct{}) (string, error) {
			return durable.RunInChildContext(ctx, "child", inner)
		}, "child"},
		{"Go", func(ctx durable.Context, _ struct{}) (string, error) {
			return durable.Go(ctx, "child", inner).Result(ctx)
		}, "child"},
		{"Map", func(ctx durable.Context, _ struct{}) (string, error) {
			res, err := durable.Map(ctx, "map", []int{1}, func(c durable.Context, _ int, _ int) (string, error) {
				return inner(c)
			}, durable.WithItemNamer(func(int) string { return "item" }))
			if err != nil {
				return "", err
			}
			return res.Results()[0], nil
		}, "item"},
		{"Parallel", func(ctx durable.Context, _ struct{}) (string, error) {
			res, err := durable.Parallel(ctx, "par", []durable.Branch[string]{{Name: "item", Func: inner}})
			if err != nil {
				return "", err
			}
			return res.Results()[0], nil
		}, "item"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := durabletest.NewLocalRunner(tc.handler).RunUntilComplete(struct{}{})
			if err != nil {
				t.Fatal(err)
			}
			if got, err := durabletest.ResultAs[string](r); err != nil || got != "ok" {
				t.Fatalf("result = %q, %v; want ok", got, err)
			}
			ids := map[string]string{}
			var innerParent string
			for _, op := range r.Operations {
				ids[op.Name] = op.ID
				if op.Name == "inner" {
					innerParent = op.ParentID
				}
			}
			if innerParent == "" || innerParent != ids[tc.parent] {
				t.Errorf("inner recorded under %q, want under %s (%q)", innerParent, tc.parent, ids[tc.parent])
			}
		})
	}
}
