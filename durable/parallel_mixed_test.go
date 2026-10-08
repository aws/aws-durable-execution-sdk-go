package durable_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// mixedRun runs h to completion and fails the test unless it succeeded.
func mixedRun[E, O any](t *testing.T, h func(durable.Context, E) (O, error), event E) *durabletest.TestResult {
	t.Helper()
	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(event)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error %+v)", r.Status, r.Error)
	}
	return r
}

// mixedReads is what one invocation read from the three typed branches.
type mixedReads struct {
	Types []string
	Order Order
	Count int
	Label string
}

func readMixed(res durable.BatchResult[json.RawMessage], order *durable.TypedBranch[Order], count *durable.TypedBranch[int], label *durable.TypedBranch[string]) (mixedReads, error) {
	o, err := order.Result(res)
	if err != nil {
		return mixedReads{}, err
	}
	c, err := count.Result(res)
	if err != nil {
		return mixedReads{}, err
	}
	l, err := label.Result(res)
	if err != nil {
		return mixedReads{}, err
	}
	return mixedReads{
		Types: []string{fmt.Sprintf("%T", o), fmt.Sprintf("%T", c), fmt.Sprintf("%T", l)},
		Order: o, Count: c, Label: l,
	}, nil
}

// TestParallelMixedReturnsBranchTypes asserts each branch's Result returns
// the branch's own static type and value on the first run and on replay.
func TestParallelMixedReturnsBranchTypes(t *testing.T) {
	var mu sync.Mutex
	var reads []mixedReads
	var replayed []bool
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		order := durable.NewTypedBranch("order", func(_ durable.Context) (Order, error) { return Order{ID: "A1", Total: 42}, nil })
		count := durable.NewTypedBranch("count", func(_ durable.Context) (int, error) { return 7, nil })
		label := durable.NewTypedBranch("label", func(_ durable.Context) (string, error) { return "hello", nil })
		res, err := durable.ParallelMixed(ctx, "fan", []durable.AnyBranch{order, count, label})
		if err != nil {
			return "", err
		}
		got, err := readMixed(res, order, count, label)
		if err != nil {
			return "", err
		}
		mu.Lock()
		reads = append(reads, got)
		replayed = append(replayed, ctx.IsReplaying())
		mu.Unlock()
		// The wait ends the first invocation, so the second invocation
		// replays the recorded batch.
		if err := durable.Wait(ctx, "after", time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}
	mixedRun(t, h, struct{}{})

	if len(reads) != 2 {
		t.Fatalf("handler read the branches %d times, want 2 (first run and replay)", len(reads))
	}
	if replayed[0] || !replayed[1] {
		t.Fatalf("IsReplaying per read = %v, want [false true]", replayed)
	}
	want := mixedReads{
		Types: []string{"durable_test.Order", "int", "string"},
		Order: Order{ID: "A1", Total: 42}, Count: 7, Label: "hello",
	}
	for i, got := range reads {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("read %d = %+v, want %+v", i, got, want)
		}
	}
}

// mixedOp is a TestOperation without its wall-clock times.
type mixedOp struct {
	ID, Name, Status, Type, SubType, ParentID string
	Context                                   *durabletest.TestContextDetails
	Step                                      *durabletest.TestStepDetails
}

// mixedOps returns the operations of r sorted by ID. A stored batch
// payload has its stack traces removed: a trace names the frames of the
// program that ran the batch, so two different programs never record
// equal traces.
func mixedOps(r *durabletest.TestResult) []mixedOp {
	out := make([]mixedOp, len(r.Operations))
	for i, op := range r.Operations {
		out[i] = mixedOp{
			ID: op.ID, Name: op.Name, Status: op.Status, Type: op.Type, SubType: op.SubType, ParentID: op.ParentID,
			Context: op.ContextDetails, Step: op.StepDetails,
		}
		if op.ContextDetails != nil {
			cd := *op.ContextDetails
			cd.Result = withoutStackTraces(cd.Result)
			out[i].Context = &cd
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// stackTraceField matches one recorded "StackTrace" member, an array of
// JSON strings, together with the comma that separates it from the member
// before it or after it.
var stackTraceField = regexp.MustCompile(`,"StackTrace":\[(?:"(?:[^"\\]|\\.)*",?)*\]|"StackTrace":\[(?:"(?:[^"\\]|\\.)*",?)*\],?`)

// withoutStackTraces removes every "StackTrace" member from payload and
// leaves every other byte unchanged, so a comparison of the results still
// detects any difference in encoding, such as HTML escaping.
func withoutStackTraces(payload string) string {
	return stackTraceField.ReplaceAllString(payload, "")
}

// findOp returns the operation of r named name.
func findOp(t *testing.T, r *durabletest.TestResult, name string) durabletest.TestOperation {
	t.Helper()
	for _, op := range r.Operations {
		if op.Name == name {
			return op
		}
	}
	t.Fatalf("no operation named %q", name)
	return durabletest.TestOperation{}
}

func assertSameOps(t *testing.T, label string, got, want *durabletest.TestResult) {
	t.Helper()
	g, w := mixedOps(got), mixedOps(want)
	if !reflect.DeepEqual(g, w) {
		t.Errorf("%s: operations differ\n got: %s\nwant: %s", label, dumpOps(g), dumpOps(w))
	}
}

func dumpOps(ops []mixedOp) string {
	var b strings.Builder
	for _, op := range ops {
		fmt.Fprintf(&b, "\n  %+v", op)
		if op.Context != nil {
			fmt.Fprintf(&b, " ctx=%+v", *op.Context)
		}
		if op.Step != nil {
			fmt.Fprintf(&b, " step=%+v", *op.Step)
		}
	}
	return b.String()
}

// marshalUnescaped encodes v as JSON with <, > and & unescaped, the bytes
// the default serdes stores for v.
func marshalUnescaped(v any) (json.RawMessage, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// TestParallelMixedHistoryEqualsParallel asserts a ParallelMixed call
// records the operations and payloads, field by field including IDs, of a
// Parallel[json.RawMessage] over the same values, and, for branches of one
// type, those of a Parallel[T] over that type.
func TestParallelMixedHistoryEqualsParallel(t *testing.T) {
	order, count, label := Order{ID: "A<1>&", Total: 42}, 7, "hello"
	values := []any{order, count, label}
	names := []string{"order", "count", ""}
	step := func(c durable.Context, v any) (json.RawMessage, error) {
		return durable.Step(c, "s", func(_ durable.StepContext) (json.RawMessage, error) { return marshalUnescaped(v) })
	}

	mixed := func(ctx durable.Context, _ struct{}) (int, error) {
		res, err := durable.ParallelMixed(ctx, "fan", []durable.AnyBranch{
			durable.NewTypedBranch(names[0], func(c durable.Context) (Order, error) {
				if _, err := step(c, values[0]); err != nil {
					return Order{}, err
				}
				return order, nil
			}),
			durable.NewTypedBranch(names[1], func(c durable.Context) (int, error) {
				if _, err := step(c, values[1]); err != nil {
					return 0, err
				}
				return count, nil
			}),
			durable.NewTypedBranch(names[2], func(c durable.Context) (string, error) {
				if _, err := step(c, values[2]); err != nil {
					return "", err
				}
				return label, nil
			}),
		})
		return res.SuccessCount(), err
	}
	raw := func(ctx durable.Context, _ struct{}) (int, error) {
		branches := make([]durable.Branch[json.RawMessage], len(values))
		for i := range values {
			branches[i] = durable.Branch[json.RawMessage]{Name: names[i], Func: func(c durable.Context) (json.RawMessage, error) {
				if _, err := step(c, values[i]); err != nil {
					return nil, err
				}
				return marshalUnescaped(values[i])
			}}
		}
		res, err := durable.Parallel(ctx, "fan", branches)
		return res.SuccessCount(), err
	}
	mixedResult := mixedRun(t, mixed, struct{}{})
	assertSameOps(t, "mixed vs Parallel[json.RawMessage]", mixedResult, mixedRun(t, raw, struct{}{}))

	// The stored batch and item payloads hold the branch value's bytes as
	// the serdes produced them, with <, > and & unescaped.
	for _, opName := range []string{"fan", "order"} {
		op := findOp(t, mixedResult, opName)
		if op.ContextDetails == nil || !strings.Contains(op.ContextDetails.Result, `"id":"A<1>&"`) {
			t.Errorf("stored payload of %q = %+v, want the raw bytes of %q", opName, op.ContextDetails, `"id":"A<1>&"`)
		}
	}

	// One type per call: ParallelMixed over int branches equals Parallel[int].
	mixedInts := func(ctx durable.Context, _ struct{}) (int, error) {
		res, err := durable.ParallelMixed(ctx, "ints", []durable.AnyBranch{
			durable.NewTypedBranch("a", func(_ durable.Context) (int, error) { return 1, nil }),
			durable.NewTypedBranch("b", func(_ durable.Context) (int, error) { return 2, nil }),
		})
		return res.SuccessCount(), err
	}
	ints := func(ctx durable.Context, _ struct{}) (int, error) {
		res, err := durable.Parallel(ctx, "ints", []durable.Branch[int]{
			{Name: "a", Func: func(_ durable.Context) (int, error) { return 1, nil }},
			{Name: "b", Func: func(_ durable.Context) (int, error) { return 2, nil }},
		})
		return res.SuccessCount(), err
	}
	assertSameOps(t, "mixed vs Parallel[int]", mixedRun(t, mixedInts, struct{}{}), mixedRun(t, ints, struct{}{}))
}

// branchKind is how a branch of the completion-policy tests behaves.
type branchKind int

const (
	kindSucceed branchKind = iota
	kindFail
	kindWaitLong // waits an hour, so an early completion abandons it
	kindInstant  // returns at once, with no operation
)

// batchShape is the comparable outcome of a batch.
type batchShape struct {
	Reason   string
	Statuses map[int]string
	ErrType  string
}

func shapeOf(res durable.BatchResult[json.RawMessage], err error) batchShape {
	s := batchShape{Reason: res.Reason.String(), Statuses: map[int]string{}}
	for _, it := range res.Items {
		s.Statuses[it.Index] = it.Status.String()
	}
	if err != nil {
		s.ErrType = fmt.Sprintf("%T", err)
	}
	return s
}

func kindBody(c durable.Context, k branchKind, i int) (int, error) {
	switch k {
	case kindFail:
		return durable.Step(c, "s", func(_ durable.StepContext) (int, error) {
			return 0, errors.New("boom")
		}, durable.WithRetry(durable.NoRetry()))
	case kindWaitLong:
		if err := durable.Wait(c, "long", time.Hour); err != nil {
			return 0, err
		}
		return i, nil
	case kindInstant:
		return i, nil
	default:
		return durable.Step(c, "s", func(_ durable.StepContext) (int, error) { return i, nil })
	}
}

// TestParallelMixedOptionsBehaveAsParallel asserts every completion policy
// and WithMaxConcurrency produce the outcome and history they produce in
// Parallel.
func TestParallelMixedOptionsBehaveAsParallel(t *testing.T) {
	cases := []struct {
		name  string
		kinds []branchKind
		opts  []durable.BatchOption
		// parentOnly compares only the batch's own operation. The
		// operations an abandoned branch records after the batch completes
		// depend on goroutine scheduling, in Parallel as in ParallelMixed.
		parentOnly bool
	}{
		{"default fail-fast", []branchKind{kindSucceed, kindFail, kindSucceed}, []durable.BatchOption{durable.WithMaxConcurrency(1)}, false},
		{"MinSuccessful", []branchKind{kindSucceed, kindSucceed, kindSucceed}, []durable.BatchOption{durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 1})}, false},
		{"MinSuccessful abandons", []branchKind{kindInstant, kindWaitLong}, []durable.BatchOption{durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 1})}, true},
		{"ToleratedFailureCount", []branchKind{kindFail, kindSucceed, kindFail}, []durable.BatchOption{durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: intPtr(1)})}, false},
		{"ToleratedFailurePercentage", []branchKind{kindFail, kindSucceed, kindSucceed, kindSucceed}, []durable.BatchOption{durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{ToleratedFailurePercentage: intPtr(25)})}, false},
		{"NestingFlat", []branchKind{kindSucceed, kindFail, kindSucceed}, []durable.BatchOption{durable.WithNesting(durable.NestingFlat), durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: intPtr(1)})}, false},
		{"ShouldComplete", []branchKind{kindSucceed, kindSucceed, kindSucceed}, []durable.BatchOption{durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{
			ShouldComplete: func(p durable.BatchProgress) durable.CompletionDecision {
				if p.SuccessCount >= 2 {
					return durable.CompleteBatch(durable.CompletionOutcomeSucceeded)
				}
				return durable.ContinueBatch()
			},
		})}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mixedShape, parShape batchShape
			mixed := func(ctx durable.Context, _ struct{}) (int, error) {
				branches := make([]durable.AnyBranch, len(tc.kinds))
				for i, k := range tc.kinds {
					branches[i] = durable.NewTypedBranch(fmt.Sprintf("b%d", i), func(c durable.Context) (int, error) { return kindBody(c, k, i) })
				}
				res, err := durable.ParallelMixed(ctx, "p", branches, tc.opts...)
				if err != nil && !isBatchErr(err) {
					return 0, err
				}
				mixedShape = shapeOf(res, err)
				return 0, nil
			}
			par := func(ctx durable.Context, _ struct{}) (int, error) {
				branches := make([]durable.Branch[json.RawMessage], len(tc.kinds))
				for i, k := range tc.kinds {
					branches[i] = durable.Branch[json.RawMessage]{Name: fmt.Sprintf("b%d", i), Func: func(c durable.Context) (json.RawMessage, error) {
						v, err := kindBody(c, k, i)
						if err != nil {
							return nil, err
						}
						return json.Marshal(v)
					}}
				}
				res, err := durable.Parallel(ctx, "p", branches, tc.opts...)
				if err != nil && !isBatchErr(err) {
					return 0, err
				}
				parShape = shapeOf(res, err)
				return 0, nil
			}
			got, want := mixedRun(t, mixed, struct{}{}), mixedRun(t, par, struct{}{})
			if tc.parentOnly {
				got.Operations, want.Operations = topLevelOps(got), topLevelOps(want)
			}
			assertSameOps(t, tc.name, got, want)
			if !reflect.DeepEqual(mixedShape, parShape) {
				t.Errorf("ParallelMixed outcome = %+v, Parallel outcome = %+v", mixedShape, parShape)
			}
		})
	}
}

// topLevelOps returns the operations of r that have no parent.
func topLevelOps(r *durabletest.TestResult) []durabletest.TestOperation {
	var out []durabletest.TestOperation
	for _, op := range r.Operations {
		if op.ParentID == "" {
			out = append(out, op)
		}
	}
	return out
}

func isBatchErr(err error) bool {
	var be *durable.BatchError
	return errors.As(err, &be)
}

// TestParallelMixedMaxConcurrency asserts WithMaxConcurrency bounds the
// branches in flight.
func TestParallelMixedMaxConcurrency(t *testing.T) {
	var inFlight, peak atomic.Int32
	h := func(ctx durable.Context, _ struct{}) (int, error) {
		branches := make([]durable.AnyBranch, 6)
		for i := range branches {
			branches[i] = durable.NewTypedBranch("", func(c durable.Context) (int, error) {
				return durable.Step(c, "s", func(_ durable.StepContext) (int, error) {
					n := inFlight.Add(1)
					defer inFlight.Add(-1)
					for {
						p := peak.Load()
						if n <= p || peak.CompareAndSwap(p, n) {
							break
						}
					}
					time.Sleep(20 * time.Millisecond)
					return i, nil
				})
			})
		}
		res, err := durable.ParallelMixed(ctx, "bounded", branches, durable.WithMaxConcurrency(2))
		return res.SuccessCount(), err
	}
	r := mixedRun(t, h, struct{}{})
	if got, _ := durabletest.ResultAs[int](r); got != 6 {
		t.Errorf("succeeded = %d, want 6", got)
	}
	if p := peak.Load(); p > 2 || p < 1 {
		t.Errorf("peak branches in flight = %d, want 1 or 2", p)
	}
}

// TestParallelMixedToleratedFailure asserts a tolerated branch failure is
// returned by that branch's Result as its item error, carrying the branch
// error's ErrorType, while the other branches read their values.
func TestParallelMixedToleratedFailure(t *testing.T) {
	type out struct {
		Value     string
		ErrType   string
		ErrString string
		IsChild   bool
	}
	h := func(ctx durable.Context, _ struct{}) (out, error) {
		good := durable.NewTypedBranch("good", func(_ durable.Context) (string, error) { return "ok", nil })
		bad := durable.NewTypedBranch("bad", func(c durable.Context) (int, error) {
			return durable.Step(c, "s", func(_ durable.StepContext) (int, error) {
				return 0, CardDeclined{}
			}, durable.WithRetry(durable.NoRetry()))
		})
		res, err := durable.ParallelMixed(ctx, "p", []durable.AnyBranch{good, bad},
			durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: intPtr(1)}))
		if err != nil {
			return out{}, err
		}
		v, err := good.Result(res)
		if err != nil {
			return out{}, err
		}
		_, berr := bad.Result(res)
		var cce *durable.ChildContextError
		o := out{Value: v, IsChild: errors.As(berr, &cce)}
		if cce != nil {
			o.ErrType = cce.ErrorType
		}
		if berr != nil {
			o.ErrString = berr.Error()
		}
		return o, nil
	}
	r := mixedRun(t, h, struct{}{})
	got, err := durabletest.ResultAs[out](r)
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != "ok" {
		t.Errorf("good branch = %q, want ok", got.Value)
	}
	if !got.IsChild || got.ErrType != "StepError" {
		t.Errorf("bad branch error = %q (ChildContextError %v, ErrorType %q), want a *ChildContextError with ErrorType StepError",
			got.ErrString, got.IsChild, got.ErrType)
	}
}

// CardDeclined is a branch error type of the tolerated-failure test.
type CardDeclined struct{}

func (CardDeclined) Error() string { return "card declined" }

// TestParallelMixedBranchNotCompleted asserts a branch that never started
// and a branch abandoned by early completion return a
// *BranchNotCompletedError from Result.
func TestParallelMixedBranchNotCompleted(t *testing.T) {
	type out struct{ NotStarted, Abandoned string }
	h := func(ctx durable.Context, _ struct{}) (out, error) {
		var o out
		first := durable.NewTypedBranch("first", func(_ durable.Context) (int, error) { return 1, nil })
		never := durable.NewTypedBranch("never", func(_ durable.Context) (string, error) { return "x", nil })
		res, err := durable.ParallelMixed(ctx, "sequential", []durable.AnyBranch{first, never},
			durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 1}))
		if err != nil {
			return out{}, err
		}
		_, err = never.Result(res)
		var bnc *durable.BranchNotCompletedError
		if !errors.As(err, &bnc) || bnc.Status != durable.BatchItemNotStarted || bnc.Index != 1 || bnc.Name != "never" {
			return out{}, fmt.Errorf("never-started branch: got %v (%#v)", err, bnc)
		}
		o.NotStarted = err.Error()

		fast := durable.NewTypedBranch("fast", func(_ durable.Context) (int, error) { return 1, nil })
		slow := durable.NewTypedBranch("slow", func(c durable.Context) (int, error) {
			if err := durable.Wait(c, "long", time.Hour); err != nil {
				return 0, err
			}
			return 2, nil
		})
		res, err = durable.ParallelMixed(ctx, "early", []durable.AnyBranch{fast, slow},
			durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 1}))
		if err != nil {
			return out{}, err
		}
		_, err = slow.Result(res)
		bnc = nil
		if !errors.As(err, &bnc) || bnc.Status != durable.BatchItemStarted || bnc.Index != 1 || bnc.Name != "slow" {
			return out{}, fmt.Errorf("abandoned branch: got %v (%#v)", err, bnc)
		}
		o.Abandoned = err.Error()
		return o, nil
	}
	r := mixedRun(t, h, struct{}{})
	got, err := durabletest.ResultAs[out](r)
	if err != nil {
		t.Fatal(err)
	}
	if want := `durable: branch "never" (index 1) produced no result: the branch never started`; got.NotStarted != want {
		t.Errorf("never-started message = %q, want %q", got.NotStarted, want)
	}
	if want := `durable: branch "slow" (index 1) produced no result: the branch was abandoned when the batch completed early`; got.Abandoned != want {
		t.Errorf("abandoned message = %q, want %q", got.Abandoned, want)
	}
}

// TestParallelMixedOwnership asserts reading a branch with the result of a
// different ParallelMixed call, or reading a branch never passed to a
// call, returns an error that is not a *BranchNotCompletedError.
func TestParallelMixedOwnership(t *testing.T) {
	h := func(ctx durable.Context, _ struct{}) ([]string, error) {
		a := durable.NewTypedBranch("a", func(_ durable.Context) (int, error) { return 1, nil })
		b := durable.NewTypedBranch("b", func(_ durable.Context) (string, error) { return "b", nil })
		resA, err := durable.ParallelMixed(ctx, "first", []durable.AnyBranch{a})
		if err != nil {
			return nil, err
		}
		resB, err := durable.ParallelMixed(ctx, "second", []durable.AnyBranch{b})
		if err != nil {
			return nil, err
		}
		_, e1 := a.Result(resB)
		_, e2 := b.Result(resA)
		_, e3 := durable.NewTypedBranch("loose", func(_ durable.Context) (int, error) { return 0, nil }).Result(resA)
		_, e4 := a.Result(durable.BatchResult[json.RawMessage]{})
		var msgs []string
		for _, e := range []error{e1, e2, e3, e4} {
			if e == nil {
				return nil, errors.New("Result returned nil error for a foreign or unbound read")
			}
			var bnc *durable.BranchNotCompletedError
			if errors.As(e, &bnc) {
				return nil, fmt.Errorf("Result returned a *BranchNotCompletedError: %v", e)
			}
			msgs = append(msgs, e.Error())
		}
		// A branch passed to a second call belongs to that call.
		resA2, err := durable.ParallelMixed(ctx, "third", []durable.AnyBranch{a})
		if err != nil {
			return nil, err
		}
		if _, err := a.Result(resA); err == nil {
			return nil, errors.New("branch read with the result of the call before its latest one returned nil error")
		}
		if v, err := a.Result(resA2); err != nil || v != 1 {
			return nil, fmt.Errorf("branch read with its latest call = %v, %v", v, err)
		}
		return msgs, nil
	}
	r := mixedRun(t, h, struct{}{})
	got, err := durabletest.ResultAs[[]string](r)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`durable: branch "a" read with the result of a different ParallelMixed call`,
		`durable: branch "b" read with the result of a different ParallelMixed call`,
		`durable: branch "loose" was never passed to a ParallelMixed call`,
		`durable: branch "a" read with the result of a different ParallelMixed call`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("messages = %q, want %q", got, want)
	}
}

// binSerdes encodes an int as the bytes "BIN:<n>", which are not JSON. It
// counts its calls and records each SerdesContext operation ID.
type binSerdes struct {
	mu        sync.Mutex
	marshals  int
	marshalID []string
}

func (s *binSerdes) Marshal(_ context.Context, sc durable.SerdesContext, v any) ([]byte, error) {
	s.mu.Lock()
	s.marshals++
	s.marshalID = append(s.marshalID, sc.OperationID)
	s.mu.Unlock()
	n, ok := v.(int)
	if !ok {
		return nil, fmt.Errorf("binSerdes: got %T", v)
	}
	return []byte("BIN:" + strconv.Itoa(n)), nil
}

func (s *binSerdes) Unmarshal(_ context.Context, _ durable.SerdesContext, data []byte, v any) error {
	n, err := strconv.Atoi(strings.TrimPrefix(string(data), "BIN:"))
	if err != nil {
		return err
	}
	p, ok := v.(*int)
	if !ok {
		return fmt.Errorf("binSerdes: cannot decode into %T", v)
	}
	*p = n
	return nil
}

// TestParallelMixedBranchSerdesBytesStoredOnce asserts the branch serdes
// marshals the branch value once, with the branch's own SerdesContext,
// that the batch stores its bytes unchanged even when they are not JSON,
// and that Result decodes them on the first run and on replay.
func TestParallelMixedBranchSerdesBytesStoredOnce(t *testing.T) {
	bin := &binSerdes{}
	var reads []int
	var mu sync.Mutex
	h := func(ctx durable.Context, _ struct{}) (int, error) {
		n := durable.NewTypedBranch("n", func(_ durable.Context) (int, error) { return 7, nil }, durable.WithTypedBranchSerdes(bin))
		s := durable.NewTypedBranch("s", func(_ durable.Context) (string, error) { return "plain", nil })
		res, err := durable.ParallelMixed(ctx, "p", []durable.AnyBranch{n, s})
		if err != nil {
			return 0, err
		}
		v, err := n.Result(res)
		if err != nil {
			return 0, err
		}
		if str, err := s.Result(res); err != nil || str != "plain" {
			return 0, fmt.Errorf("s = %q, %v", str, err)
		}
		mu.Lock()
		reads = append(reads, v)
		mu.Unlock()
		if err := durable.Wait(ctx, "after", time.Second); err != nil {
			return 0, err
		}
		return v, nil
	}
	r := mixedRun(t, h, struct{}{})
	if !reflect.DeepEqual(reads, []int{7, 7}) {
		t.Errorf("reads = %v, want [7 7] (first run and replay)", reads)
	}
	if bin.marshals != 1 {
		t.Errorf("branch serdes Marshal ran %d times, want 1", bin.marshals)
	}
	var stored string
	for _, op := range r.Operations {
		if op.Name == "n" && op.ContextDetails != nil {
			stored = op.ContextDetails.Result
		}
	}
	if stored != "BIN:7" {
		t.Errorf("stored branch payload = %q, want %q", stored, "BIN:7")
	}

	// The branch serdes sees the operation ID a Parallel[int] item at the
	// same position sees.
	ref := &binSerdes{}
	par := func(ctx durable.Context, _ struct{}) (int, error) {
		res, err := durable.Parallel(ctx, "p", []durable.Branch[int]{
			{Name: "n", Func: func(_ durable.Context) (int, error) { return 7, nil }},
		}, durable.WithBatchSerdes(ref))
		return res.SuccessCount(), err
	}
	mixedRun(t, par, struct{}{})
	if len(bin.marshalID) != 1 || len(ref.marshalID) == 0 || bin.marshalID[0] != ref.marshalID[0] || bin.marshalID[0] == "" {
		t.Errorf("branch SerdesContext.OperationID = %q, Parallel item = %q; want equal and non-empty", bin.marshalID, ref.marshalID)
	}
}

// TestParallelMixedRejectsInvalidBranches asserts a nil branch, a nil
// *TypedBranch, and a branch passed twice fail the call before anything is
// recorded.
func TestParallelMixedRejectsInvalidBranches(t *testing.T) {
	h := func(ctx durable.Context, _ struct{}) ([]string, error) {
		b := durable.NewTypedBranch("b", func(_ durable.Context) (int, error) { return 1, nil })
		_, e1 := durable.ParallelMixed(ctx, "nil", []durable.AnyBranch{b, nil})
		_, e2 := durable.ParallelMixed(ctx, "twice", []durable.AnyBranch{b, b})
		var typedNil *durable.TypedBranch[int]
		_, e3 := durable.ParallelMixed(ctx, "typed-nil", []durable.AnyBranch{b, typedNil})
		if e1 == nil || e2 == nil || e3 == nil {
			return nil, fmt.Errorf("errors = %v, %v, %v; want all non-nil", e1, e2, e3)
		}
		if want := `durable: ParallelMixed "typed-nil": branch 1 is nil`; e3.Error() != want {
			return nil, fmt.Errorf("typed-nil error = %q, want %q", e3, want)
		}
		return []string{e1.Error(), e2.Error(), e3.Error()}, nil
	}
	r := mixedRun(t, h, struct{}{})
	if len(r.Operations) != 0 {
		t.Errorf("recorded %d operations, want 0", len(r.Operations))
	}
}

// TestParallelMixedBatchSummaryType asserts a WithBatchSummary function
// for ParallelMixed takes BatchResult[json.RawMessage].
func TestParallelMixedBatchSummaryType(t *testing.T) {
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		b := durable.NewTypedBranch("b", func(_ durable.Context) (int, error) { return 1, nil })
		_, err := durable.ParallelMixed(ctx, "ok", []durable.AnyBranch{b},
			durable.WithBatchSummary(func(r durable.BatchResult[json.RawMessage]) string { return "s" }))
		if err != nil {
			return "", err
		}
		_, err = durable.ParallelMixed(ctx, "bad", []durable.AnyBranch{b},
			durable.WithBatchSummary(func(r durable.BatchResult[int]) string { return "s" }))
		if err == nil {
			return "", errors.New("summary of the wrong result type was accepted")
		}
		return err.Error(), nil
	}
	mixedRun(t, h, struct{}{})
}

// TestParallelMixedLargeBranchReplays asserts a branch whose stored bytes
// exceed the per-operation payload limit reads the same value on the first
// run and on replay, which runs the branch body again.
func TestParallelMixedLargeBranchReplays(t *testing.T) {
	big := strings.Repeat("x", 300*1024)
	var mu sync.Mutex
	var lens []int
	h := func(ctx durable.Context, _ struct{}) (int, error) {
		s := durable.NewTypedBranch("big", func(_ durable.Context) (string, error) { return big, nil })
		n := durable.NewTypedBranch("n", func(_ durable.Context) (int, error) { return 3, nil })
		res, err := durable.ParallelMixed(ctx, "p", []durable.AnyBranch{s, n})
		if err != nil {
			return 0, err
		}
		v, err := s.Result(res)
		if err != nil {
			return 0, err
		}
		if v != big {
			return 0, errors.New("big branch value differs")
		}
		k, err := n.Result(res)
		if err != nil {
			return 0, err
		}
		mu.Lock()
		lens = append(lens, len(v)+k)
		mu.Unlock()
		if err := durable.Wait(ctx, "after", time.Second); err != nil {
			return 0, err
		}
		return k, nil
	}
	mixedRun(t, h, struct{}{})
	if want := []int{len(big) + 3, len(big) + 3}; !reflect.DeepEqual(lens, want) {
		t.Errorf("reads = %v, want %v", lens, want)
	}
}
