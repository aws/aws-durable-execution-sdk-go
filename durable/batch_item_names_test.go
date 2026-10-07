package durable_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// mapIterationNames returns the recorded operation names of the Map item
// contexts, in input order.
func mapIterationNames(r *durabletest.TestResult) []string {
	var names []string
	for i := range r.Operations {
		if r.Operations[i].SubType == "MapIteration" {
			names = append(names, r.Operations[i].Name)
		}
	}
	return names
}

// parallelBranchNames returns the recorded operation names of the Parallel
// branch contexts, in input order.
func parallelBranchNames(r *durabletest.TestResult) []string {
	var names []string
	for i := range r.Operations {
		if r.Operations[i].SubType == "ParallelBranch" {
			names = append(names, r.Operations[i].Name)
		}
	}
	return names
}

// runSucceeded runs h to completion and fails the test unless the
// execution succeeded.
func runSucceeded[O any](t *testing.T, h durable.Handler[any, O], opts ...durable.HandlerOption) *durabletest.TestResult {
	t.Helper()
	r, err := durabletest.NewLocalRunner(h, opts...).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", r.Status)
	}
	return r
}

// assertNames fails the test unless got equals want element by element.
func assertNames(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d names %q, want %d %q", what, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s[%d] = %q, want %q", what, i, got[i], want[i])
		}
	}
}

// TestUnnamedMapItemsAreNamedMapItemIndex asserts that a Map item with no
// configured name is named "map-item-<index>" in the recorded operation.
func TestUnnamedMapItemsAreNamedMapItemIndex(t *testing.T) {
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", []int{10, 20},
			func(c durable.Context, item, _ int) (int, error) {
				return durable.Step(c, "s", func(_ durable.StepContext) (int, error) {
					return item + 1, nil
				})
			})
		if err != nil {
			return 0, err
		}
		return res.SuccessCount(), nil
	}
	r := runSucceeded(t, h)
	assertNames(t, "MapIteration recorded name", mapIterationNames(r), []string{"map-item-0", "map-item-1"})
}

// TestUnnamedMapItemBatchResultLookup asserts the BatchResult carries the
// default item names and that lookup by name finds them.
func TestUnnamedMapItemBatchResultLookup(t *testing.T) {
	var names []string
	var found0 bool
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", []int{10, 20},
			func(c durable.Context, item, _ int) (int, error) {
				return durable.Step(c, "s", func(_ durable.StepContext) (int, error) { return item + 1, nil })
			})
		if err != nil {
			return 0, err
		}
		names = names[:0]
		for _, it := range res.Items {
			names = append(names, it.Name)
		}
		found0 = res.Item("map-item-0") != nil
		return res.SuccessCount(), nil
	}
	runSucceeded(t, h)
	assertNames(t, "Items.Name", names, []string{"map-item-0", "map-item-1"})
	if !found0 {
		t.Errorf("result.Item(\"map-item-0\") = nil, want the first item")
	}
}

// TestUnnamedParallelBranchesAreNamedParallelBranchIndex asserts that a
// Parallel branch with an empty Name is named "parallel-branch-<index>" in
// the BatchResult and in the recorded operation.
func TestUnnamedParallelBranchesAreNamedParallelBranchIndex(t *testing.T) {
	var names []string
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Parallel(ctx, "p", []durable.Branch[int]{
			{Name: "", Func: func(c durable.Context) (int, error) {
				return durable.Step(c, "s", func(_ durable.StepContext) (int, error) { return 1, nil })
			}},
			{Name: "", Func: func(c durable.Context) (int, error) {
				return durable.Step(c, "s", func(_ durable.StepContext) (int, error) { return 2, nil })
			}},
		})
		if err != nil {
			return 0, err
		}
		names = names[:0]
		for _, it := range res.Items {
			names = append(names, it.Name)
		}
		return res.SuccessCount(), nil
	}
	r := runSucceeded(t, h)
	want := []string{"parallel-branch-0", "parallel-branch-1"}
	assertNames(t, "Items.Name", names, want)
	assertNames(t, "ParallelBranch recorded name", parallelBranchNames(r), want)
}

// TestConfiguredBatchItemNamesAreUsedUnchanged asserts that a non-empty
// name from WithItemNamer, and a non-empty Branch.Name, are used as given.
// A Parallel that mixes named and unnamed branches names only the unnamed
// one by its index.
func TestConfiguredBatchItemNamesAreUsedUnchanged(t *testing.T) {
	t.Run("Map", func(t *testing.T) {
		ids := []string{"order-a", "order-b"}
		var names []string
		h := func(ctx durable.Context, _ any) (int, error) {
			res, err := durable.Map(ctx, "m", ids,
				func(_ durable.Context, _ string, i int) (int, error) { return i, nil },
				durable.WithItemNamer(func(i int) string { return ids[i] }))
			if err != nil {
				return 0, err
			}
			names = names[:0]
			for _, it := range res.Items {
				names = append(names, it.Name)
			}
			return res.SuccessCount(), nil
		}
		r := runSucceeded(t, h)
		assertNames(t, "Items.Name", names, ids)
		assertNames(t, "MapIteration recorded name", mapIterationNames(r), ids)
	})
	t.Run("Parallel", func(t *testing.T) {
		var names []string
		h := func(ctx durable.Context, _ any) (int, error) {
			res, err := durable.Parallel(ctx, "p", []durable.Branch[int]{
				{Name: "first", Func: func(durable.Context) (int, error) { return 1, nil }},
				{Name: "", Func: func(durable.Context) (int, error) { return 2, nil }},
				{Name: "third", Func: func(durable.Context) (int, error) { return 3, nil }},
			})
			if err != nil {
				return 0, err
			}
			names = names[:0]
			for _, it := range res.Items {
				names = append(names, it.Name)
			}
			return res.SuccessCount(), nil
		}
		r := runSucceeded(t, h)
		want := []string{"first", "parallel-branch-1", "third"}
		assertNames(t, "Items.Name", names, want)
		assertNames(t, "ParallelBranch recorded name", parallelBranchNames(r), want)
	})
}

// TestItemNamerEmptyFallsBackToMapItemIndex asserts that an index for which
// WithItemNamer returns "" is named "map-item-<index>", and that the index
// is written in decimal with no padding.
func TestItemNamerEmptyFallsBackToMapItemIndex(t *testing.T) {
	items := make([]int, 12)
	var names []string
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", items,
			func(_ durable.Context, _ int, i int) (int, error) { return i, nil },
			durable.WithItemNamer(func(i int) string {
				if i%2 == 0 {
					return ""
				}
				return "odd"
			}))
		if err != nil {
			return 0, err
		}
		names = names[:0]
		for _, it := range res.Items {
			names = append(names, it.Name)
		}
		return res.SuccessCount(), nil
	}
	r := runSucceeded(t, h)
	want := []string{
		"map-item-0", "odd", "map-item-2", "odd", "map-item-4", "odd",
		"map-item-6", "odd", "map-item-8", "odd", "map-item-10", "odd",
	}
	assertNames(t, "Items.Name", names, want)
	assertNames(t, "MapIteration recorded name", mapIterationNames(r), want)
}

// TestUnnamedMapItemResultByName asserts that BatchResult.Result finds the
// second item of an unnamed Map by its default name.
func TestUnnamedMapItemResultByName(t *testing.T) {
	var got int
	var ok bool
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", []int{10, 20},
			func(c durable.Context, item, _ int) (int, error) {
				return durable.Step(c, "s", func(_ durable.StepContext) (int, error) { return item + 1, nil })
			})
		if err != nil {
			return 0, err
		}
		got, ok = res.Result("map-item-1")
		return res.SuccessCount(), nil
	}
	runSucceeded(t, h)
	if !ok || got != 21 {
		t.Errorf("Result(\"map-item-1\") = (%d, %v), want (21, true)", got, ok)
	}
}

// TestUnnamedBatchItemNamesAreStableAcrossReplay asserts that an unnamed
// item keeps its default name when the batch is replayed: the handler
// suspends on a wait inside each item, and the names read after the
// replaying invocation equal the names recorded by the first.
func TestUnnamedBatchItemNamesAreStableAcrossReplay(t *testing.T) {
	var names []string
	h := func(ctx durable.Context, _ any) (int, error) {
		res, err := durable.Map(ctx, "m", []int{10, 20},
			func(c durable.Context, item, _ int) (int, error) {
				if err := durable.Wait(c, "w", time.Second); err != nil {
					return 0, err
				}
				return item, nil
			})
		if err != nil {
			return 0, err
		}
		names = names[:0]
		for _, it := range res.Items {
			names = append(names, it.Name)
		}
		return res.SuccessCount(), nil
	}
	r := runSucceeded(t, h)
	want := []string{"map-item-0", "map-item-1"}
	assertNames(t, "Items.Name", names, want)
	assertNames(t, "MapIteration recorded name", mapIterationNames(r), want)
}

// TestUnnamedBatchItemLogsCarryDefaultOperationName asserts that a log
// record emitted inside an unnamed Map item or Parallel branch carries the
// default name in its operationName field.
func TestUnnamedBatchItemLogsCarryDefaultOperationName(t *testing.T) {
	var mu sync.Mutex
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&lockedWriter{mu: &mu, w: &buf}, nil)
	h := func(ctx durable.Context, _ any) (int, error) {
		if _, err := durable.Map(ctx, "m", []int{0},
			func(c durable.Context, _, _ int) (int, error) {
				c.Logger().Info("in-map-item")
				return 0, nil
			}); err != nil {
			return 0, err
		}
		if _, err := durable.Parallel(ctx, "p", []durable.Branch[int]{
			{Func: func(c durable.Context) (int, error) {
				c.Logger().Info("in-parallel-branch")
				return 0, nil
			}},
		}); err != nil {
			return 0, err
		}
		return 0, nil
	}
	runSucceeded(t, h, durable.WithLogHandler(handler))

	mu.Lock()
	defer mu.Unlock()
	want := map[string]string{
		"in-map-item":        "map-item-0",
		"in-parallel-branch": "parallel-branch-0",
	}
	seen := map[string]bool{}
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		msg, _ := rec["msg"].(string)
		if msg == "" {
			msg, _ = rec["message"].(string)
		}
		wantName, ok := want[msg]
		if !ok {
			continue
		}
		seen[msg] = true
		if got := rec["operationName"]; got != wantName {
			t.Errorf("record %q: operationName = %v, want %q", msg, got, wantName)
		}
	}
	for msg := range want {
		if !seen[msg] {
			t.Errorf("no log record %q", msg)
		}
	}
}

// lockedWriter serializes writes from concurrent batch items.
type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
