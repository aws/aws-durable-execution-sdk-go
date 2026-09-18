package durable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// replayOpsFromUpdates folds the checkpoint updates recorded by a fake into
// the operation list a following invocation receives as its initial state,
// so a handler can be re-invoked against exactly what it checkpointed.
func replayOpsFromUpdates(updates []OperationUpdate) []wireOperation {
	byID := map[string]*wireOperation{}
	var order []string
	for _, u := range updates {
		id := aws.ToString(u.Id)
		op, seen := byID[id]
		if !seen {
			op = &wireOperation{
				Id:       id,
				ParentId: aws.ToString(u.ParentId),
				Type:     string(u.Type),
				SubType:  aws.ToString(u.SubType),
				Name:     aws.ToString(u.Name),
			}
			byID[id] = op
			order = append(order, id)
		}
		switch u.Action {
		case OperationActionStart:
			op.Status = "STARTED"
		case OperationActionSucceed:
			op.Status = "SUCCEEDED"
		case OperationActionFail:
			op.Status = "FAILED"
		case OperationActionRetry, OperationActionCancel:
			continue
		}
		switch u.Type {
		case OperationTypeStep:
			if u.Action == OperationActionSucceed {
				op.StepDetails = &wireStepDetails{Result: aws.ToString(u.Payload)}
			}
		case OperationTypeContext:
			if u.Action == OperationActionSucceed {
				op.ContextDetails = &wireContextDetails{
					Result:         aws.ToString(u.Payload),
					ReplayChildren: u.ContextOptions != nil && aws.ToBool(u.ContextOptions.ReplayChildren),
				}
			}
		}
	}
	ops := make([]wireOperation, 0, len(order))
	for _, id := range order {
		ops = append(ops, *byID[id])
	}
	return ops
}

// TestBatchReplayCounterMatchesLive asserts, for every batch configuration
// that takes a distinct ID-allocation path, that replaying a terminal batch
// leaves the enclosing context's ID counter where live execution left it.
// If the two differ, every operation after the batch gets a new ID on the
// resume invocation and executes again.
func TestBatchReplayCounterMatchesLive(t *testing.T) {
	cases := []struct {
		name  string
		items []int
		opts  []BatchOption
	}{
		{"sequential NORMAL", []int{1, 2}, []BatchOption{WithMaxConcurrency(1)}},
		{"sequential FLAT", []int{1, 2}, []BatchOption{WithMaxConcurrency(1), WithNesting(NestingFlat)}},
		{"concurrent NORMAL", []int{1, 2}, []BatchOption{WithMaxConcurrency(2)}},
		{"concurrent FLAT", []int{1, 2}, []BatchOption{WithMaxConcurrency(2), WithNesting(NestingFlat)}},
		{"single item default concurrency NORMAL", []int{1}, nil},
		{"single item default concurrency FLAT", []int{1}, []BatchOption{WithNesting(NestingFlat)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var counters []int
			handler := func(ctx Context, _ any) (int, error) {
				br, err := Map(ctx, "batch", tc.items, func(c Context, item int, _ int) (int, error) {
					return Step(c, "s", func(StepContext) (int, error) { return item, nil })
				}, tc.opts...)
				if err != nil {
					return 0, err
				}
				ec, ok := ctx.(*execContext)
				if !ok {
					t.Fatalf("context is %T, want *execContext", ctx)
				}
				counters = append(counters, ec.ids.counter)
				return Step(ctx, "after", func(StepContext) (int, error) { return br.SuccessCount(), nil })
			}

			live := &fakeLambda{}
			assertSucceeded(t, invokeBatch(t, live, batchPayload(`null`), handler))

			replay := &fakeLambda{}
			assertSucceeded(t, invokeBatch(t, replay, batchPayload(`null`, replayOpsFromUpdates(updateBatch(t, live))...), handler))

			if len(counters) != 2 {
				t.Fatalf("handler ran %d times, want 2", len(counters))
			}
			if counters[1] != counters[0] {
				t.Errorf("ID counter after batch: replay = %d, live = %d", counters[1], counters[0])
			}
			for _, u := range updateBatch(t, replay) {
				if u.Type == OperationTypeStep {
					t.Errorf("replay checkpointed a STEP update (%s %s): an operation after the batch re-executed", aws.ToString(u.Name), u.Action)
				}
			}
		})
	}
}

// TestFlatItemsRecordBatchAsParent asserts that the operations inside FLAT
// items record the batch as their parent on both the sequential and the
// concurrent path: the virtual item context is never checkpointed, so the
// batch is the nearest checkpointed ancestor.
func TestFlatItemsRecordBatchAsParent(t *testing.T) {
	for _, concurrency := range []int{1, 2} {
		fake := &fakeLambda{}
		resp := invokeBatch(t, fake, batchPayload(`null`), func(ctx Context, _ any) ([]int, error) {
			br, err := Map(ctx, "flat", []int{1, 2}, func(c Context, item int, _ int) (int, error) {
				return Step(c, "s", func(StepContext) (int, error) { return item, nil })
			}, WithMaxConcurrency(concurrency), WithNesting(NestingFlat))
			if err != nil {
				return nil, err
			}
			return br.Results(), nil
		})
		assertSucceeded(t, resp)
		steps := 0
		for _, u := range updateBatch(t, fake) {
			if u.Type != OperationTypeStep {
				continue
			}
			steps++
			if got, want := aws.ToString(u.ParentId), hashID("1"); got != want {
				t.Errorf("concurrency %d: step ParentId = %q, want the batch %q", concurrency, got, want)
			}
		}
		if steps == 0 {
			t.Fatalf("concurrency %d: no STEP updates recorded", concurrency)
		}
	}
}

// TestBatchItemSerdesOperationIDsDistinct asserts that every serdes call
// for an item's result, including the aggregate the batch checkpoints, is
// keyed on the item's own operation ID: with a serdes that writes one file
// per OperationID, a batch of n items leaves exactly n files, and the
// results survive the round trip through replay.
func TestBatchItemSerdesOperationIDsDistinct(t *testing.T) {
	cases := []struct {
		name string
		opts []BatchOption
	}{
		{"sequential FLAT", []BatchOption{WithMaxConcurrency(1), WithNesting(NestingFlat)}},
		{"concurrent FLAT", []BatchOption{WithMaxConcurrency(3), WithNesting(NestingFlat)}},
		{"sequential NORMAL", []BatchOption{WithMaxConcurrency(1)}},
		{"concurrent NORMAL", []BatchOption{WithMaxConcurrency(3)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			items := []string{"a", "b", "c"}
			handler := func(ctx Context, _ any) ([]string, error) {
				opts := append([]BatchOption{WithBatchSerdes(NewFileSystemSerdes(dir))}, tc.opts...)
				br, err := Map(ctx, "m", items, func(c Context, item string, _ int) (string, error) {
					return Step(c, "s", func(StepContext) (string, error) { return item, nil })
				}, opts...)
				if err != nil {
					return nil, err
				}
				return br.Results(), nil
			}

			live := &fakeLambda{}
			resp := invokeBatch(t, live, batchPayload(`null`), handler)
			assertSucceeded(t, resp)
			if resp.Result != `["a","b","c"]` {
				t.Fatalf("live result = %s", resp.Result)
			}

			var files []string
			if err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
				if err == nil && !d.IsDir() {
					files = append(files, filepath.Base(path))
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if len(files) != len(items) {
				t.Errorf("serdes wrote %d files %v, want one per item (%d)", len(files), files, len(items))
			}

			replay := &fakeLambda{}
			resp = invokeBatch(t, replay, batchPayload(`null`, replayOpsFromUpdates(updateBatch(t, live))...), handler)
			assertSucceeded(t, resp)
			if resp.Result != `["a","b","c"]` {
				t.Errorf("replay result = %s, want the live results", resp.Result)
			}
		})
	}
}
