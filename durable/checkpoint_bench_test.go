package durable

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowLambda is an ExecutionClient whose Checkpoint call takes a fixed
// amount of time. It stands in for a backend round trip so that checkpoint
// concurrency effects are visible in a benchmark or test. It rotates the
// token on every call and records every batch it receives.
type slowLambda struct {
	latency time.Duration

	mu      sync.Mutex
	calls   int
	updates int
	batches [][]OperationUpdate
	tokens  []string

	// inFlight counts calls currently inside Checkpoint. A test may read it
	// to observe whether two calls ever overlap.
	inFlight atomic.Int32
}

func (s *slowLambda) GetExecutionState(_ context.Context, _ GetExecutionStateInput) (GetExecutionStateOutput, error) {
	return GetExecutionStateOutput{}, nil
}

func (s *slowLambda) Checkpoint(ctx context.Context, in CheckpointInput) (CheckpointOutput, error) {
	s.inFlight.Add(1)
	defer s.inFlight.Add(-1)

	s.mu.Lock()
	s.calls++
	s.updates += len(in.Updates)
	s.batches = append(s.batches, in.Updates)
	s.tokens = append(s.tokens, in.CheckpointToken)
	next := "token-" + strconv.Itoa(s.calls)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		return CheckpointOutput{}, ctx.Err()
	case <-time.After(s.latency):
	}
	return CheckpointOutput{CheckpointToken: next}, nil
}

func (s *slowLambda) stats() (calls, updates int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.updates
}

// BenchmarkMapCheckpointLatency measures the wall time spent per checkpointed
// operation while a 20-item Map runs all items concurrently against a
// backend with a fixed 2ms round trip. Each item is one Step, so each item
// checkpoints twice (START and SUCCEED) plus the Map bookkeeping.
//
// The reported ns/checkpoint metric is invocation wall time divided by the
// number of operation updates the backend received. api-calls/invocation is
// how many Checkpoint calls one invocation made.
func BenchmarkMapCheckpointLatency(b *testing.B) {
	const items = 20
	list := make([]string, items)
	for i := range list {
		list[i] = strconv.Itoa(i)
	}
	event, err := json.Marshal(list)
	if err != nil {
		b.Fatal(err)
	}
	payload := batchPayload(string(event))

	handler := func(ctx Context, in []string) (int, error) {
		res, err := Map(ctx, "map", in, func(child Context, item string, _ int) (string, error) {
			return Step(child, "", func(StepContext) (string, error) {
				return item, nil
			})
		})
		if err != nil {
			return 0, err
		}
		return len(res.Results()), nil
	}

	var totalCalls, totalUpdates int
	b.ResetTimer()
	start := time.Now()
	for range b.N {
		fake := &slowLambda{latency: 2 * time.Millisecond}
		h := Wrap(handler, withLambdaAPI(fake))
		if _, err := h(context.Background(), payload); err != nil {
			b.Fatalf("invoke: %v", err)
		}
		calls, updates := fake.stats()
		totalCalls += calls
		totalUpdates += updates
	}
	elapsed := time.Since(start)
	b.StopTimer()

	if totalUpdates > 0 {
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(totalUpdates), "ns/checkpoint")
	}
	b.ReportMetric(float64(totalCalls)/float64(b.N), "api-calls/invocation")
}
