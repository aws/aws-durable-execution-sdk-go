package durable

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	smithy "github.com/aws/smithy-go"
)

// gateLambda is an ExecutionClient whose Checkpoint call blocks until the
// test releases it. It gives a test deterministic control over what is "in
// flight" while other goroutines enqueue updates. Every call records its
// batch and token, rotates the token, and reports whether the checkpointer's
// mutex was free while the call ran.
type gateLambda struct {
	cp      *checkpointer
	release chan struct{}

	mu      sync.Mutex
	batches [][]OperationUpdate
	tokens  []string
	// muHeld counts calls during which cp.mu could not be acquired.
	muHeld int

	inFlight atomic.Int32
}

func newGateLambda() *gateLambda {
	return &gateLambda{release: make(chan struct{})}
}

func (g *gateLambda) GetExecutionState(_ context.Context, _ GetExecutionStateInput) (GetExecutionStateOutput, error) {
	return GetExecutionStateOutput{}, nil
}

func (g *gateLambda) Checkpoint(ctx context.Context, in CheckpointInput) (CheckpointOutput, error) {
	g.inFlight.Add(1)
	defer g.inFlight.Add(-1)

	g.mu.Lock()
	g.batches = append(g.batches, in.Updates)
	g.tokens = append(g.tokens, in.CheckpointToken)
	next := "token-" + strconv.Itoa(len(g.tokens))
	if g.cp != nil {
		if g.cp.mu.TryLock() {
			g.cp.mu.Unlock()
		} else {
			g.muHeld++
		}
	}
	g.mu.Unlock()

	select {
	case <-ctx.Done():
		return CheckpointOutput{}, ctx.Err()
	case <-g.release:
	}
	return CheckpointOutput{CheckpointToken: next}, nil
}

func (g *gateLambda) snapshot() (batches [][]OperationUpdate, tokens []string, muHeld int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([][]OperationUpdate(nil), g.batches...), append([]string(nil), g.tokens...), g.muHeld
}

// waitFor polls cond until it returns true or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func (cp *checkpointer) queueLen() int {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return len(cp.queue)
}

func idUpdate(id string) OperationUpdate {
	return OperationUpdate{Id: aws.String(id)}
}

// startInFlight issues one checkpoint on its own goroutine and returns once
// the fake reports the call is in flight. The returned channel yields the
// call's result.
func startInFlight(t *testing.T, cp *checkpointer, fake *gateLambda, id string) <-chan error {
	t.Helper()
	res := make(chan error, 1)
	go func() { res <- cp.checkpoint(context.Background(), []OperationUpdate{idUpdate(id)}) }()
	waitFor(t, "first call in flight", func() bool { return fake.inFlight.Load() == 1 })
	return res
}

func TestCheckpointCoalescesUpdatesArrivingWhileInFlight(t *testing.T) {
	const others = 15
	fake := newGateLambda()
	cp := newCheckpointer(fake, "arn:test", "token-0")
	fake.cp = cp

	first := startInFlight(t, cp, fake, "first")

	// While the first call is blocked, 15 more branches checkpoint.
	results := make(chan error, others)
	for i := range others {
		go func() {
			results <- cp.checkpoint(context.Background(), []OperationUpdate{idUpdate("op-" + strconv.Itoa(i))})
		}()
	}
	waitFor(t, "all others queued", func() bool { return cp.queueLen() == others })

	// Release the first call, then the coalesced second call.
	fake.release <- struct{}{}
	if err := <-first; err != nil {
		t.Fatalf("first checkpoint: %v", err)
	}
	waitFor(t, "second call in flight", func() bool { return fake.inFlight.Load() == 1 })
	fake.release <- struct{}{}
	for range others {
		if err := <-results; err != nil {
			t.Fatalf("coalesced checkpoint: %v", err)
		}
	}

	batches, tokens, muHeld := fake.snapshot()
	if len(batches) != 2 {
		t.Fatalf("backend received %d calls for %d operations, want 2", len(batches), others+1)
	}
	if len(batches[0]) != 1 || len(batches[1]) != others {
		t.Fatalf("batch sizes = [%d %d], want [1 %d]", len(batches[0]), len(batches[1]), others)
	}
	if tokens[0] != "token-0" || tokens[1] != "token-1" {
		t.Errorf("tokens sent = %v, want [token-0 token-1]", tokens)
	}
	if got := cp.currentToken(); got != "token-2" {
		t.Errorf("currentToken() = %q, want token-2", got)
	}
	if muHeld != 0 {
		t.Errorf("checkpointer mutex was held during %d client calls, want 0", muHeld)
	}
}

func TestCheckpointMutexFreeDuringRetryBackoff(t *testing.T) {
	serverErr := &smithy.GenericAPIError{Code: "ServiceException", Fault: smithy.FaultServer}
	var calls atomic.Int32
	fake := &fakeLambdaFunc{
		checkpoint: func(_ context.Context, _ CheckpointInput) (CheckpointOutput, error) {
			if calls.Add(1) == 1 {
				return CheckpointOutput{}, serverErr
			}
			return CheckpointOutput{CheckpointToken: "token-1"}, nil
		},
		getState: emptyGetState,
	}
	cp := newCheckpointer(fake, "arn:test", "token-0")

	done := make(chan error, 1)
	go func() { done <- cp.checkpoint(context.Background(), nil) }()
	waitFor(t, "first attempt made", func() bool { return calls.Load() == 1 })

	// The flusher is now sleeping for the first backoff (100ms). Reading the
	// token takes cp.mu, so it must return well before the sleep ends.
	tok := make(chan string, 1)
	go func() { tok <- cp.currentToken() }()
	select {
	case got := <-tok:
		if got != "token-0" {
			t.Errorf("currentToken() during backoff = %q, want token-0", got)
		}
	case <-time.After(checkpointBaseDelay / 2):
		t.Fatal("currentToken() blocked during retry backoff: mutex held across sleep")
	}
	if err := <-done; err != nil {
		t.Fatalf("checkpoint after retry: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("client calls = %d, want 2", got)
	}
}

func TestCheckpointBatchRespectsPayloadLimit(t *testing.T) {
	fake := newGateLambda()
	cp := newCheckpointer(fake, "arn:test", "token-0")
	first := startInFlight(t, cp, fake, "first")

	big := strings.Repeat("x", 400*1024)
	enqueue := func(id string, payload string) <-chan error {
		res := make(chan error, 1)
		u := OperationUpdate{Id: aws.String(id)}
		if payload != "" {
			u.Payload = aws.String(payload)
		}
		go func() { res <- cp.checkpoint(context.Background(), []OperationUpdate{u}) }()
		return res
	}
	// Queue in a fixed order so the batch boundary is deterministic.
	a := enqueue("a", big)
	waitFor(t, "a queued", func() bool { return cp.queueLen() == 1 })
	s := enqueue("s", "")
	waitFor(t, "s queued", func() bool { return cp.queueLen() == 2 })
	b := enqueue("b", big)
	waitFor(t, "b queued", func() bool { return cp.queueLen() == 3 })

	// Release: first, then [a s] (a+s fits under 750KB), then [b] alone
	// because a+s+b would exceed the limit.
	for range 3 {
		fake.release <- struct{}{}
	}
	for _, r := range []<-chan error{first, a, s, b} {
		if err := <-r; err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
	}

	batches, _, _ := fake.snapshot()
	var got []string
	for _, batch := range batches {
		var ids []string
		for _, u := range batch {
			ids = append(ids, aws.ToString(u.Id))
		}
		got = append(got, strings.Join(ids, ","))
	}
	want := []string{"first", "a,s", "b"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("batches = %v, want %v", got, want)
	}
}

func TestCheckpointBatchRespectsUpdateCountLimit(t *testing.T) {
	const n = checkpointMaxBatchUpdates + 1
	fake := newGateLambda()
	cp := newCheckpointer(fake, "arn:test", "token-0")
	first := startInFlight(t, cp, fake, "first")

	results := make(chan error, n)
	for i := range n {
		go func() {
			results <- cp.checkpoint(context.Background(), []OperationUpdate{idUpdate("op-" + strconv.Itoa(i))})
		}()
	}
	waitFor(t, "all queued", func() bool { return cp.queueLen() == n })

	for range 3 {
		fake.release <- struct{}{}
	}
	if err := <-first; err != nil {
		t.Fatalf("first checkpoint: %v", err)
	}
	for range n {
		if err := <-results; err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
	}
	batches, _, _ := fake.snapshot()
	if len(batches) != 3 || len(batches[1]) != checkpointMaxBatchUpdates || len(batches[2]) != 1 {
		sizes := make([]int, len(batches))
		for i, b := range batches {
			sizes[i] = len(b)
		}
		t.Fatalf("batch sizes = %v, want [1 %d 1]", sizes, checkpointMaxBatchUpdates)
	}
}

func TestCheckpointBranchUpdatesKeepIssueOrder(t *testing.T) {
	const rounds, noise = 25, 8
	fake := &slowLambda{latency: time.Millisecond}
	cp := newCheckpointer(fake, "arn:test", "token-0")

	var wg sync.WaitGroup
	// Noise branches checkpoint concurrently so that batches mix updates
	// from several goroutines.
	for i := range noise {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range rounds {
				id := "noise-" + strconv.Itoa(i) + "-" + strconv.Itoa(r)
				if err := cp.checkpoint(context.Background(), []OperationUpdate{idUpdate(id)}); err != nil {
					t.Errorf("noise checkpoint: %v", err)
					return
				}
			}
		}()
	}
	// The branch under test issues its updates one after another.
	for r := range rounds {
		if err := cp.checkpoint(context.Background(), []OperationUpdate{idUpdate("branch-" + strconv.Itoa(r))}); err != nil {
			t.Fatalf("branch checkpoint %d: %v", r, err)
		}
	}
	wg.Wait()

	fake.mu.Lock()
	defer fake.mu.Unlock()
	next := 0
	for _, batch := range fake.batches {
		for _, u := range batch {
			id := aws.ToString(u.Id)
			if !strings.HasPrefix(id, "branch-") {
				continue
			}
			if want := "branch-" + strconv.Itoa(next); id != want {
				t.Fatalf("branch update %q arrived where %q was expected", id, want)
			}
			next++
		}
	}
	if next != rounds {
		t.Fatalf("backend received %d branch updates, want %d", next, rounds)
	}
	if fake.calls >= rounds*(noise+1) {
		t.Errorf("backend received %d calls for %d operations; expected coalescing", fake.calls, rounds*(noise+1))
	}
}

func TestCheckpointNextCallUsesReturnedToken(t *testing.T) {
	const workers, rounds = 12, 10
	fake := &slowLambda{latency: 500 * time.Microsecond}
	cp := newCheckpointer(fake, "arn:test", "token-0")

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range rounds {
				id := strconv.Itoa(i) + "-" + strconv.Itoa(r)
				if err := cp.checkpoint(context.Background(), []OperationUpdate{idUpdate(id)}); err != nil {
					t.Errorf("checkpoint: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	fake.mu.Lock()
	defer fake.mu.Unlock()
	// slowLambda returns "token-<n>" from call n (1-based), so call n+1 must
	// send "token-<n>" and the first call sends the initial token.
	for i, tok := range fake.tokens {
		if want := "token-" + strconv.Itoa(i); tok != want {
			t.Fatalf("call %d sent token %q, want %q (token returned by call %d)", i+1, tok, want, i)
		}
	}
	if got, want := cp.currentToken(), "token-"+strconv.Itoa(fake.calls); got != want {
		t.Errorf("currentToken() = %q, want %q", got, want)
	}
	if fake.updates != workers*rounds {
		t.Errorf("backend received %d updates, want %d", fake.updates, workers*rounds)
	}
}

func TestCheckpointCancelledWhileQueuedIsNotSent(t *testing.T) {
	fake := newGateLambda()
	cp := newCheckpointer(fake, "arn:test", "token-0")
	first := startInFlight(t, cp, fake, "first")

	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() { res <- cp.checkpoint(ctx, []OperationUpdate{idUpdate("cancelled")}) }()
	waitFor(t, "cancelled request queued", func() bool { return cp.queueLen() == 1 })
	cancel()
	if err := <-res; !errors.Is(err, context.Canceled) {
		t.Fatalf("checkpoint with cancelled ctx = %v, want context.Canceled", err)
	}

	fake.release <- struct{}{}
	if err := <-first; err != nil {
		t.Fatalf("first checkpoint: %v", err)
	}
	waitFor(t, "queue drained", func() bool { return cp.queueLen() == 0 && fake.inFlight.Load() == 0 })

	batches, _, _ := fake.snapshot()
	for _, batch := range batches {
		for _, u := range batch {
			if aws.ToString(u.Id) == "cancelled" {
				t.Fatal("cancelled update was sent to the backend")
			}
		}
	}
}

func TestCheckpointTerminatedWhileQueuedIsRefused(t *testing.T) {
	fake := newGateLambda()
	cp := newCheckpointer(fake, "arn:test", "token-0")
	first := startInFlight(t, cp, fake, "first")

	res := make(chan error, 1)
	go func() { res <- cp.checkpoint(context.Background(), []OperationUpdate{idUpdate("late")}) }()
	waitFor(t, "late request queued", func() bool { return cp.queueLen() == 1 })

	cp.terminate()
	fake.release <- struct{}{}
	if err := <-first; !errors.Is(err, errCheckpointTerminated) {
		t.Fatalf("in-flight checkpoint after terminate = %v, want errCheckpointTerminated", err)
	}
	if err := <-res; !errors.Is(err, errCheckpointTerminated) {
		t.Fatalf("queued checkpoint after terminate = %v, want errCheckpointTerminated", err)
	}
	batches, _, _ := fake.snapshot()
	if len(batches) != 1 {
		t.Fatalf("backend received %d calls, want 1 (queued request must not be sent)", len(batches))
	}
	if got := cp.currentToken(); got != "token-0" {
		t.Errorf("currentToken() after terminate = %q, want unchanged token-0", got)
	}
}

func TestCheckpointFailedBatchReportsErrorToEveryRequest(t *testing.T) {
	clientErr := &smithy.GenericAPIError{Code: "InvalidParameterValueException", Fault: smithy.FaultClient}
	var calls atomic.Int32
	release := make(chan struct{})
	fake := &fakeLambdaFunc{
		checkpoint: func(_ context.Context, _ CheckpointInput) (CheckpointOutput, error) {
			n := calls.Add(1)
			<-release
			if n == 2 {
				return CheckpointOutput{}, clientErr
			}
			return CheckpointOutput{CheckpointToken: "token-" + strconv.Itoa(int(n))}, nil
		},
		getState: emptyGetState,
	}
	cp := newCheckpointer(fake, "arn:test", "token-0")

	first := make(chan error, 1)
	go func() { first <- cp.checkpoint(context.Background(), nil) }()
	waitFor(t, "first call in flight", func() bool { return calls.Load() == 1 })

	const n = 4
	results := make(chan error, n)
	for range n {
		go func() { results <- cp.checkpoint(context.Background(), []OperationUpdate{idUpdate("x")}) }()
	}
	waitFor(t, "all queued", func() bool { return cp.queueLen() == n })

	release <- struct{}{}
	if err := <-first; err != nil {
		t.Fatalf("first checkpoint: %v", err)
	}
	release <- struct{}{}
	for range n {
		if err := <-results; !errors.Is(err, clientErr) {
			t.Fatalf("coalesced request error = %v, want %v", err, clientErr)
		}
	}
	if got := cp.currentToken(); got != "token-1" {
		t.Errorf("currentToken() after failed batch = %q, want token-1 (unchanged by the failure)", got)
	}
}
