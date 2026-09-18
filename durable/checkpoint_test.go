package durable

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// fakeLambda is an in-memory ExecutionClient double recording calls and serving
// canned pages. Checkpoint bookkeeping is mutex-guarded so the concurrency
// test can hammer it from multiple goroutines.
type fakeLambda struct {
	statePages [][]Operation
	stateErr   error

	checkpointErr error

	mu               sync.Mutex
	nextToken        string
	rotateTokens     bool
	gotTokens        []string
	gotUpdateBatches [][]OperationUpdate
}

func (f *fakeLambda) GetExecutionState(_ context.Context, in GetExecutionStateInput) (GetExecutionStateOutput, error) {
	if f.stateErr != nil {
		return GetExecutionStateOutput{}, f.stateErr
	}
	page := 0
	if in.Marker != "" {
		var err error
		if page, err = strconv.Atoi(in.Marker); err != nil {
			return GetExecutionStateOutput{}, fmt.Errorf("fake: bad marker %q: %w", in.Marker, err)
		}
	}
	out := GetExecutionStateOutput{Operations: f.statePages[page]}
	if next := page + 1; next < len(f.statePages) {
		out.NextMarker = strconv.Itoa(next)
	}
	return out, nil
}

func (f *fakeLambda) Checkpoint(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotTokens = append(f.gotTokens, in.CheckpointToken)
	f.gotUpdateBatches = append(f.gotUpdateBatches, in.Updates)
	if f.checkpointErr != nil {
		return CheckpointOutput{}, f.checkpointErr
	}
	if f.rotateTokens {
		f.nextToken = "token-" + strconv.Itoa(len(f.gotTokens))
	}
	if f.nextToken == "" {
		// An empty token means the backend returned none, which the
		// checkpointer rejects. Default to a fixed valid token so tests
		// that don't exercise rotation succeed.
		f.nextToken = "token-fake"
	}
	return CheckpointOutput{CheckpointToken: f.nextToken}, nil
}

func opWire(id string, status OperationStatus) Operation {
	return Operation{Id: aws.String(id), Status: status}
}

func TestLoadStateFollowsPagination(t *testing.T) {
	fake := &fakeLambda{
		statePages: [][]Operation{
			{opWire(hashID("1"), OperationStatusSucceeded)},
			{opWire(hashID("2"), OperationStatusFailed)},
			{opWire(hashID("2-1"), OperationStatusStarted)},
		},
	}
	cp := newCheckpointer(fake, "arn:test", "token-0")

	state, err := cp.loadState(context.Background())
	if err != nil {
		t.Fatalf("loadState() error: %v", err)
	}
	if got := len(state.operations); got != 3 {
		t.Fatalf("loadState() returned %d operations, want 3", got)
	}
	tests := []struct {
		id   string
		want operationStatus
	}{
		{"1", statusSucceeded},
		{"2", statusFailed},
		{"2-1", statusStarted},
	}
	for _, tt := range tests {
		op := state.get(tt.id)
		if op == nil {
			t.Errorf("get(%q) = nil, want operation", tt.id)
			continue
		}
		if op.status != tt.want {
			t.Errorf("get(%q).status = %q, want %q", tt.id, op.status, tt.want)
		}
	}
}

func TestLoadStateError(t *testing.T) {
	wantErr := errors.New("boom")
	cp := newCheckpointer(&fakeLambda{stateErr: wantErr}, "arn:test", "token-0")

	if _, err := cp.loadState(context.Background()); !errors.Is(err, wantErr) {
		t.Errorf("loadState() error = %v, want wrapped %v", err, wantErr)
	}
}

func TestCheckpointRotatesToken(t *testing.T) {
	fake := &fakeLambda{nextToken: "token-1"}
	cp := newCheckpointer(fake, "arn:test", "token-0")

	if err := cp.checkpoint(context.Background(), nil); err != nil {
		t.Fatalf("checkpoint() 1: %v", err)
	}
	fake.nextToken = "token-2"
	if err := cp.checkpoint(context.Background(), nil); err != nil {
		t.Fatalf("checkpoint() 2: %v", err)
	}

	want := []string{"token-0", "token-1"}
	if len(fake.gotTokens) != len(want) {
		t.Fatalf("checkpoint calls sent %d tokens, want %d", len(fake.gotTokens), len(want))
	}
	for i, tok := range want {
		if fake.gotTokens[i] != tok {
			t.Errorf("checkpoint call %d sent token %q, want %q", i+1, fake.gotTokens[i], tok)
		}
	}
	if got := cp.currentToken(); got != "token-2" {
		t.Errorf("currentToken() after rotation = %q, want %q", got, "token-2")
	}
}

func TestCheckpointErrorKeepsToken(t *testing.T) {
	wantErr := errors.New("throttled")
	cp := newCheckpointer(&fakeLambda{checkpointErr: wantErr}, "arn:test", "token-0")

	if err := cp.checkpoint(context.Background(), nil); !errors.Is(err, wantErr) {
		t.Fatalf("checkpoint() error = %v, want wrapped %v", err, wantErr)
	}
	if got := cp.currentToken(); got != "token-0" {
		t.Errorf("currentToken() after failed checkpoint = %q, want unchanged %q", got, "token-0")
	}
}

func TestCheckpointNilTokenOnSuccess(t *testing.T) {
	fake := &fakeLambda{} // nextToken empty: fake returns pointer to ""
	cp := newCheckpointer(&nilTokenLambda{fake}, "arn:test", "token-0")

	if err := cp.checkpoint(context.Background(), nil); err == nil {
		t.Error("checkpoint() with nil response token = nil error, want error")
	}
	if got := cp.currentToken(); got != "token-0" {
		t.Errorf("currentToken() after nil-token response = %q, want unchanged %q", got, "token-0")
	}
}

// nilTokenLambda wraps fakeLambda and clears the checkpoint token on
// successful responses.
type nilTokenLambda struct {
	*fakeLambda
}

func (n *nilTokenLambda) Checkpoint(ctx context.Context, in CheckpointInput) (CheckpointOutput, error) {
	out, err := n.fakeLambda.Checkpoint(ctx, in)
	if err != nil {
		return CheckpointOutput{}, err
	}
	out.CheckpointToken = ""
	return out, nil
}

func TestLoadStateEmptyExecution(t *testing.T) {
	fake := &fakeLambda{statePages: [][]Operation{{}}}
	cp := newCheckpointer(fake, "arn:test", "token-0")

	state, err := cp.loadState(context.Background())
	if err != nil {
		t.Fatalf("loadState() error: %v", err)
	}
	if got := len(state.operations); got != 0 {
		t.Errorf("loadState() on empty execution returned %d operations, want 0", got)
	}
}

func TestCheckpointConcurrentRotation(t *testing.T) {
	const workers = 32
	fake := &fakeLambda{rotateTokens: true}
	cp := newCheckpointer(fake, "arn:test", "token-0")

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			update := OperationUpdate{Id: aws.String("op-" + strconv.Itoa(i))}
			if err := cp.checkpoint(context.Background(), []OperationUpdate{update}); err != nil {
				t.Errorf("concurrent checkpoint(): %v", err)
			}
		}()
	}
	wg.Wait()

	// Concurrent requests may be coalesced, so the backend sees at most one
	// call per worker, and every worker's update arrives exactly once.
	calls := len(fake.gotTokens)
	if calls == 0 || calls > workers {
		t.Fatalf("backend received %d checkpoint calls, want between 1 and %d", calls, workers)
	}
	seenID := make(map[string]int, workers)
	for _, batch := range fake.gotUpdateBatches {
		for _, u := range batch {
			seenID[aws.ToString(u.Id)]++
		}
	}
	if len(seenID) != workers {
		t.Fatalf("backend received %d distinct updates, want %d", len(seenID), workers)
	}
	for id, n := range seenID {
		if n != 1 {
			t.Errorf("update %q sent %d times, want 1", id, n)
		}
	}
	// Serialization invariant: calls are issued one at a time, so call n+1
	// carries the token that call n returned and no token is reused.
	for i, tok := range fake.gotTokens {
		if want := "token-" + strconv.Itoa(i); tok != want {
			t.Fatalf("call %d sent token %q, want %q", i+1, tok, want)
		}
	}
	if got, want := cp.currentToken(), "token-"+strconv.Itoa(calls); got != want {
		t.Errorf("currentToken() after %d rotations = %q, want %q", calls, got, want)
	}
}
