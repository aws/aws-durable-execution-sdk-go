package durable

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// fakeLambda is an in-memory ExecutionClient double recording calls and serving
// canned pages. Checkpoint bookkeeping is mutex-guarded so the concurrency
// test can hammer it from multiple goroutines.
type fakeLambda struct {
	statePages [][]types.Operation
	stateErr   error

	checkpointErr error

	mu               sync.Mutex
	nextToken        string
	rotateTokens     bool
	gotTokens        []string
	gotUpdateBatches [][]types.OperationUpdate
}

func (f *fakeLambda) GetDurableExecutionState(_ context.Context, in *lambda.GetDurableExecutionStateInput, _ ...func(*lambda.Options)) (*lambda.GetDurableExecutionStateOutput, error) {
	if f.stateErr != nil {
		return nil, f.stateErr
	}
	page := 0
	if in.Marker != nil {
		var err error
		if page, err = strconv.Atoi(*in.Marker); err != nil {
			return nil, fmt.Errorf("fake: bad marker %q: %w", *in.Marker, err)
		}
	}
	out := &lambda.GetDurableExecutionStateOutput{Operations: f.statePages[page]}
	if next := page + 1; next < len(f.statePages) {
		marker := strconv.Itoa(next)
		out.NextMarker = &marker
	}
	return out, nil
}

func (f *fakeLambda) CheckpointDurableExecution(_ context.Context, in *lambda.CheckpointDurableExecutionInput, _ ...func(*lambda.Options)) (*lambda.CheckpointDurableExecutionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotTokens = append(f.gotTokens, aws.ToString(in.CheckpointToken))
	f.gotUpdateBatches = append(f.gotUpdateBatches, in.Updates)
	if f.checkpointErr != nil {
		return nil, f.checkpointErr
	}
	if f.rotateTokens {
		f.nextToken = "token-" + strconv.Itoa(len(f.gotTokens))
	}
	token := f.nextToken
	return &lambda.CheckpointDurableExecutionOutput{CheckpointToken: &token}, nil
}

func opWire(id string, status types.OperationStatus) types.Operation {
	return types.Operation{Id: aws.String(id), Status: status}
}

func TestLoadStateFollowsPagination(t *testing.T) {
	fake := &fakeLambda{
		statePages: [][]types.Operation{
			{opWire(hashID("1"), types.OperationStatusSucceeded)},
			{opWire(hashID("2"), types.OperationStatusFailed)},
			{opWire(hashID("2-1"), types.OperationStatusStarted)},
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

// nilTokenLambda wraps fakeLambda and strips the checkpoint token from
// successful responses.
type nilTokenLambda struct {
	*fakeLambda
}

func (n *nilTokenLambda) CheckpointDurableExecution(ctx context.Context, in *lambda.CheckpointDurableExecutionInput, opts ...func(*lambda.Options)) (*lambda.CheckpointDurableExecutionOutput, error) {
	out, err := n.fakeLambda.CheckpointDurableExecution(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	out.CheckpointToken = nil
	return out, nil
}

func TestLoadStateEmptyExecution(t *testing.T) {
	fake := &fakeLambda{statePages: [][]types.Operation{{}}}
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
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cp.checkpoint(context.Background(), nil); err != nil {
				t.Errorf("concurrent checkpoint(): %v", err)
			}
		}()
	}
	wg.Wait()

	if got := len(fake.gotTokens); got != workers {
		t.Fatalf("backend received %d checkpoint calls, want %d", got, workers)
	}
	// Serialization invariant: every token the backend issued is used by
	// exactly one subsequent call, so the sent tokens are all distinct.
	seen := make(map[string]bool, workers)
	for _, tok := range fake.gotTokens {
		if seen[tok] {
			t.Fatalf("token %q sent to the backend more than once", tok)
		}
		seen[tok] = true
	}
	if got, want := cp.currentToken(), "token-"+strconv.Itoa(workers); got != want {
		t.Errorf("currentToken() after %d rotations = %q, want %q", workers, got, want)
	}
}
