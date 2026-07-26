package durable

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

const (
	// checkpointMaxAttempts is the maximum number of times a checkpoint
	// call is attempted on retryable errors before giving up.
	checkpointMaxAttempts = 3

	// checkpointBaseDelay is the initial delay before the first retry.
	checkpointBaseDelay = 100 * time.Millisecond

	// checkpointMaxDelay caps the exponential backoff.
	checkpointMaxDelay = 2 * time.Second
)

// ExecutionClient is the subset of the Lambda service client that the
// durable execution engine consumes. It is satisfied by
// [github.com/aws/aws-sdk-go-v2/service/lambda.Client] and by test fakes.
//
// The [durabletest] package implements an in-memory ExecutionClient for
// local testing without AWS infrastructure.
type ExecutionClient interface {
	GetDurableExecutionState(ctx context.Context, in *lambda.GetDurableExecutionStateInput, opts ...func(*lambda.Options)) (*lambda.GetDurableExecutionStateOutput, error)
	CheckpointDurableExecution(ctx context.Context, in *lambda.CheckpointDurableExecutionInput, opts ...func(*lambda.Options)) (*lambda.CheckpointDurableExecutionOutput, error)
}

// checkpointer persists operation updates for one durable execution and
// tracks the rotating checkpoint token. It is safe for concurrent use:
// operation bodies running on multiple goroutines checkpoint through one
// checkpointer.
type checkpointer struct {
	client       ExecutionClient
	executionArn string

	mu    sync.Mutex
	token string

	// state is the shared execution state. When non-nil, the checkpointer
	// merges backend-returned operations into it so that subsequent reads
	// (e.g. CallbackId lookup after checkpointing START) reflect
	// backend-assigned fields. Set during invocation wiring.
	state *executionState
}

func newCheckpointer(client ExecutionClient, executionArn, initialToken string) *checkpointer {
	return &checkpointer{
		client:       client,
		executionArn: executionArn,
		token:        initialToken,
	}
}

// loadState fetches the complete checkpointed operation log for the
// execution, following pagination until exhausted.
//
// loadState must complete before any concurrent checkpoint calls begin: it
// runs during invocation setup, before operation goroutines exist, and
// reads the token without coordinating with in-flight rotation.
func (cp *checkpointer) loadState(ctx context.Context) (*executionState, error) {
	ops, err := cp.loadStateFrom(ctx, "")
	if err != nil {
		return nil, err
	}
	return newExecutionState(ops), nil
}

// loadStateFrom fetches operation-log pages starting at marker ("" for the
// first page), following pagination until exhausted. The same setup-time
// constraint as loadState applies.
func (cp *checkpointer) loadStateFrom(ctx context.Context, marker string) ([]*operation, error) {
	var ops []*operation
	var next *string
	if marker != "" {
		next = &marker
	}
	for {
		out, err := cp.client.GetDurableExecutionState(ctx, &lambda.GetDurableExecutionStateInput{
			DurableExecutionArn: aws.String(cp.executionArn),
			CheckpointToken:     aws.String(cp.currentToken()),
			Marker:              next,
		})
		if err != nil {
			return nil, fmt.Errorf("durable: load execution state: %w", err)
		}
		for _, op := range out.Operations {
			ops = append(ops, operationFromAPI(op))
		}
		if out.NextMarker == nil || *out.NextMarker == "" {
			break
		}
		next = out.NextMarker
	}
	return ops, nil
}

// checkpoint applies updates atomically and rotates the checkpoint token.
// The returned operations are the updated state from the backend response,
// which may include backend-assigned fields (e.g. CallbackId).
//
// On retryable failures (server faults, throttling, network errors),
// checkpoint retries up to [checkpointMaxAttempts] with exponential
// backoff. Non-retryable failures (client faults other than throttling)
// fail immediately. On any failure the token remains unchanged.
func (cp *checkpointer) checkpoint(ctx context.Context, updates []types.OperationUpdate) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	var lastErr error
	for attempt := range checkpointMaxAttempts {
		out, err := cp.client.CheckpointDurableExecution(ctx, &lambda.CheckpointDurableExecutionInput{
			DurableExecutionArn: aws.String(cp.executionArn),
			CheckpointToken:     aws.String(cp.token),
			Updates:             updates,
		})
		if err != nil {
			classified := classifyCheckpointError(err)
			if !classified.Retryable() {
				return classified
			}
			lastErr = classified
			if attempt < checkpointMaxAttempts-1 {
				delay := checkpointBackoff(attempt)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
				}
			}
			continue
		}
		if out.CheckpointToken == nil {
			return errors.New("durable: checkpoint: backend returned no checkpoint token")
		}
		cp.token = *out.CheckpointToken

		// Merge updated operations into the execution state so that
		// subsequent reads (e.g. reading CallbackId after START) see
		// backend-assigned fields.
		if cp.state != nil && out.NewExecutionState != nil {
			ops := make([]*operation, 0, len(out.NewExecutionState.Operations))
			for _, apiOp := range out.NewExecutionState.Operations {
				ops = append(ops, operationFromAPI(apiOp))
			}
			cp.state.merge(ops)
		}
		return nil
	}
	return lastErr
}

// checkpointBackoff computes the delay for the given retry attempt using
// exponential backoff capped at [checkpointMaxDelay].
func checkpointBackoff(attempt int) time.Duration {
	delay := time.Duration(float64(checkpointBaseDelay) * math.Pow(2, float64(attempt)))
	if delay > checkpointMaxDelay {
		delay = checkpointMaxDelay
	}
	return delay
}

func (cp *checkpointer) currentToken() string {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.token
}

// operationFromAPI converts a wire operation into the engine's record.
func operationFromAPI(op types.Operation) *operation {
	rec := &operation{
		id:      aws.ToString(op.Id),
		status:  operationStatus(op.Status),
		opType:  string(op.Type),
		subType: aws.ToString(op.SubType),
		name:    aws.ToString(op.Name),
	}
	if sd := op.StepDetails; sd != nil {
		rec.step = &stepDetails{
			attempt: int(sd.Attempt),
			result:  aws.ToString(sd.Result),
		}
		if sd.Error != nil {
			rec.step.errType = aws.ToString(sd.Error.ErrorType)
			rec.step.errMessage = aws.ToString(sd.Error.ErrorMessage)
		}
	}
	if id := op.ChainedInvokeDetails; id != nil {
		rec.invoke = &invokeDetails{result: aws.ToString(id.Result)}
		if id.Error != nil {
			rec.invoke.errType = aws.ToString(id.Error.ErrorType)
			rec.invoke.errMessage = aws.ToString(id.Error.ErrorMessage)
			rec.invoke.errData = aws.ToString(id.Error.ErrorData)
		}
	}
	if cd := op.ContextDetails; cd != nil {
		rec.childCtx = &contextDetails{result: aws.ToString(cd.Result)}
		if cd.ReplayChildren != nil && *cd.ReplayChildren {
			rec.childCtx.replayChildren = true
		}
		if cd.Error != nil {
			rec.childCtx.errType = aws.ToString(cd.Error.ErrorType)
			rec.childCtx.errMessage = aws.ToString(cd.Error.ErrorMessage)
			rec.childCtx.errData = aws.ToString(cd.Error.ErrorData)
		}
	}
	if cb := op.CallbackDetails; cb != nil {
		rec.callback = &callbackDetails{
			callbackID: aws.ToString(cb.CallbackId),
			result:     aws.ToString(cb.Result),
		}
		if cb.Error != nil {
			rec.callback.errType = aws.ToString(cb.Error.ErrorType)
			rec.callback.errMessage = aws.ToString(cb.Error.ErrorMessage)
		}
	}
	return rec
}
