package testing

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// executionRootID is used as the fixed root EXECUTION operation ID within
// this runner's synthetic per-execution ARNs. A real backend assigns this
// dynamically (see execmgr.Manager's doc on rootExecutionID), but nothing
// in the SDK depends on the root ID's specific value — only on it being
// present, stable across an execution's invocations, and of type
// EXECUTION — so a fixed sentinel is fine for local testing.
const executionRootID = "exec-root"

// newInvocationInput builds a types.DurableExecutionInvocationInput for
// arn, matching the real backend's confirmed shape (see
// pkg/durable/types/wire.go's doc comment and
// docs/checkpoint-replay-design.md): the root EXECUTION operation is
// always present in InitialExecutionState.Operations, carrying the
// JSON-encoded event as ExecutionDetails.InputPayload.
//
// If client is non-nil, this is a replay invocation: the root EXECUTION
// operation's InputPayload is taken from the client's already-recorded
// state (so the ORIGINAL event is preserved verbatim across invocations,
// matching what a real backend does — it does not re-derive the input
// payload from whatever event value a test happens to pass to a later
// Run call), and every other previously-checkpointed operation for this
// execution is included too, driving the SDK's replay-skip logic. If
// client is nil, this is a fresh execution's first invocation: event is
// marshaled and used directly.
func newInvocationInput[TEvent any](arn, checkpointToken string, event TEvent, client *inMemoryClient) (types.DurableExecutionInvocationInput, error) {
	if client != nil {
		if ops, ok := client.snapshotForExecution(arn); ok {
			return types.DurableExecutionInvocationInput{
				DurableExecutionArn:   arn,
				CheckpointToken:       checkpointToken,
				InitialExecutionState: types.InitialExecutionState{Operations: ops},
			}, nil
		}
		// No prior state recorded for this ARN yet — fall through and
		// treat this as this execution's first invocation, below.
	}

	b, err := json.Marshal(event)
	if err != nil {
		return types.DurableExecutionInvocationInput{}, fmt.Errorf("marshaling event: %w", err)
	}
	payload := string(b)

	rootOp := types.Operation{
		ID:     executionRootID,
		Type:   types.OperationTypeExecution,
		Status: types.OperationStatusStarted,
		ExecutionDetails: &types.ExecutionDetails{
			InputPayload: &payload,
		},
	}

	// Persist the root EXECUTION operation into the client itself (not
	// just returned in this invocation's input), so that subsequent
	// invocations of the SAME execution ARN - driven by
	// LocalTestRunner.Continue, or by driveToCompletion's own internal
	// re-invocation loop - see it in their own InitialExecutionState too.
	// This mirrors what a real backend does: the root EXECUTION
	// operation's record persists for the lifetime of the execution.
	// durable.WithDurableExecution itself never checkpoints this
	// operation back to the Client (see durable.go: it only calls
	// execManager.PutOperation, an in-memory-only call scoped to a
	// single invocation) - it relies entirely on the backend (here, this
	// client) to keep supplying it on every invocation, exactly as
	// confirmed from the real wire format (see
	// docs/checkpoint-replay-design.md: "the root EXECUTION operation is
	// always present... even first invocation").
	if client != nil {
		client.seedRootExecutionOperation(arn, rootOp)
	}

	return types.DurableExecutionInvocationInput{
		DurableExecutionArn:   arn,
		CheckpointToken:       checkpointToken,
		InitialExecutionState: types.InitialExecutionState{Operations: []types.Operation{rootOp}},
	}, nil
}
