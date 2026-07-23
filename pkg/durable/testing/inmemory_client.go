package testing

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// inMemoryClient is a checkpoint.Client implementation backed by an
// in-memory map, applying each OperationUpdate exactly as the real
// backend would (per the field-by-field verification recorded in
// docs/checkpoint-replay-design.md and pkg/durable/types/wire.go). It is
// the checkpoint.Client LocalTestRunner injects into
// durable.WithDurableExecution, and is the direct generalization of the
// test-only fakeClient previously duplicated in durable_fake_client_test.go
// (see docs/remaining-work.md task 17a).
//
// A single inMemoryClient is shared across every execution a
// LocalTestRunner drives (one per Run/RunAsync call), so operations are
// keyed by (execution ARN, operation ID) rather than by operation ID
// alone — otherwise two independent executions started from the same
// runner would collide on step ID "1", "2", etc., since step IDs are
// simple per-context counters (see context.Context.NextStepID) with no
// execution-scoping built into the ID string itself.
//
// Unlike a real backend, WAIT operations complete synchronously once
// checkpointed rather than waiting for a real timer to elapse (matching
// the officially documented local-runner behavior: "context.wait() delays
// ... complete in zero wall-clock time" - see
// docs.aws.amazon.com/durable-execution/testing/api-reference/, "Control
// time"). This is the local runner's only intentional divergence from
// production timing behavior; see LocalTestRunnerConfig.SkipTime.
type inMemoryClient struct {
	mu       sync.Mutex
	byExecID map[string]map[string]types.Operation // execution ARN -> operation ID -> Operation
	token    int

	// skipTime controls whether WAIT operations and step retry delays
	// resolve immediately (true, the default - matches the documented
	// "skipTime" behavior of the reference SDKs' local runners) or are
	// left PENDING for the caller to resolve via AdvanceTime (false).
	skipTime bool

	// mockFunctions maps a function name/ARN (as passed to
	// operations.Invoke) to a mock implementation registered via
	// LocalTestRunner.RegisterFunction, matching the officially
	// documented cross-SDK pattern of "register mock handlers for
	// invoke" (docs.aws.amazon.com/durable-execution/testing/api-reference/,
	// "Register mock handlers for invoke") rather than a manual
	// complete-the-invoke API - CHAINED_INVOKE operations are resolved
	// SYNCHRONOUSLY against the registered mock as part of processing
	// the START checkpoint itself, since (unlike CALLBACK) there is no
	// confirmed real-backend notion of a human/external system
	// completing an invoke out-of-band - the backend calls the target
	// function directly.
	mockFunctions map[string]func(payload string) (string, error)
}

func newInMemoryClient(skipTime bool) *inMemoryClient {
	return &inMemoryClient{
		byExecID:      make(map[string]map[string]types.Operation),
		skipTime:      skipTime,
		mockFunctions: make(map[string]func(payload string) (string, error)),
	}
}

// registerFunction records a mock implementation for functionName, used
// to resolve operations.Invoke calls targeting it. See mockFunctions'
// doc.
func (c *inMemoryClient) registerFunction(functionName string, fn func(payload string) (string, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mockFunctions[functionName] = fn
}

// seedRootExecutionOperation records op (expected to be the root
// EXECUTION operation) directly into this client's state for arn,
// bypassing the OperationUpdate/Checkpoint path since the root operation
// is never checkpointed by durable.WithDurableExecution itself - see
// newInvocationInput's doc for why this needs to exist at all.
func (c *inMemoryClient) seedRootExecutionOperation(arn string, op types.Operation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ops := c.byExecID[arn]
	if ops == nil {
		ops = make(map[string]types.Operation)
		c.byExecID[arn] = ops
	}
	ops[op.ID] = copyOperation(op)
}

func (c *inMemoryClient) Checkpoint(_ context.Context, req types.CheckpointDurableExecutionRequest) (*types.CheckpointDurableExecutionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Real-backend constraint, confirmed via a genuine live deployment
	// failure (examples/map-with-condition-and-callback-go invoked
	// against a real Lambda function, account 730758745077, us-east-1;
	// see wait_for_condition.go's package-level bug writeup for the full
	// investigation): a RETRY action's OperationUpdate must not carry
	// BOTH Error and Payload. The real backend rejects this outright
	// with "InvalidParameterValueException: Cannot provide both an Error
	// and Payload for RETRY action." This in-memory client previously
	// applied any combination of fields unconditionally - the exact gap
	// that let this bug ship to a real deployment without any local test
	// (LocalTestRunner-based or otherwise) catching it first. Checked up
	// front, against every update in the batch, before any of them are
	// applied - a real backend validates and rejects the WHOLE checkpoint
	// request atomically, it does not partially apply a batch and then
	// fail partway through.
	for _, u := range req.Updates {
		if u.Action == types.OperationActionRetry && u.Error != nil && u.Payload != nil {
			return nil, fmt.Errorf("checkpoint API error (NON_RETRYABLE): InvalidParameterValueException: Cannot provide both an Error and Payload for RETRY action (operation id %s)", u.ID)
		}
	}

	ops := c.byExecID[req.DurableExecutionArn]
	if ops == nil {
		ops = make(map[string]types.Operation)
		c.byExecID[req.DurableExecutionArn] = ops
	}

	var updated []types.Operation
	for _, u := range req.Updates {
		op := ops[u.ID]
		op.ID = u.ID
		op.ParentID = u.ParentID
		op.Name = u.Name
		op.Type = u.Type
		if u.SubType != "" {
			op.SubType = u.SubType
		}

		switch u.Action {
		case types.OperationActionStart:
			op.Status = types.OperationStatusStarted
			if u.Type == types.OperationTypeChainedInvoke {
				op.ChainedInvokeDetails = ensureChainedInvokeDetails(op.ChainedInvokeDetails)
				op = c.resolveChainedInvoke(op, u)
			}
		case types.OperationActionSucceed:
			op.Status = types.OperationStatusSucceeded
			if u.Type == types.OperationTypeStep {
				op.StepDetails = ensureStepDetails(op.StepDetails)
				op.StepDetails.Result = u.Payload
			}
			if u.Type == types.OperationTypeCallback {
				op.CallbackDetails = ensureCallbackDetails(op.CallbackDetails)
				op.CallbackDetails.Result = u.Payload
			}
			if u.Type == types.OperationTypeChainedInvoke {
				op.ChainedInvokeDetails = ensureChainedInvokeDetails(op.ChainedInvokeDetails)
				op.ChainedInvokeDetails.Result = u.Payload
			}
			if u.Type == types.OperationTypeContext {
				op.ContextDetails = ensureContextDetails(op.ContextDetails)
				op.ContextDetails.Result = u.Payload
			}
		case types.OperationActionFail:
			op.Status = types.OperationStatusFailed
			if u.Type == types.OperationTypeStep {
				op.StepDetails = ensureStepDetails(op.StepDetails)
				op.StepDetails.Error = u.Error
			}
			if u.Type == types.OperationTypeCallback {
				op.CallbackDetails = ensureCallbackDetails(op.CallbackDetails)
				op.CallbackDetails.Error = u.Error
			}
			if u.Type == types.OperationTypeChainedInvoke {
				op.ChainedInvokeDetails = ensureChainedInvokeDetails(op.ChainedInvokeDetails)
				op.ChainedInvokeDetails.Error = u.Error
			}
			if u.Type == types.OperationTypeContext {
				op.ContextDetails = ensureContextDetails(op.ContextDetails)
				op.ContextDetails.Error = u.Error
			}
		case types.OperationActionRetry:
			op.Status = types.OperationStatusPending
			if u.Type == types.OperationTypeStep {
				op.StepDetails = ensureStepDetails(op.StepDetails)
				op.StepDetails.Attempt++
				op.StepDetails.Error = u.Error
				// See durable_fake_client_test.go's fakeClient
				// OperationActionRetry case for why this must persist
				// Payload too: WaitForCondition's poll-loop retry
				// checkpoints carry the latest state, and
				// runWaitForCondition's OperationStatusPending branch
				// reads it back via deserializeConditionState on the
				// next invocation.
				if u.Payload != nil {
					op.StepDetails.Result = u.Payload
				}
			}
		case types.OperationActionCancel:
			op.Status = types.OperationStatusCancelled
		}

		if u.Type == types.OperationTypeCallback && u.Action == types.OperationActionStart {
			op.CallbackDetails = ensureCallbackDetails(op.CallbackDetails)
			if op.CallbackDetails.CallbackID == "" {
				op.CallbackDetails.CallbackID = fmt.Sprintf("local-callback-%s-%s", req.DurableExecutionArn, u.ID)
			}
		}

		if c.skipTime && u.Type == types.OperationTypeWait && u.Action == types.OperationActionStart {
			// Matches the documented local-runner "skipTime" behavior:
			// wait delays complete in zero wall-clock time.
			op.Status = types.OperationStatusSucceeded
		}
		if c.skipTime && u.Type == types.OperationTypeStep && u.Action == types.OperationActionRetry {
			// Matches the documented local-runner "skipTime" behavior for
			// step/WaitForCondition retry delays: retries become
			// immediately eligible rather than waiting out
			// NextAttemptDelaySeconds.
			//
			// READY, not PENDING, per a real, confirmed live-deployment
			// finding (see pkg/durable/operations/wait_for_condition.go's
			// package-level bug writeup): the real backend transitions a
			// RETRY-checkpointed operation to READY (types.OperationStatusReady,
			// a distinct eighth OperationStatus value - see wire.go's own
			// doc on how it was discovered) once the delay elapses and it
			// re-invokes, NOT to PENDING again - PENDING is only the
			// state immediately after the RETRY checkpoint itself, before
			// the delay has elapsed. This in-memory client previously
			// used PENDING here unconditionally, which is why the real
			// bug this comment references - runWaitForCondition's replay
			// switch having no case for READY at all, silently resetting
			// poll state to the caller's initialState on every
			// re-invocation - passed every LocalTestRunner-driven test in
			// this repo (including wait-for-condition-go's and this
			// example's own dedicated tests) yet failed the very first
			// time it was actually deployed and re-invoked by the real
			// backend. Modeling READY correctly here is what closes that
			// gap: any future operation that forgets to handle READY in
			// its own replay-lookup switch will now be caught locally,
			// under `go test`, instead of only in production.
			op.Status = types.OperationStatusReady
		}

		// Store an independent copy in this client's persistent map (see
		// copyOperation's doc for why this matters), and hand the caller
		// its OWN independent copy too - op itself may still be mutated
		// by this loop's later iterations (unlikely for the same ID
		// within one batch, but not guaranteed impossible) or read
		// concurrently by another invocation's goroutine before this
		// function returns.
		ops[u.ID] = copyOperation(op)
		updated = append(updated, copyOperation(op))
	}

	c.token++
	nextToken := fmt.Sprintf("token-%d", c.token)
	return &types.CheckpointDurableExecutionResponse{
		NextCheckpointToken: &nextToken,
		UpdatedOperations:   updated,
	}, nil
}

// snapshotForExecution returns the recorded operations for the given
// execution ARN, as a slice suitable for
// InitialExecutionState.Operations, plus whether any state has been
// recorded for that ARN yet.
//
// Returns DEEP copies (see copyOperation) - critical because each
// invocation constructs its own execmgr.Manager and checkpoint.Manager
// from these operations (see durable.WithDurableExecution and
// LocalTestRunner.driveToCompletion's re-invocation loop), and prior
// invocations' handler goroutines may still be running when a later
// invocation starts (an ABANDONED goroutine from a suspended invocation
// is never forcibly stopped - see execmgr's package doc). Without a deep
// copy here, two invocations' Operation values would share the same
// underlying *StepDetails/*CallbackDetails/etc. pointers, and a still-
// running abandoned goroutine mutating its copy (e.g. Step's
// executeAndCheckpoint writing StepDetails.Result) would race with a
// later invocation reading its own, supposedly-independent copy of what
// should be the same LOGICAL operation but a DIFFERENT struct instance.
// This was a real bug, caught by go test -race: see the commit history
// for the exact race trace this fixed.
func (c *inMemoryClient) snapshotForExecution(arn string) ([]types.Operation, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ops, ok := c.byExecID[arn]
	if !ok {
		return nil, false
	}
	out := make([]types.Operation, 0, len(ops))
	for _, op := range ops {
		out = append(out, copyOperation(op))
	}
	return out, true
}

// snapshot returns a copy of every operation recorded across ALL
// executions this client has ever checkpointed, keyed by operation ID.
// Used by TestResult/LocalTestRunner accessors that operate on a single
// execution's results — safe in practice because TestResult is
// constructed immediately after that execution's own invocation
// completes and only that execution's IDs are looked up by name
// afterward, but see snapshotForExecution for the ARN-scoped variant used
// internally to avoid cross-execution ID collisions during replay.
//
// Returns DEEP copies - see snapshotForExecution's doc for why this
// matters (an abandoned goroutine from a still-suspended invocation may
// mutate its copy of an operation's *StepDetails/etc. concurrently with
// a caller reading this snapshot).
func (c *inMemoryClient) snapshot() map[string]types.Operation {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]types.Operation)
	for _, ops := range c.byExecID {
		for id, op := range ops {
			out[id] = copyOperation(op)
		}
	}
	return out
}

// snapshotSingleExecution is like snapshot but scoped to one execution
// ARN, avoiding the cross-execution merge snapshot performs. Used by
// LocalTestRunner.toTestResult so a TestResult's operation list reflects
// only the execution it was produced from.
//
// Returns DEEP copies - see snapshotForExecution's doc.
func (c *inMemoryClient) snapshotSingleExecution(arn string) map[string]types.Operation {
	c.mu.Lock()
	defer c.mu.Unlock()
	ops := c.byExecID[arn]
	out := make(map[string]types.Operation, len(ops))
	for id, op := range ops {
		out[id] = copyOperation(op)
	}
	return out
}

// completeCallback resolves a pending callback operation identified by
// its checkpointed callback ID, driving it to a terminal state exactly as
// SendDurableExecutionCallbackSuccess/Failure would on a real backend.
// Used by TestOperation.SendCallbackSuccess/Failure (see operation.go).
// Searches across all executions since callback IDs are already globally
// unique (see the ID's construction in Checkpoint, above).
//
// Mutates its OWN copy of CallbackDetails (via copyOperation) before
// storing it back, rather than mutating the stored operation's
// CallbackDetails pointer in place - the in-place version was a second
// instance of the same shared-pointer race described in
// snapshotForExecution's doc: an abandoned goroutine from a suspended
// invocation could be concurrently reading through what it believes is
// its own independent copy of this same operation.
func (c *inMemoryClient) completeCallback(callbackID string, result *string, opErr *types.ErrorObject) (types.Operation, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for arn, ops := range c.byExecID {
		for id, op := range ops {
			if op.Type != types.OperationTypeCallback || op.CallbackDetails == nil || op.CallbackDetails.CallbackID != callbackID {
				continue
			}
			updated := copyOperation(op)
			if opErr != nil {
				updated.Status = types.OperationStatusFailed
				updated.CallbackDetails.Error = opErr
			} else {
				updated.Status = types.OperationStatusSucceeded
				updated.CallbackDetails.Result = result
			}
			c.byExecID[arn][id] = updated
			return copyOperation(updated), true
		}
	}
	return types.Operation{}, false
}

// copyOperation returns a deep copy of op: a new Operation value with
// freshly allocated copies of every pointer field (StartTimestamp,
// EndTimestamp, and each per-type *Details struct, which themselves
// contain pointer fields like *ErrorObject and *string that also need
// copying). See snapshotForExecution's doc comment for why every hand-out
// point in this client uses this rather than a plain struct copy: Go
// struct assignment/range copies the Operation VALUE but NOT the memory
// its pointer fields point to, so two "independent" copies obtained from
// two different snapshot calls would still alias the same underlying
// StepDetails/CallbackDetails/etc. struct and race on concurrent
// mutation - a real bug this fixed, caught by go test -race.
func copyOperation(op types.Operation) types.Operation {
	out := op

	if op.StartTimestamp != nil {
		t := *op.StartTimestamp
		out.StartTimestamp = &t
	}
	if op.EndTimestamp != nil {
		t := *op.EndTimestamp
		out.EndTimestamp = &t
	}
	if op.ExecutionDetails != nil {
		d := *op.ExecutionDetails
		d.InputPayload = copyStringPtr(op.ExecutionDetails.InputPayload)
		out.ExecutionDetails = &d
	}
	if op.ContextDetails != nil {
		d := *op.ContextDetails
		d.Result = copyStringPtr(op.ContextDetails.Result)
		d.Error = copyErrorPtr(op.ContextDetails.Error)
		out.ContextDetails = &d
	}
	if op.StepDetails != nil {
		d := *op.StepDetails
		d.Result = copyStringPtr(op.StepDetails.Result)
		d.Error = copyErrorPtr(op.StepDetails.Error)
		if op.StepDetails.NextAttemptTimestamp != nil {
			t := *op.StepDetails.NextAttemptTimestamp
			d.NextAttemptTimestamp = &t
		}
		out.StepDetails = &d
	}
	if op.WaitDetails != nil {
		d := *op.WaitDetails
		if op.WaitDetails.ScheduledEndTimestamp != nil {
			t := *op.WaitDetails.ScheduledEndTimestamp
			d.ScheduledEndTimestamp = &t
		}
		out.WaitDetails = &d
	}
	if op.CallbackDetails != nil {
		d := *op.CallbackDetails
		d.Result = copyStringPtr(op.CallbackDetails.Result)
		d.Error = copyErrorPtr(op.CallbackDetails.Error)
		out.CallbackDetails = &d
	}
	if op.ChainedInvokeDetails != nil {
		d := *op.ChainedInvokeDetails
		d.Result = copyStringPtr(op.ChainedInvokeDetails.Result)
		d.Error = copyErrorPtr(op.ChainedInvokeDetails.Error)
		out.ChainedInvokeDetails = &d
	}

	return out
}

func copyStringPtr(s *string) *string {
	if s == nil {
		return nil
	}
	v := *s
	return &v
}

func copyErrorPtr(e *types.ErrorObject) *types.ErrorObject {
	if e == nil {
		return nil
	}
	v := *e
	if e.StackTrace != nil {
		v.StackTrace = append([]string(nil), e.StackTrace...)
	}
	return &v
}

// resolveChainedInvoke synchronously resolves a CHAINED_INVOKE operation
// against a registered mock function (see mockFunctions' doc), setting
// its terminal status and result/error, or leaves it STARTED with an
// error recorded in ChainedInvokeDetails.Error if no mock was registered
// for the target function name - matching Java's testing-runner
// convention of surfacing a clear "no mock registered" failure rather
// than hanging forever (there is no real backend here to eventually time
// it out).
//
// Must be called with c.mu already held (it's invoked from within
// Checkpoint's locked section).
func (c *inMemoryClient) resolveChainedInvoke(op types.Operation, u types.OperationUpdate) types.Operation {
	functionName := ""
	if u.ChainedInvokeOptions != nil {
		functionName = u.ChainedInvokeOptions.FunctionName
	}

	fn, ok := c.mockFunctions[functionName]
	if !ok {
		op.Status = types.OperationStatusFailed
		op.ChainedInvokeDetails.Error = &types.ErrorObject{
			ErrorMessage: fmt.Sprintf("testing.LocalTestRunner: no mock function registered for %q - call RegisterFunction before invoking it (see the official runner API's \"Register mock handlers for invoke\")", functionName),
			ErrorType:    "NoMockRegistered",
		}
		return op
	}

	var inputPayload string
	if u.Payload != nil {
		inputPayload = *u.Payload
	}

	result, err := fn(inputPayload)
	if err != nil {
		op.Status = types.OperationStatusFailed
		op.ChainedInvokeDetails.Error = &types.ErrorObject{ErrorMessage: err.Error()}
		return op
	}

	op.Status = types.OperationStatusSucceeded
	op.ChainedInvokeDetails.Result = &result
	return op
}

func ensureStepDetails(d *types.StepDetails) *types.StepDetails {
	if d == nil {
		return &types.StepDetails{}
	}
	return d
}

func ensureCallbackDetails(d *types.CallbackDetails) *types.CallbackDetails {
	if d == nil {
		return &types.CallbackDetails{}
	}
	return d
}

func ensureChainedInvokeDetails(d *types.ChainedInvokeDetails) *types.ChainedInvokeDetails {
	if d == nil {
		return &types.ChainedInvokeDetails{}
	}
	return d
}

func ensureContextDetails(d *types.ContextDetails) *types.ContextDetails {
	if d == nil {
		return &types.ContextDetails{}
	}
	return d
}
