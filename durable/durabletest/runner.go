// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/internal/wire"
)

// DefaultMaxInvocations is the maximum number of re-invocations that
// [LocalRunner.RunUntilComplete] performs before returning.
const DefaultMaxInvocations = 100

// RunnerOption configures a [LocalRunner].
type RunnerOption func(*runnerConfig)

type runnerConfig struct {
	maxInvocations int
}

// WithMaxInvocations overrides the invocation cap for
// [LocalRunner.RunUntilComplete]. The default is [DefaultMaxInvocations].
func WithMaxInvocations(n int) RunnerOption {
	return func(c *runnerConfig) { c.maxInvocations = n }
}

// LocalRunner executes a durable handler function locally using an
// in-memory execution client. Each [Run] call performs a single invocation
// cycle: the handler sees any previously checkpointed state plus the new
// input, checkpoints new operations, and returns a [TestResult].
//
// Use [RunUntilComplete] to loop automatically until the execution reaches
// a terminal status (SUCCEEDED or FAILED) or until it is blocked awaiting
// external resolution (callbacks, chained invokes).
//
// LocalRunner is safe for sequential use from a single test goroutine.
// It is NOT safe for concurrent use from multiple goroutines.
type LocalRunner[I, O any] struct {
	handler func(context.Context, []byte) ([]byte, error)
	client  *memoryClient
	cfg     runnerConfig
}

// NewLocalRunner creates a runner for the given durable handler function.
// The handler is wired to an in-memory execution client; no AWS
// credentials or network access is required.
//
// Additional [durable.HandlerOption] values (such as [durable.WithSerdes])
// can be passed to configure the handler exactly as it would be in
// production, minus the execution client.
func NewLocalRunner[I, O any](handler durable.Handler[I, O], opts ...durable.HandlerOption) *LocalRunner[I, O] {
	client := newMemoryClient()

	// Prepend the in-memory client option before user options so that a
	// user-supplied WithExecutionClient would win (unlikely but safe).
	allOpts := make([]durable.HandlerOption, 0, len(opts)+1)
	allOpts = append(allOpts, durable.WithExecutionClient(client))
	allOpts = append(allOpts, opts...)

	return &LocalRunner[I, O]{
		handler: durable.Wrap(handler, allOpts...),
		client:  client,
		cfg:     runnerConfig{maxInvocations: DefaultMaxInvocations},
	}
}

// Run performs a single durable invocation against the in-memory client.
// It constructs the invocation payload from the current checkpoint state,
// invokes the handler, and returns the result.
//
// On a fresh runner, Run starts a new execution. On subsequent calls, it
// re-invokes with the accumulated checkpoint state, simulating the Lambda
// re-invocation loop.
//
// Run calls t.Fatal on infrastructure errors (payload marshaling, handler
// invocation errors that indicate a bug rather than a user-handler
// failure). User-handler errors are reflected in [TestResult.Status] as
// [Failed], not as test failures.
func (r *LocalRunner[I, O]) Run(t *testing.T, event I) *TestResult {
	t.Helper()

	payload, err := r.buildPayload(event)
	if err != nil {
		t.Fatalf("durabletest: build invocation payload: %v", err)
	}

	response, err := r.handler(context.Background(), payload)
	if err != nil {
		t.Fatalf("durabletest: handler.Invoke returned error: %v", err)
	}

	ops := r.client.allOperations()
	result, err := testResultFromResponse(response, ops)
	if err != nil {
		t.Fatalf("durabletest: parse response: %v", err)
	}
	return result
}

// RunUntilComplete performs repeated invocations until the execution
// reaches a terminal status (SUCCEEDED or FAILED), is blocked awaiting
// external resolution, or the invocation cap is reached.
//
// Between invocations, RunUntilComplete automatically advances
// time-eligible operations (STEP in PENDING → READY for retry, WAIT in
// STARTED → SUCCEEDED). If no operations can be auto-advanced and the
// execution is still PENDING, the method returns the PENDING result
// without spinning — this indicates that external action (callback
// submission, chained-invoke completion) is required before progress can
// continue.
//
// The invocation cap defaults to [DefaultMaxInvocations] (100) and can be
// changed with [WithMaxInvocations]. If the cap is exhausted, the final
// result (typically PENDING) is returned with [TestResult.CapReached] set
// to true.
//
// Run calls t.Fatal on infrastructure errors. User-handler errors surface
// as a FAILED [TestResult].
func (r *LocalRunner[I, O]) RunUntilComplete(t *testing.T, event I, opts ...RunnerOption) *TestResult {
	t.Helper()

	cfg := r.cfg
	for _, o := range opts {
		o(&cfg)
	}

	var result *TestResult
	for i := range cfg.maxInvocations {
		result = r.Run(t, event)

		if result.Status != Pending {
			return result
		}

		// Complete what would complete on its own with the passage
		// of time (retry timers, wait expirations). If nothing
		// completed, the execution is
		// blocked on external resolution — return immediately.
		if !r.client.completePendingTimers() {
			return result
		}

		_ = i // iteration consumed
	}

	// Cap reached.
	if result != nil {
		result.CapReached = true
	}
	return result
}

// CompletePendingTimers completes every operation that is blocked on a
// timer, regardless of its configured duration. Exactly two kinds of
// operations are affected:
//
//   - A step awaiting a retry timer (PENDING) becomes ready for
//     re-execution (READY). This includes the delays between
//     [durable.WaitForCondition] checks.
//   - A wait whose duration has not yet elapsed (STARTED) completes
//     (SUCCEEDED).
//
// No other operations are touched: callbacks and chained invokes remain
// blocked until resolved explicitly. There is no virtual clock — timers do
// not fire selectively by duration.
//
// Returns true if any operation was completed.
func (r *LocalRunner[I, O]) CompletePendingTimers() bool {
	return r.client.completePendingTimers()
}

// SendCallbackSuccess completes a pending callback operation with a
// success result. The callbackID is the hashed operation ID visible via
// [TestOperation.CallbackDetails] or [OpenCallbacks].
//
// The payload is serialized to JSON and stored in
// CallbackDetails.Result, matching the wire behavior of the
// SendDurableExecutionCallbackSuccess API.
func (r *LocalRunner[I, O]) SendCallbackSuccess(callbackID string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("durabletest: marshal callback payload: %w", err)
	}
	return r.client.completeCallback(callbackID, operationResult{status: statusSucceeded, result: string(data)})
}

// SendCallbackFailure fails a pending callback operation with a typed
// error, matching the wire behavior of the
// SendDurableExecutionCallbackFailure API.
//
// The errorType and errorMessage map to ErrorObject.ErrorType and
// ErrorObject.ErrorMessage on the wire. The resulting [*durable.CallbackError]
// carries these values so callers in test code can inspect them with
// [errors.As].
func (r *LocalRunner[I, O]) SendCallbackFailure(callbackID, errorType, errorMessage string) error {
	return r.client.completeCallback(callbackID, operationResult{
		status:  statusFailed,
		errType: errorType,
		errMsg:  errorMessage,
	})
}

// SendCallbackHeartbeat extends the heartbeat timeout of a pending
// callback operation, matching the wire behavior of the
// SendDurableExecutionCallbackHeartbeat API.
//
// In the local testing runner, this is a no-op that validates the
// callback exists and is in STARTED status. Real heartbeat timeout
// mechanics are not simulated locally.
func (r *LocalRunner[I, O]) SendCallbackHeartbeat(callbackID string) error {
	return r.client.heartbeatCallback(callbackID)
}

// TimeoutCallback transitions a pending callback operation to TIMED_OUT,
// simulating the backend behavior when a callback's configured timeout
// elapses without external resolution.
//
// After calling TimeoutCallback, invoke [Run] or [RunUntilComplete] to
// allow the handler to observe the timeout and continue execution.
func (r *LocalRunner[I, O]) TimeoutCallback(callbackID string) error {
	return r.client.timeoutCallback(callbackID)
}

// OpenCallbacks returns the callback IDs of all CALLBACK operations
// currently in STARTED status (pending external resolution). The
// returned IDs can be passed to [SendCallbackSuccess],
// [SendCallbackFailure], or [SendCallbackHeartbeat].
func (r *LocalRunner[I, O]) OpenCallbacks() []OpenCallback {
	return r.client.openCallbacks()
}

// CompleteChainedInvoke resolves a pending chained-invoke operation with
// a success result. The name must match the operation name passed to
// [durable.Invoke]. The payload is serialized to JSON and stored in
// ChainedInvokeDetails.Result.
func (r *LocalRunner[I, O]) CompleteChainedInvoke(name string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("durabletest: marshal chained-invoke result: %w", err)
	}
	return r.client.completeChainedInvoke(name, operationResult{status: statusSucceeded, result: string(data)})
}

// FailChainedInvoke fails a pending chained-invoke operation with a typed
// error. The resulting [*durable.InvokeError] carries these values when the
// handler re-invokes and encounters the FAILED status.
func (r *LocalRunner[I, O]) FailChainedInvoke(name, errorType, errorMessage string) error {
	return r.client.completeChainedInvoke(name, operationResult{
		status:  statusFailed,
		errType: errorType,
		errMsg:  errorMessage,
	})
}

// buildPayload constructs the durable invocation input from the current
// in-memory state. The payload shape matches what the Lambda durable
// execution service delivers to a handler.
func (r *LocalRunner[I, O]) buildPayload(event I) ([]byte, error) {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal event: %w", err)
	}

	// Build the operations list for the initial state: starts with the
	// execution operation carrying the customer input, followed by all
	// checkpointed operations.
	allOps := r.client.allOperationsRaw()
	wireOps := make([]wire.Operation, 0, len(allOps)+1)

	// Execution operation always first.
	wireOps = append(wireOps, wire.Operation{
		Id:     "exec-op",
		Status: "STARTED",
		Type:   "EXECUTION",
		ExecutionDetails: &wire.ExecutionDetails{
			InputPayload: string(eventJSON),
		},
	})

	// Append all checkpointed operations.
	for _, op := range allOps {
		wireOps = append(wireOps, apiOperationToWire(op))
	}

	input := wire.InvocationInput{
		DurableExecutionArn: "arn:aws:lambda:local:local:durable-execution:test",
		CheckpointToken:     r.client.currentToken(),
		InitialExecutionState: wire.InitialExecutionState{
			Operations: wireOps,
		},
	}

	return json.Marshal(input)
}

// allOperationsRaw returns all stored operations (including execution type)
// in insertion order for building the invocation payload.
func (m *memoryClient) allOperationsRaw() []operationSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	ops := make([]operationSnapshot, 0, len(m.operations))
	for _, id := range m.opOrder {
		op, ok := m.operations[id]
		if !ok {
			continue
		}
		// Skip execution operations — we build those ourselves.
		if op.Type == durable.OperationTypeExecution {
			continue
		}
		ops = append(ops, operationToSnapshot(*op))
	}
	return ops
}

// operationSnapshot is a denormalized view of an operation for payload
// construction.
type operationSnapshot struct {
	ID                   string
	Status               string
	Type                 string
	SubType              string
	Name                 string
	ParentId             string
	StepDetails          *wire.StepDetails
	CallbackDetails      *wire.CallbackDetails
	ChainedInvokeDetails *wire.ChainedInvokeDetails
	ContextDetails       *wire.ContextDetails
}

func operationToSnapshot(op durable.Operation) operationSnapshot {
	s := operationSnapshot{
		ID:       ptrStr(op.Id),
		Status:   string(op.Status),
		Type:     string(op.Type),
		SubType:  ptrStr(op.SubType),
		Name:     ptrStr(op.Name),
		ParentId: ptrStr(op.ParentId),
	}
	if sd := op.StepDetails; sd != nil {
		details := &wire.StepDetails{
			Attempt: int(sd.Attempt),
			Result:  ptrStr(sd.Result),
		}
		details.Error = wireErrorObject(sd.Error)
		s.StepDetails = details
	}
	if cd := op.CallbackDetails; cd != nil {
		s.CallbackDetails = &wire.CallbackDetails{
			CallbackId: ptrStr(cd.CallbackId),
			Result:     ptrStr(cd.Result),
		}
		s.CallbackDetails.Error = wireErrorObject(cd.Error)
	}
	if id := op.ChainedInvokeDetails; id != nil {
		s.ChainedInvokeDetails = &wire.ChainedInvokeDetails{
			Result: ptrStr(id.Result),
		}
		s.ChainedInvokeDetails.Error = wireErrorObject(id.Error)
	}
	if cd := op.ContextDetails; cd != nil {
		s.ContextDetails = &wire.ContextDetails{
			Result: ptrStr(cd.Result),
		}
		if cd.ReplayChildren != nil && *cd.ReplayChildren {
			s.ContextDetails.ReplayChildren = true
		}
		s.ContextDetails.Error = wireErrorObject(cd.Error)
	}
	return s
}

func apiOperationToWire(s operationSnapshot) wire.Operation {
	return wire.Operation{
		Id:                   s.ID,
		Status:               s.Status,
		Type:                 s.Type,
		SubType:              s.SubType,
		Name:                 s.Name,
		ParentId:             s.ParentId,
		StepDetails:          s.StepDetails,
		CallbackDetails:      s.CallbackDetails,
		ChainedInvokeDetails: s.ChainedInvokeDetails,
		ContextDetails:       s.ContextDetails,
	}
}

func ptrStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// wireErrorObject converts a stored error record to the invocation payload
// shape, carrying every field the service would return: type, message,
// data, and the stack trace frames.
func wireErrorObject(e *durable.ErrorObject) *wire.ErrorObject {
	if e == nil {
		return nil
	}
	var trace []string
	if len(e.StackTrace) > 0 {
		trace = make([]string, len(e.StackTrace))
		copy(trace, e.StackTrace)
	}
	return &wire.ErrorObject{
		ErrorType:    ptrStr(e.ErrorType),
		ErrorMessage: ptrStr(e.ErrorMessage),
		ErrorData:    ptrStr(e.ErrorData),
		StackTrace:   trace,
	}
}
