// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// DefaultMaxInvocations is the maximum number of re-invocations that
// [LocalRunner.RunUntilComplete] performs before returning. This matches
// the Java SDK's local test runner.
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
	handler lambda.Handler
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

	response, err := r.handler.Invoke(context.Background(), payload)
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

		// Auto-advance what the backend would advance (retry timers,
		// wait expirations). If nothing advanced, the execution is
		// blocked on external resolution — return immediately.
		if !r.client.advanceTime() {
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

// AdvanceTime transitions all time-eligible operations to their next state,
// simulating the passage of time without tracking a real clock:
//
//   - STEP in PENDING → READY (retry timer elapsed, ready for re-execution)
//   - WAIT in STARTED → SUCCEEDED (wait duration elapsed)
//
// The duration parameter is accepted for API-forward-compatibility but is
// currently ignored: all eligible operations advance regardless of their
// configured duration. This matches the Java SDK's advanceTime() which
// also ignores magnitude.
//
// Returns true if any operation was advanced.
func (r *LocalRunner[I, O]) AdvanceTime(_ time.Duration) bool {
	return r.client.advanceTime()
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
	wireOps := make([]invocationOperation, 0, len(allOps)+1)

	// Execution operation always first.
	wireOps = append(wireOps, invocationOperation{
		ID:     "exec-op",
		Status: "STARTED",
		Type:   "EXECUTION",
		ExecutionDetails: &executionDetails{
			InputPayload: string(eventJSON),
		},
	})

	// Append all checkpointed operations.
	for _, op := range allOps {
		wireOps = append(wireOps, apiOperationToWire(op))
	}

	input := invocationPayload{
		DurableExecutionArn: "arn:aws:lambda:us-east-1:123456789012:durable-execution:test",
		CheckpointToken:     r.client.currentToken(),
		InitialExecutionState: initialState{
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
		if op.Type == types.OperationTypeExecution {
			continue
		}
		ops = append(ops, operationToSnapshot(*op))
	}
	return ops
}

// Wire types for constructing the invocation payload. These mirror the
// durable package's invocationInput but are defined locally because the
// durable package's types are unexported.

type invocationPayload struct {
	DurableExecutionArn   string       `json:"DurableExecutionArn"`
	CheckpointToken       string       `json:"CheckpointToken"`
	InitialExecutionState initialState `json:"InitialExecutionState"`
}

type initialState struct {
	Operations []invocationOperation `json:"Operations"`
}

type invocationOperation struct {
	ID                   string               `json:"Id"`
	Status               string               `json:"Status"`
	Type                 string               `json:"Type,omitempty"`
	SubType              string               `json:"SubType,omitempty"`
	Name                 string               `json:"Name,omitempty"`
	ParentId             string               `json:"ParentId,omitempty"`
	ExecutionDetails     *executionDetails    `json:"ExecutionDetails,omitempty"`
	StepDetails          *stepDetailsWire     `json:"StepDetails,omitempty"`
	CallbackDetails      *callbackDetailsWire `json:"CallbackDetails,omitempty"`
	ChainedInvokeDetails *invokeDetailsWire   `json:"ChainedInvokeDetails,omitempty"`
	ContextDetails       *contextDetailsWire  `json:"ContextDetails,omitempty"`
	WaitDetails          *waitDetailsWire     `json:"WaitDetails,omitempty"`
}

type executionDetails struct {
	InputPayload string `json:"InputPayload,omitempty"`
}

type stepDetailsWire struct {
	Attempt int32      `json:"Attempt,omitempty"`
	Result  string     `json:"Result,omitempty"`
	Error   *errorWire `json:"Error,omitempty"`
}

type callbackDetailsWire struct {
	CallbackId string     `json:"CallbackId,omitempty"`
	Result     string     `json:"Result,omitempty"`
	Error      *errorWire `json:"Error,omitempty"`
}

type invokeDetailsWire struct {
	Result string     `json:"Result,omitempty"`
	Error  *errorWire `json:"Error,omitempty"`
}

type contextDetailsWire struct {
	Result         string     `json:"Result,omitempty"`
	ReplayChildren bool       `json:"ReplayChildren,omitempty"`
	Error          *errorWire `json:"Error,omitempty"`
}

type waitDetailsWire struct{}

type errorWire struct {
	ErrorType    string `json:"ErrorType,omitempty"`
	ErrorMessage string `json:"ErrorMessage,omitempty"`
	ErrorData    string `json:"ErrorData,omitempty"`
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
	StepDetails          *stepDetailsWire
	CallbackDetails      *callbackDetailsWire
	ChainedInvokeDetails *invokeDetailsWire
	ContextDetails       *contextDetailsWire
}

func operationToSnapshot(op types.Operation) operationSnapshot {
	s := operationSnapshot{
		ID:       ptrStr(op.Id),
		Status:   string(op.Status),
		Type:     string(op.Type),
		SubType:  ptrStr(op.SubType),
		Name:     ptrStr(op.Name),
		ParentId: ptrStr(op.ParentId),
	}
	if sd := op.StepDetails; sd != nil {
		wire := &stepDetailsWire{
			Attempt: sd.Attempt,
			Result:  ptrStr(sd.Result),
		}
		if sd.Error != nil {
			wire.Error = &errorWire{
				ErrorType:    ptrStr(sd.Error.ErrorType),
				ErrorMessage: ptrStr(sd.Error.ErrorMessage),
			}
		}
		s.StepDetails = wire
	}
	if cd := op.CallbackDetails; cd != nil {
		s.CallbackDetails = &callbackDetailsWire{
			CallbackId: ptrStr(cd.CallbackId),
			Result:     ptrStr(cd.Result),
		}
		if cd.Error != nil {
			s.CallbackDetails.Error = &errorWire{
				ErrorType:    ptrStr(cd.Error.ErrorType),
				ErrorMessage: ptrStr(cd.Error.ErrorMessage),
			}
		}
	}
	if id := op.ChainedInvokeDetails; id != nil {
		s.ChainedInvokeDetails = &invokeDetailsWire{
			Result: ptrStr(id.Result),
		}
		if id.Error != nil {
			s.ChainedInvokeDetails.Error = &errorWire{
				ErrorType:    ptrStr(id.Error.ErrorType),
				ErrorMessage: ptrStr(id.Error.ErrorMessage),
				ErrorData:    ptrStr(id.Error.ErrorData),
			}
		}
	}
	if cd := op.ContextDetails; cd != nil {
		s.ContextDetails = &contextDetailsWire{
			Result: ptrStr(cd.Result),
		}
		if cd.ReplayChildren != nil && *cd.ReplayChildren {
			s.ContextDetails.ReplayChildren = true
		}
		if cd.Error != nil {
			s.ContextDetails.Error = &errorWire{
				ErrorType:    ptrStr(cd.Error.ErrorType),
				ErrorMessage: ptrStr(cd.Error.ErrorMessage),
			}
		}
	}
	return s
}

func apiOperationToWire(s operationSnapshot) invocationOperation {
	return invocationOperation{
		ID:                   s.ID,
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
