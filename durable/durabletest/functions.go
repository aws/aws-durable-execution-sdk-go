// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/internal/wire"
)

// MaxInvokeDepth bounds how deeply registered functions may invoke one
// another. The handler passed to [NewLocalRunner] runs at depth 0. A
// registered function it invokes runs at depth 1, a registered function
// that one invokes runs at depth 2, and so on. An invoke that would start a
// registered function beyond MaxInvokeDepth does not run it: the invoke
// fails, and the caller receives a [*durable.InvokeError] whose ErrorType
// is "InvokeDepthExceeded". A registered function that invokes back into
// its caller's function identifier therefore stops after MaxInvokeDepth
// levels instead of recursing without limit.
const MaxInvokeDepth = 10

// errTypeInvokeDepthExceeded is the ErrorType recorded on an invoke that
// would exceed [MaxInvokeDepth].
const errTypeInvokeDepthExceeded = "InvokeDepthExceeded"

// Function is a handler registered with [LocalRunner.RegisterFunction] as
// the target of a chained invoke. Construct one with [DurableFunction] or
// [PlainFunction]; the zero value is not usable.
type Function struct {
	// durable builds the wrapped durable handler over the client of the
	// execution that will run it. Set for durable targets.
	durable func(client durable.ExecutionClient) func(context.Context, []byte) ([]byte, error)

	// plain runs a non-durable handler once on the serialized input and
	// returns the serialized result. Set for non-durable targets.
	plain func(context.Context, []byte) ([]byte, error)
}

// DurableFunction makes a durable handler registrable as an invoke target.
// When the workflow under test invokes the identifier the Function is
// registered under, the runner starts a separate local execution for
// handler, with its own in-memory client and its own checkpoint log, and
// feeds the invoke's serialized input to it as the execution input.
//
// The opts configure the target handler the same way the opts of
// [NewLocalRunner] configure the handler under test.
func DurableFunction[I, O any](handler durable.Handler[I, O], opts ...durable.HandlerOption) Function {
	if handler == nil {
		panic("durabletest: DurableFunction: handler must not be nil")
	}
	return Function{
		durable: func(client durable.ExecutionClient) func(context.Context, []byte) ([]byte, error) {
			allOpts := make([]durable.HandlerOption, 0, len(opts)+1)
			allOpts = append(allOpts, durable.WithExecutionClient(client))
			allOpts = append(allOpts, opts...)
			return durable.Wrap(handler, allOpts...)
		},
	}
}

// PlainFunction makes a non-durable handler registrable as an invoke
// target. The handler has the shape of an ordinary typed Lambda handler.
// When the workflow under test invokes the identifier the Function is
// registered under, the runner decodes the invoke's input as JSON into I,
// calls handler once, and encodes the returned O as JSON for the invoke's
// result. A returned error fails the invoke; its ErrorType is the error's
// Go type name, or "Error" for an unnamed type such as one produced by
// errors.New or fmt.Errorf.
func PlainFunction[I, O any](handler func(context.Context, I) (O, error)) Function {
	if handler == nil {
		panic("durabletest: PlainFunction: handler must not be nil")
	}
	return Function{
		plain: func(ctx context.Context, payload []byte) ([]byte, error) {
			var in I
			if len(payload) > 0 {
				if err := json.Unmarshal(payload, &in); err != nil {
					return nil, fmt.Errorf("durabletest: decode invoke input: %w", err)
				}
			}
			out, err := handler(ctx, in)
			if err != nil {
				return nil, err
			}
			data, err := json.Marshal(out)
			if err != nil {
				return nil, fmt.Errorf("durabletest: encode invoke result: %w", err)
			}
			return data, nil
		},
	}
}

// functionRegistry maps function identifiers to registered targets. One
// registry is shared by a runner and every local execution it starts, so a
// registered target can itself invoke registered targets.
type functionRegistry struct {
	fns map[string]Function
}

func newFunctionRegistry() *functionRegistry {
	return &functionRegistry{fns: make(map[string]Function)}
}

func (fr *functionRegistry) lookup(functionID string) (Function, bool) {
	fn, ok := fr.fns[functionID]
	return fn, ok
}

// localExecution is one durable execution driven in-process: a wrapped
// handler, the in-memory client that stores its checkpoint log, and the
// registered functions its chained invokes may resolve to. The handler
// under test is the root execution; each registered durable target that a
// chained invoke starts is a child execution.
type localExecution struct {
	handler  func(context.Context, []byte) ([]byte, error)
	client   *memoryClient
	registry *functionRegistry
	arn      string
	depth    int

	// children holds the child executions started for registered durable
	// targets, keyed by the CHAINED_INVOKE operation ID in this execution.
	// A child that has not settled stays here and is driven again on the
	// next invocation of this execution.
	children map[string]*localExecution

	// invocations counts the handler invocations performed so far. It
	// numbers the request ID of each invocation.
	invocations int
}

func newLocalExecution(handler func(context.Context, []byte) ([]byte, error), client *memoryClient, registry *functionRegistry, arn string, depth int) *localExecution {
	return &localExecution{
		handler:  handler,
		client:   client,
		registry: registry,
		arn:      arn,
		depth:    depth,
		children: make(map[string]*localExecution),
	}
}

// invokeOutcome reports what one invocation of an execution did.
type invokeOutcome struct {
	// response is the handler's invocation response.
	response []byte

	// progressed is true when a registered target settled a chained
	// invoke during this invocation, so the execution can continue
	// without a timer or external action.
	progressed bool

	// capReached is true when a child execution exhausted the invocation
	// cap without settling.
	capReached bool
}

// invoke performs one invocation cycle: it builds the invocation payload
// from the checkpoint log, runs the handler, and then dispatches every
// open chained invoke whose target is a registered function.
//
// Around the handler run, invoke records the execution's lifecycle events:
// ExecutionStarted before the first invocation, InvocationCompleted after
// every invocation, and ExecutionSucceeded or ExecutionFailed after the
// invocation whose response ends the execution.
func (e *localExecution) invoke(eventJSON []byte, maxInvocations int) (invokeOutcome, error) {
	payload, err := e.buildPayload(eventJSON)
	if err != nil {
		return invokeOutcome{}, fmt.Errorf("build invocation payload: %w", err)
	}

	e.client.recordExecutionStarted(string(eventJSON))
	e.invocations++
	requestID := fmt.Sprintf("%s%d", localRequestIDPrefix, e.invocations)

	start := time.Now()
	response, err := e.handler(context.Background(), payload)
	end := time.Now()
	if err != nil {
		return invokeOutcome{}, fmt.Errorf("handler.Invoke returned error: %w", err)
	}

	resp, err := parseResponse(response)
	if err != nil {
		return invokeOutcome{}, err
	}
	respErr := responseError(resp)
	e.client.recordInvocationCompleted(requestID, start, end, respErr)
	e.client.recordExecutionEnded(resp.Status, resp.Result, respErr)

	progressed, capReached, err := e.dispatchInvokes(maxInvocations)
	if err != nil {
		return invokeOutcome{}, err
	}
	return invokeOutcome{response: response, progressed: progressed, capReached: capReached}, nil
}

// localRequestIDPrefix prefixes the request ID the local runner assigns to
// each invocation. The number that follows counts invocations of the
// execution from 1.
const localRequestIDPrefix = "local-request-"

// responseError returns the error an invocation response carries as an
// error record, or nil when the response carries none.
func responseError(resp wire.InvocationResponse) *durable.ErrorObject {
	if resp.Error == nil {
		return nil
	}
	e := &durable.ErrorObject{
		ErrorType:    strptr(resp.Error.ErrorType),
		ErrorMessage: strptr(resp.Error.ErrorMessage),
	}
	if resp.Error.ErrorData != "" {
		e.ErrorData = strptr(resp.Error.ErrorData)
	}
	if len(resp.Error.StackTrace) > 0 {
		e.StackTrace = append([]string(nil), resp.Error.StackTrace...)
	}
	return e
}

// driveUntilSettled repeats invoke until the execution reaches a terminal
// status, is blocked on external action, or has been invoked
// maxInvocations times. Between invocations it completes timer-blocked
// operations. It returns the last response and whether the cap was
// reached.
func (e *localExecution) driveUntilSettled(eventJSON []byte, maxInvocations int) ([]byte, bool, error) {
	var response []byte
	for range maxInvocations {
		outcome, err := e.invoke(eventJSON, maxInvocations)
		if err != nil {
			return nil, false, err
		}
		response = outcome.response

		resp, err := parseResponse(response)
		if err != nil {
			return nil, false, err
		}
		if resp.Status != wire.StatusPending {
			return response, false, nil
		}
		if outcome.capReached {
			return response, true, nil
		}

		// Complete what would complete on its own with the passage of
		// time (retry timers, wait expirations). If nothing completed and
		// no registered target settled an invoke, the execution is
		// blocked on external resolution: return immediately.
		timers := e.client.completePendingTimers()
		if !timers && !outcome.progressed {
			return response, false, nil
		}
	}
	return response, true, nil
}

// dispatchInvokes runs every open chained invoke whose target identifier
// is registered. It reports whether any of them settled and whether any
// child execution reached the invocation cap. Invokes of unregistered
// identifiers are left STARTED for [LocalRunner.CompleteChainedInvoke] or
// [LocalRunner.FailChainedInvoke].
func (e *localExecution) dispatchInvokes(maxInvocations int) (bool, bool, error) {
	settled := false
	capReached := false
	for _, inv := range e.client.openInvokes() {
		fn, ok := e.registry.lookup(inv.target.functionID)
		if !ok {
			continue
		}
		result, done, childCap, err := e.runTarget(inv, fn, maxInvocations)
		if err != nil {
			return false, false, fmt.Errorf("invoke %q of %q: %w", inv.name, inv.target.functionID, err)
		}
		if childCap {
			capReached = true
		}
		if !done {
			continue
		}
		if err := e.client.settleInvokeByID(inv.id, result); err != nil {
			return false, false, err
		}
		settled = true
	}
	return settled, capReached, nil
}

// runTarget runs a registered function for one open invoke. It returns the
// outcome to record, whether the target settled, and whether a durable
// target reached the invocation cap.
func (e *localExecution) runTarget(inv openInvoke, fn Function, maxInvocations int) (operationResult, bool, bool, error) {
	if e.depth+1 > MaxInvokeDepth {
		return operationResult{
			status:  statusFailed,
			errType: errTypeInvokeDepthExceeded,
			errMsg: fmt.Sprintf("durabletest: invoke of %q would exceed MaxInvokeDepth (%d) registered function levels",
				inv.target.functionID, MaxInvokeDepth),
		}, true, false, nil
	}

	if fn.plain != nil {
		data, err := fn.plain(context.Background(), []byte(inv.target.payload))
		if err != nil {
			return operationResult{status: statusFailed, errType: errorTypeName(err), errMsg: err.Error()}, true, false, nil
		}
		return operationResult{status: statusSucceeded, result: string(data)}, true, false, nil
	}

	child := e.children[inv.id]
	if child == nil {
		client := newMemoryClient()
		child = newLocalExecution(fn.durable(client), client, e.registry, e.arn+"/"+inv.id, e.depth+1)
		e.children[inv.id] = child
	}

	response, capReached, err := child.driveUntilSettled([]byte(inv.target.payload), maxInvocations)
	if err != nil {
		return operationResult{}, false, false, err
	}
	resp, err := parseResponse(response)
	if err != nil {
		return operationResult{}, false, false, err
	}

	switch resp.Status {
	case wire.StatusSucceeded:
		delete(e.children, inv.id)
		result := operationResult{status: statusSucceeded}
		if resp.Result != nil {
			result.result = *resp.Result
		}
		return result, true, false, nil
	case wire.StatusFailed:
		delete(e.children, inv.id)
		result := operationResult{status: statusFailed, errType: "Error", errMsg: "invoked function failed"}
		if resp.Error != nil {
			result.errType = resp.Error.ErrorType
			result.errMsg = resp.Error.ErrorMessage
			result.errData = resp.Error.ErrorData
			result.stackTrace = resp.Error.StackTrace
		}
		return result, true, false, nil
	default:
		// The target is blocked on external action or hit the cap. It
		// stays in children and is driven again on the next invocation.
		return operationResult{}, false, capReached, nil
	}
}

// openInvokeMatch locates one STARTED chained invoke: the execution whose
// checkpoint log holds it and its operation ID there.
type openInvokeMatch struct {
	exec *localExecution
	id   string
}

// findOpenInvokes collects every STARTED chained invoke named name in this
// execution and in the child executions it is running, depth-first.
// Children are visited in the insertion order of the invokes that started
// them, so the result order is deterministic.
func (e *localExecution) findOpenInvokes(name string) []openInvokeMatch {
	var matches []openInvokeMatch
	open := e.client.openInvokes()
	for _, inv := range open {
		if inv.name == name {
			matches = append(matches, openInvokeMatch{exec: e, id: inv.id})
		}
	}
	for _, inv := range open {
		if child := e.children[inv.id]; child != nil {
			matches = append(matches, child.findOpenInvokes(name)...)
		}
	}
	return matches
}

// completeChainedInvoke settles the STARTED chained invoke named name,
// wherever it is open: in this execution, or in a registered durable
// target that this execution is running, at any depth. Exactly one open
// invoke may carry the name; if several do, nothing is settled and the
// error names the executions involved. If none does, this execution's own
// client reports why, so the errors for a single execution are unchanged.
func (e *localExecution) completeChainedInvoke(name string, result operationResult) error {
	matches := e.findOpenInvokes(name)
	switch len(matches) {
	case 0:
		return e.client.completeChainedInvoke(name, result)
	case 1:
		return matches[0].exec.client.settleInvokeByID(matches[0].id, result)
	default:
		arns := make([]string, len(matches))
		for i, m := range matches {
			arns[i] = m.exec.arn
		}
		return fmt.Errorf("durabletest: chained-invoke %q is open in %d executions (%s): give the invokes distinct names",
			name, len(matches), strings.Join(arns, ", "))
	}
}

// buildPayload constructs the durable invocation input from the current
// checkpoint log. The payload shape matches what the Lambda durable
// execution service delivers to a handler.
func (e *localExecution) buildPayload(eventJSON []byte) ([]byte, error) {
	// Build the operations list for the initial state: starts with the
	// execution operation carrying the customer input, followed by all
	// checkpointed operations.
	allOps := e.client.allOperationsRaw()
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
		DurableExecutionArn: e.arn,
		CheckpointToken:     e.client.currentToken(),
		InitialExecutionState: wire.InitialExecutionState{
			Operations: wireOps,
		},
	}

	return json.Marshal(input)
}

// parseResponse decodes an invocation response.
func parseResponse(response []byte) (wire.InvocationResponse, error) {
	var resp wire.InvocationResponse
	if err := json.Unmarshal(response, &resp); err != nil {
		return wire.InvocationResponse{}, fmt.Errorf("parse invocation response: %w", err)
	}
	return resp, nil
}

// errorTypeName returns the type name recorded for an error returned by a
// non-durable target: the error's Go type name, or "Error" for the
// unnamed types behind errors.New, fmt.Errorf, and errors.Join.
func errorTypeName(err error) string {
	t := reflect.TypeOf(err)
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil {
		return "Error"
	}
	name := t.Name()
	if name == "" || isStdlibErrorType(t.PkgPath(), name) {
		return "Error"
	}
	return name
}

// isStdlibErrorType reports whether pkgPath and name identify one of the
// unexported types behind errors.New, errors.Join, and fmt.Errorf. The
// package path is checked so a user-defined type with the same name keeps
// its own name.
func isStdlibErrorType(pkgPath, name string) bool {
	switch pkgPath {
	case "errors":
		return name == "errorString" || name == "joinError"
	case "fmt":
		return name == "wrapError" || name == "wrapErrors"
	}
	return false
}
