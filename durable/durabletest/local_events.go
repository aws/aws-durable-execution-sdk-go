// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// localExecutionOperationID is the operation ID of the execution operation
// in a local execution. It matches the ID the invocation payload carries,
// so events about the execution and the payload's execution operation
// agree.
const localExecutionOperationID = "exec-op"

// recordEvent appends one history event to the client's event log. Event
// IDs increase by one per event, starting at 1. at is the event's
// timestamp: the wall-clock time of the transition the event describes,
// which is also the time stamped on the operation record (see
// stampTransition), so the event log and the operation timestamps agree.
// The caller fills in the event's type, operation identity, and details.
//
// Caller must hold m.mu.
func (m *memoryClient) recordEvent(ev types.Event, at time.Time) {
	m.eventSeq++
	ev.EventId = aws.Int32(m.eventSeq)
	ev.EventTimestamp = aws.Time(at)
	m.events = append(m.events, ev)
}

// allEvents returns a copy of the event log in recording order.
func (m *memoryClient) allEvents() []types.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]types.Event(nil), m.events...)
}

// recordExecutionStarted records the ExecutionStarted event for the
// execution's first invocation. It is a no-op after the first call.
func (m *memoryClient) recordExecutionStarted(input string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.executionStarted {
		return
	}
	m.executionStarted = true
	m.recordEvent(types.Event{
		EventType: types.EventTypeExecutionStarted,
		Id:        aws.String(localExecutionOperationID),
		ExecutionStartedDetails: &types.ExecutionStartedDetails{
			Input: &types.EventInput{Payload: aws.String(input)},
		},
	}, time.Now())
}

// recordInvocationCompleted records the InvocationCompleted event for one
// handler invocation. requestID identifies the invocation; err is the
// handler's error when the invocation ended the execution as FAILED.
func (m *memoryClient) recordInvocationCompleted(requestID string, start, end time.Time, err *durable.ErrorObject) {
	m.mu.Lock()
	defer m.mu.Unlock()
	details := &types.InvocationCompletedDetails{
		RequestId:      aws.String(requestID),
		StartTimestamp: aws.Time(start),
		EndTimestamp:   aws.Time(end),
	}
	if err != nil {
		details.Error = eventError(err)
	}
	m.recordEvent(types.Event{
		EventType:                  types.EventTypeInvocationCompleted,
		InvocationCompletedDetails: details,
	}, time.Now())
}

// recordExecutionEnded records the execution's terminal event from the
// invocation response: ExecutionSucceeded with the result, or
// ExecutionFailed with the error. A PENDING response records nothing.
//
// When the response carries no result or error but the handler
// checkpointed the outcome on the execution operation (see
// [memoryClient.checkpointedEnd]), the checkpointed result or error fills
// the event. The event is therefore recorded after the invocation's
// InvocationCompleted event, not at checkpoint time. It is a no-op once a
// terminal event has been recorded.
func (m *memoryClient) recordExecutionEnded(status string, result *string, err *durable.ErrorObject) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.executionEnded {
		return
	}
	cp := m.checkpointedEnd
	switch status {
	case statusSucceeded:
		m.executionEnded = true
		if (result == nil || *result == "") && cp != nil && cp.action == durable.OperationActionSucceed {
			result = cp.result
		}
		m.recordEvent(types.Event{
			EventType:                 types.EventTypeExecutionSucceeded,
			Id:                        aws.String(localExecutionOperationID),
			ExecutionSucceededDetails: &types.ExecutionSucceededDetails{Result: eventResult(result)},
		}, time.Now())
	case statusFailed:
		m.executionEnded = true
		if err == nil && cp != nil && cp.action == durable.OperationActionFail {
			err = cp.err
		}
		m.recordEvent(types.Event{
			EventType:              types.EventTypeExecutionFailed,
			Id:                     aws.String(localExecutionOperationID),
			ExecutionFailedDetails: &types.ExecutionFailedDetails{Error: eventError(err)},
		}, time.Now())
	}
}

// recordUpdateEvent records the history event that one checkpoint update
// produces, given the operation record the update resulted in and the
// time at of the transition. Updates with no corresponding event type
// record nothing.
//
// Caller must hold m.mu.
func (m *memoryClient) recordUpdateEvent(u durable.OperationUpdate, op durable.Operation, at time.Time) {
	ev := types.Event{
		Id:       u.Id,
		Name:     u.Name,
		ParentId: u.ParentId,
		SubType:  u.SubType,
	}

	switch u.Type {
	case durable.OperationTypeExecution:
		// A checkpoint on the execution operation carries the outcome of
		// a result too large to return inline. Its terminal event is
		// recorded from the invocation response instead, after that
		// invocation's InvocationCompleted event; see recordExecutionEnded.
		switch u.Action {
		case durable.OperationActionSucceed, durable.OperationActionFail:
			m.checkpointedEnd = &checkpointedExecutionEnd{action: u.Action, result: u.Payload, err: u.Error}
		}
		return

	case durable.OperationTypeStep:
		retry := &types.RetryDetails{}
		if op.StepDetails != nil {
			retry.CurrentAttempt = op.StepDetails.Attempt
		}
		if u.StepOptions != nil {
			retry.NextAttemptDelaySeconds = u.StepOptions.NextAttemptDelaySeconds
		}
		switch u.Action {
		case durable.OperationActionStart:
			ev.EventType = types.EventTypeStepStarted
			ev.StepStartedDetails = &types.StepStartedDetails{}
		case durable.OperationActionSucceed:
			ev.EventType = types.EventTypeStepSucceeded
			ev.StepSucceededDetails = &types.StepSucceededDetails{Result: eventResult(u.Payload), RetryDetails: retry}
		case durable.OperationActionFail:
			ev.EventType = types.EventTypeStepFailed
			ev.StepFailedDetails = &types.StepFailedDetails{Error: eventError(u.Error), RetryDetails: retry}
		case durable.OperationActionRetry:
			// A retry that carries an error is a failed attempt awaiting
			// its retry timer. A retry without an error carries the
			// intermediate state of a polling step, which the event log
			// records as that attempt's result.
			if u.Error != nil {
				ev.EventType = types.EventTypeStepFailed
				ev.StepFailedDetails = &types.StepFailedDetails{Error: eventError(u.Error), RetryDetails: retry}
			} else {
				ev.EventType = types.EventTypeStepSucceeded
				ev.StepSucceededDetails = &types.StepSucceededDetails{Result: eventResult(u.Payload), RetryDetails: retry}
			}
		default:
			return
		}

	case durable.OperationTypeWait:
		if u.Action != durable.OperationActionStart {
			return
		}
		ev.EventType = types.EventTypeWaitStarted
		details := &types.WaitStartedDetails{}
		if u.WaitOptions != nil && u.WaitOptions.WaitSeconds != nil {
			details.Duration = u.WaitOptions.WaitSeconds
			details.ScheduledEndTimestamp = aws.Time(at.Add(time.Duration(*u.WaitOptions.WaitSeconds) * time.Second))
		}
		ev.WaitStartedDetails = details

	case durable.OperationTypeCallback:
		if u.Action != durable.OperationActionStart {
			return
		}
		ev.EventType = types.EventTypeCallbackStarted
		details := &types.CallbackStartedDetails{}
		if op.CallbackDetails != nil {
			details.CallbackId = op.CallbackDetails.CallbackId
		}
		if u.CallbackOptions != nil {
			details.Timeout = aws.Int32(u.CallbackOptions.TimeoutSeconds)
			details.HeartbeatTimeout = aws.Int32(u.CallbackOptions.HeartbeatTimeoutSeconds)
		}
		ev.CallbackStartedDetails = details

	case durable.OperationTypeChainedInvoke:
		if u.Action != durable.OperationActionStart {
			return
		}
		ev.EventType = types.EventTypeChainedInvokeStarted
		details := &types.ChainedInvokeStartedDetails{Input: eventInput(u.Payload)}
		if u.ChainedInvokeOptions != nil {
			details.FunctionName = u.ChainedInvokeOptions.FunctionName
			details.TenantId = u.ChainedInvokeOptions.TenantId
		}
		ev.ChainedInvokeStartedDetails = details

	case durable.OperationTypeContext:
		switch u.Action {
		case durable.OperationActionStart:
			ev.EventType = types.EventTypeContextStarted
			ev.ContextStartedDetails = &types.ContextStartedDetails{}
		case durable.OperationActionSucceed:
			ev.EventType = types.EventTypeContextSucceeded
			ev.ContextSucceededDetails = &types.ContextSucceededDetails{Result: eventResult(u.Payload)}
		case durable.OperationActionFail:
			ev.EventType = types.EventTypeContextFailed
			ev.ContextFailedDetails = &types.ContextFailedDetails{Error: eventError(u.Error)}
		default:
			return
		}

	default:
		return
	}

	m.recordEvent(ev, at)
}

// recordOperationEvent records an event for a transition the runner applied
// to a stored operation outside a checkpoint: a timer completing, a
// callback resolving, or a chained invoke settling. at is the time of the
// transition.
//
// Caller must hold m.mu.
func (m *memoryClient) recordOperationEvent(op *durable.Operation, eventType types.EventType, result *string, err *durable.ErrorObject, at time.Time) {
	ev := types.Event{
		EventType: eventType,
		Id:        op.Id,
		Name:      op.Name,
		ParentId:  op.ParentId,
		SubType:   op.SubType,
	}
	switch eventType {
	case types.EventTypeWaitSucceeded:
		ev.WaitSucceededDetails = &types.WaitSucceededDetails{}
	case types.EventTypeCallbackSucceeded:
		ev.CallbackSucceededDetails = &types.CallbackSucceededDetails{Result: eventResult(result)}
	case types.EventTypeCallbackFailed:
		ev.CallbackFailedDetails = &types.CallbackFailedDetails{Error: eventError(err)}
	case types.EventTypeCallbackTimedOut:
		ev.CallbackTimedOutDetails = &types.CallbackTimedOutDetails{Error: eventError(err)}
	case types.EventTypeChainedInvokeSucceeded:
		ev.ChainedInvokeSucceededDetails = &types.ChainedInvokeSucceededDetails{Result: eventResult(result)}
	case types.EventTypeChainedInvokeFailed:
		ev.ChainedInvokeFailedDetails = &types.ChainedInvokeFailedDetails{Error: eventError(err)}
	case types.EventTypeChainedInvokeTimedOut:
		ev.ChainedInvokeTimedOutDetails = &types.ChainedInvokeTimedOutDetails{Error: eventError(err)}
	default:
		return
	}
	m.recordEvent(ev, at)
}

// eventResult wraps a result payload for an event, or returns nil when
// there is none.
func eventResult(payload *string) *types.EventResult {
	if payload == nil {
		return nil
	}
	return &types.EventResult{Payload: payload}
}

// eventInput wraps an input payload for an event, or returns nil when
// there is none.
func eventInput(payload *string) *types.EventInput {
	if payload == nil {
		return nil
	}
	return &types.EventInput{Payload: payload}
}

// eventError wraps an error record for an event, or returns nil when there
// is none.
func eventError(e *durable.ErrorObject) *types.EventError {
	if e == nil {
		return nil
	}
	return &types.EventError{Payload: &types.ErrorObject{
		ErrorType:    e.ErrorType,
		ErrorMessage: e.ErrorMessage,
		ErrorData:    e.ErrorData,
		StackTrace:   append([]string(nil), e.StackTrace...),
	}}
}
