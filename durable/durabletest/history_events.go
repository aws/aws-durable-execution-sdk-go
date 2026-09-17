// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// operationsFromEvents folds an execution's history events into one
// [durable.Operation] record per operation, ordered by first appearance.
// Each event contributes its operation's identity on first sight and its
// status transition and details thereafter, so the final record reflects
// the operation's latest observed state.
func operationsFromEvents(events []types.Event) []durable.Operation {
	byID := make(map[string]*durable.Operation)
	var order []string

	for _, ev := range events {
		if ev.Id == nil {
			// Events without an operation ID (such as invocation
			// lifecycle events) do not describe an operation.
			continue
		}
		op, ok := byID[*ev.Id]
		if !ok {
			op = &durable.Operation{Id: ev.Id}
			byID[*ev.Id] = op
			order = append(order, *ev.Id)
		}
		if ev.Name != nil {
			op.Name = ev.Name
		}
		if ev.ParentId != nil {
			op.ParentId = ev.ParentId
		}
		if ev.SubType != nil {
			op.SubType = ev.SubType
		}
		applyEvent(op, ev)
	}

	ops := make([]durable.Operation, 0, len(order))
	for _, id := range order {
		ops = append(ops, *byID[id])
	}
	return ops
}

// applyEvent applies one event's type, status transition, and details to
// its operation record.
func applyEvent(op *durable.Operation, ev types.Event) {
	switch ev.EventType {
	case types.EventTypeExecutionStarted:
		op.Type = durable.OperationTypeExecution
		op.Status = durable.OperationStatusStarted
		op.StartTimestamp = ev.EventTimestamp
	case types.EventTypeExecutionSucceeded:
		op.Type = durable.OperationTypeExecution
		op.Status = durable.OperationStatusSucceeded
		op.EndTimestamp = ev.EventTimestamp
	case types.EventTypeExecutionFailed:
		op.Type = durable.OperationTypeExecution
		op.Status = durable.OperationStatusFailed
		op.EndTimestamp = ev.EventTimestamp
	case types.EventTypeExecutionTimedOut:
		op.Type = durable.OperationTypeExecution
		op.Status = durable.OperationStatusTimedOut
		op.EndTimestamp = ev.EventTimestamp
	case types.EventTypeExecutionStopped:
		op.Type = durable.OperationTypeExecution
		op.Status = durable.OperationStatusStopped
		op.EndTimestamp = ev.EventTimestamp

	case types.EventTypeContextStarted:
		op.Type = durable.OperationTypeContext
		op.Status = durable.OperationStatusStarted
		op.StartTimestamp = ev.EventTimestamp
	case types.EventTypeContextSucceeded:
		op.Type = durable.OperationTypeContext
		op.Status = durable.OperationStatusSucceeded
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.ContextSucceededDetails; d != nil {
			ensureContextDetails(op).Result = resultPayload(d.Result)
		}
	case types.EventTypeContextFailed:
		op.Type = durable.OperationTypeContext
		op.Status = durable.OperationStatusFailed
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.ContextFailedDetails; d != nil {
			ensureContextDetails(op).Error = errorPayload(d.Error)
		}

	case types.EventTypeWaitStarted:
		op.Type = durable.OperationTypeWait
		op.Status = durable.OperationStatusStarted
		op.StartTimestamp = ev.EventTimestamp
		if d := ev.WaitStartedDetails; d != nil {
			ensureWaitDetails(op).ScheduledEndTimestamp = d.ScheduledEndTimestamp
		}
	case types.EventTypeWaitSucceeded:
		op.Type = durable.OperationTypeWait
		op.Status = durable.OperationStatusSucceeded
		op.EndTimestamp = ev.EventTimestamp
		ensureWaitDetails(op)
	case types.EventTypeWaitCancelled:
		op.Type = durable.OperationTypeWait
		op.Status = durable.OperationStatusCancelled
		op.EndTimestamp = ev.EventTimestamp
		ensureWaitDetails(op)

	case types.EventTypeStepStarted:
		op.Type = durable.OperationTypeStep
		op.Status = durable.OperationStatusStarted
		op.StartTimestamp = ev.EventTimestamp
	case types.EventTypeStepSucceeded:
		op.Type = durable.OperationTypeStep
		op.Status = durable.OperationStatusSucceeded
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.StepSucceededDetails; d != nil {
			sd := ensureStepDetails(op)
			sd.Result = resultPayload(d.Result)
			if d.RetryDetails != nil {
				sd.Attempt = d.RetryDetails.CurrentAttempt
			}
		}
	case types.EventTypeStepFailed:
		op.Type = durable.OperationTypeStep
		op.Status = durable.OperationStatusFailed
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.StepFailedDetails; d != nil {
			sd := ensureStepDetails(op)
			sd.Error = errorPayload(d.Error)
			if d.RetryDetails != nil {
				sd.Attempt = d.RetryDetails.CurrentAttempt
			}
		}

	case types.EventTypeChainedInvokeStarted:
		op.Type = durable.OperationTypeChainedInvoke
		op.Status = durable.OperationStatusStarted
		op.StartTimestamp = ev.EventTimestamp
	case types.EventTypeChainedInvokeSucceeded:
		op.Type = durable.OperationTypeChainedInvoke
		op.Status = durable.OperationStatusSucceeded
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.ChainedInvokeSucceededDetails; d != nil {
			ensureInvokeDetails(op).Result = resultPayload(d.Result)
		}
	case types.EventTypeChainedInvokeFailed:
		op.Type = durable.OperationTypeChainedInvoke
		op.Status = durable.OperationStatusFailed
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.ChainedInvokeFailedDetails; d != nil {
			ensureInvokeDetails(op).Error = errorPayload(d.Error)
		}
	case types.EventTypeChainedInvokeTimedOut:
		op.Type = durable.OperationTypeChainedInvoke
		op.Status = durable.OperationStatusTimedOut
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.ChainedInvokeTimedOutDetails; d != nil {
			ensureInvokeDetails(op).Error = errorPayload(d.Error)
		}
	case types.EventTypeChainedInvokeStopped:
		op.Type = durable.OperationTypeChainedInvoke
		op.Status = durable.OperationStatusStopped
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.ChainedInvokeStoppedDetails; d != nil {
			ensureInvokeDetails(op).Error = errorPayload(d.Error)
		}

	case types.EventTypeCallbackStarted:
		op.Type = durable.OperationTypeCallback
		op.Status = durable.OperationStatusStarted
		op.StartTimestamp = ev.EventTimestamp
		if d := ev.CallbackStartedDetails; d != nil {
			ensureCallbackDetails(op).CallbackId = d.CallbackId
		}
	case types.EventTypeCallbackSucceeded:
		op.Type = durable.OperationTypeCallback
		op.Status = durable.OperationStatusSucceeded
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.CallbackSucceededDetails; d != nil {
			ensureCallbackDetails(op).Result = resultPayload(d.Result)
		}
	case types.EventTypeCallbackFailed:
		op.Type = durable.OperationTypeCallback
		op.Status = durable.OperationStatusFailed
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.CallbackFailedDetails; d != nil {
			ensureCallbackDetails(op).Error = errorPayload(d.Error)
		}
	case types.EventTypeCallbackTimedOut:
		op.Type = durable.OperationTypeCallback
		op.Status = durable.OperationStatusTimedOut
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.CallbackTimedOutDetails; d != nil {
			ensureCallbackDetails(op).Error = errorPayload(d.Error)
		}
	}
}

func ensureStepDetails(op *durable.Operation) *durable.StepDetails {
	if op.StepDetails == nil {
		op.StepDetails = &durable.StepDetails{}
	}
	return op.StepDetails
}

func ensureCallbackDetails(op *durable.Operation) *durable.CallbackDetails {
	if op.CallbackDetails == nil {
		op.CallbackDetails = &durable.CallbackDetails{}
	}
	return op.CallbackDetails
}

func ensureInvokeDetails(op *durable.Operation) *durable.ChainedInvokeDetails {
	if op.ChainedInvokeDetails == nil {
		op.ChainedInvokeDetails = &durable.ChainedInvokeDetails{}
	}
	return op.ChainedInvokeDetails
}

func ensureContextDetails(op *durable.Operation) *durable.ContextDetails {
	if op.ContextDetails == nil {
		op.ContextDetails = &durable.ContextDetails{}
	}
	return op.ContextDetails
}

func ensureWaitDetails(op *durable.Operation) *durable.WaitDetails {
	if op.WaitDetails == nil {
		op.WaitDetails = &durable.WaitDetails{}
	}
	return op.WaitDetails
}

// resultPayload extracts an event result's payload string.
func resultPayload(r *types.EventResult) *string {
	if r == nil {
		return nil
	}
	return r.Payload
}

// errorPayload extracts an event error's payload object.
func errorPayload(e *types.EventError) *durable.ErrorObject {
	if e == nil || e.Payload == nil {
		return nil
	}
	p := e.Payload
	return &durable.ErrorObject{
		ErrorType:    p.ErrorType,
		ErrorMessage: p.ErrorMessage,
		ErrorData:    p.ErrorData,
		StackTrace:   p.StackTrace,
	}
}
