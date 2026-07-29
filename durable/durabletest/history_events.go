// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// operationsFromEvents folds an execution's history events into one
// [types.Operation] record per operation, ordered by first appearance.
// Each event contributes its operation's identity on first sight and its
// status transition and details thereafter, so the final record reflects
// the operation's latest observed state.
func operationsFromEvents(events []types.Event) []types.Operation {
	byID := make(map[string]*types.Operation)
	var order []string

	for _, ev := range events {
		if ev.Id == nil {
			// Events without an operation ID (such as invocation
			// lifecycle events) do not describe an operation.
			continue
		}
		op, ok := byID[*ev.Id]
		if !ok {
			op = &types.Operation{Id: ev.Id}
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

	ops := make([]types.Operation, 0, len(order))
	for _, id := range order {
		ops = append(ops, *byID[id])
	}
	return ops
}

// applyEvent applies one event's type, status transition, and details to
// its operation record.
func applyEvent(op *types.Operation, ev types.Event) {
	switch ev.EventType {
	case types.EventTypeExecutionStarted:
		op.Type = types.OperationTypeExecution
		op.Status = types.OperationStatusStarted
		op.StartTimestamp = ev.EventTimestamp
	case types.EventTypeExecutionSucceeded:
		op.Type = types.OperationTypeExecution
		op.Status = types.OperationStatusSucceeded
		op.EndTimestamp = ev.EventTimestamp
	case types.EventTypeExecutionFailed:
		op.Type = types.OperationTypeExecution
		op.Status = types.OperationStatusFailed
		op.EndTimestamp = ev.EventTimestamp
	case types.EventTypeExecutionTimedOut:
		op.Type = types.OperationTypeExecution
		op.Status = types.OperationStatusTimedOut
		op.EndTimestamp = ev.EventTimestamp
	case types.EventTypeExecutionStopped:
		op.Type = types.OperationTypeExecution
		op.Status = types.OperationStatusStopped
		op.EndTimestamp = ev.EventTimestamp

	case types.EventTypeContextStarted:
		op.Type = types.OperationTypeContext
		op.Status = types.OperationStatusStarted
		op.StartTimestamp = ev.EventTimestamp
	case types.EventTypeContextSucceeded:
		op.Type = types.OperationTypeContext
		op.Status = types.OperationStatusSucceeded
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.ContextSucceededDetails; d != nil {
			ensureContextDetails(op).Result = resultPayload(d.Result)
		}
	case types.EventTypeContextFailed:
		op.Type = types.OperationTypeContext
		op.Status = types.OperationStatusFailed
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.ContextFailedDetails; d != nil {
			ensureContextDetails(op).Error = errorPayload(d.Error)
		}

	case types.EventTypeWaitStarted:
		op.Type = types.OperationTypeWait
		op.Status = types.OperationStatusStarted
		op.StartTimestamp = ev.EventTimestamp
		if d := ev.WaitStartedDetails; d != nil {
			ensureWaitDetails(op).ScheduledEndTimestamp = d.ScheduledEndTimestamp
		}
	case types.EventTypeWaitSucceeded:
		op.Type = types.OperationTypeWait
		op.Status = types.OperationStatusSucceeded
		op.EndTimestamp = ev.EventTimestamp
		ensureWaitDetails(op)
	case types.EventTypeWaitCancelled:
		op.Type = types.OperationTypeWait
		op.Status = types.OperationStatusCancelled
		op.EndTimestamp = ev.EventTimestamp
		ensureWaitDetails(op)

	case types.EventTypeStepStarted:
		op.Type = types.OperationTypeStep
		op.Status = types.OperationStatusStarted
		op.StartTimestamp = ev.EventTimestamp
	case types.EventTypeStepSucceeded:
		op.Type = types.OperationTypeStep
		op.Status = types.OperationStatusSucceeded
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.StepSucceededDetails; d != nil {
			sd := ensureStepDetails(op)
			sd.Result = resultPayload(d.Result)
			if d.RetryDetails != nil {
				sd.Attempt = d.RetryDetails.CurrentAttempt
			}
		}
	case types.EventTypeStepFailed:
		op.Type = types.OperationTypeStep
		op.Status = types.OperationStatusFailed
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.StepFailedDetails; d != nil {
			sd := ensureStepDetails(op)
			sd.Error = errorPayload(d.Error)
			if d.RetryDetails != nil {
				sd.Attempt = d.RetryDetails.CurrentAttempt
			}
		}

	case types.EventTypeChainedInvokeStarted:
		op.Type = types.OperationTypeChainedInvoke
		op.Status = types.OperationStatusStarted
		op.StartTimestamp = ev.EventTimestamp
	case types.EventTypeChainedInvokeSucceeded:
		op.Type = types.OperationTypeChainedInvoke
		op.Status = types.OperationStatusSucceeded
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.ChainedInvokeSucceededDetails; d != nil {
			ensureInvokeDetails(op).Result = resultPayload(d.Result)
		}
	case types.EventTypeChainedInvokeFailed:
		op.Type = types.OperationTypeChainedInvoke
		op.Status = types.OperationStatusFailed
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.ChainedInvokeFailedDetails; d != nil {
			ensureInvokeDetails(op).Error = errorPayload(d.Error)
		}
	case types.EventTypeChainedInvokeTimedOut:
		op.Type = types.OperationTypeChainedInvoke
		op.Status = types.OperationStatusTimedOut
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.ChainedInvokeTimedOutDetails; d != nil {
			ensureInvokeDetails(op).Error = errorPayload(d.Error)
		}
	case types.EventTypeChainedInvokeStopped:
		op.Type = types.OperationTypeChainedInvoke
		op.Status = types.OperationStatusStopped
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.ChainedInvokeStoppedDetails; d != nil {
			ensureInvokeDetails(op).Error = errorPayload(d.Error)
		}

	case types.EventTypeCallbackStarted:
		op.Type = types.OperationTypeCallback
		op.Status = types.OperationStatusStarted
		op.StartTimestamp = ev.EventTimestamp
		if d := ev.CallbackStartedDetails; d != nil {
			ensureCallbackDetails(op).CallbackId = d.CallbackId
		}
	case types.EventTypeCallbackSucceeded:
		op.Type = types.OperationTypeCallback
		op.Status = types.OperationStatusSucceeded
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.CallbackSucceededDetails; d != nil {
			ensureCallbackDetails(op).Result = resultPayload(d.Result)
		}
	case types.EventTypeCallbackFailed:
		op.Type = types.OperationTypeCallback
		op.Status = types.OperationStatusFailed
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.CallbackFailedDetails; d != nil {
			ensureCallbackDetails(op).Error = errorPayload(d.Error)
		}
	case types.EventTypeCallbackTimedOut:
		op.Type = types.OperationTypeCallback
		op.Status = types.OperationStatusTimedOut
		op.EndTimestamp = ev.EventTimestamp
		if d := ev.CallbackTimedOutDetails; d != nil {
			ensureCallbackDetails(op).Error = errorPayload(d.Error)
		}
	}
}

func ensureStepDetails(op *types.Operation) *types.StepDetails {
	if op.StepDetails == nil {
		op.StepDetails = &types.StepDetails{}
	}
	return op.StepDetails
}

func ensureCallbackDetails(op *types.Operation) *types.CallbackDetails {
	if op.CallbackDetails == nil {
		op.CallbackDetails = &types.CallbackDetails{}
	}
	return op.CallbackDetails
}

func ensureInvokeDetails(op *types.Operation) *types.ChainedInvokeDetails {
	if op.ChainedInvokeDetails == nil {
		op.ChainedInvokeDetails = &types.ChainedInvokeDetails{}
	}
	return op.ChainedInvokeDetails
}

func ensureContextDetails(op *types.Operation) *types.ContextDetails {
	if op.ContextDetails == nil {
		op.ContextDetails = &types.ContextDetails{}
	}
	return op.ContextDetails
}

func ensureWaitDetails(op *types.Operation) *types.WaitDetails {
	if op.WaitDetails == nil {
		op.WaitDetails = &types.WaitDetails{}
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
func errorPayload(e *types.EventError) *types.ErrorObject {
	if e == nil {
		return nil
	}
	return e.Payload
}
