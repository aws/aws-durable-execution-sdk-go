package durable

import (
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable/internal/wire"
)

// operationsFromPayload converts the operation log page embedded in the
// invocation payload into engine operations. The wire shape is decoded
// separately from the SDK's exported Operation type: that type has no
// JSON tags and carries time.Time fields that reject the payload's numeric
// timestamps, so the embedded page gets its own narrow decode shape.
func operationsFromPayload(in *wire.InitialExecutionState) []*operation {
	ops := make([]*operation, 0, len(in.Operations))
	for _, w := range in.Operations {
		op := &operation{
			id:       w.Id,
			parentID: w.ParentId,
			status:   operationStatus(w.Status),
			opType:   w.Type,
			subType:  w.SubType,
			name:     w.Name,
		}
		if w.StartTimestamp.Valid {
			op.startTimestamp = w.StartTimestamp.Time
		}
		if w.EndTimestamp.Valid {
			op.endTimestamp = w.EndTimestamp.Time
		}
		if sd := w.StepDetails; sd != nil {
			op.step = &stepDetails{attempt: sd.Attempt, result: sd.Result}
			if sd.Error != nil {
				op.step.errType = sd.Error.ErrorType
				op.step.errMessage = sd.Error.ErrorMessage
				op.step.errData = sd.Error.ErrorData
				op.step.stackTrace = stackTraceLines(sd.Error.StackTrace)
			}
		}
		if id := w.ChainedInvokeDetails; id != nil {
			op.invoke = &invokeDetails{result: id.Result}
			if id.Error != nil {
				op.invoke.errType = id.Error.ErrorType
				op.invoke.errMessage = id.Error.ErrorMessage
				op.invoke.errData = id.Error.ErrorData
				op.invoke.stackTrace = stackTraceLines(id.Error.StackTrace)
			}
		}
		if cd := w.ContextDetails; cd != nil {
			op.childCtx = &contextDetails{result: cd.Result, replayChildren: cd.ReplayChildren}
			if cd.Error != nil {
				op.childCtx.errType = cd.Error.ErrorType
				op.childCtx.errMessage = cd.Error.ErrorMessage
				op.childCtx.errData = cd.Error.ErrorData
				op.childCtx.stackTrace = stackTraceLines(cd.Error.StackTrace)
			}
		}
		if cb := w.CallbackDetails; cb != nil {
			op.callback = &callbackDetails{callbackID: cb.CallbackId, result: cb.Result}
			if cb.Error != nil {
				op.callback.errType = cb.Error.ErrorType
				op.callback.errMessage = cb.Error.ErrorMessage
				op.callback.errData = cb.Error.ErrorData
				op.callback.stackTrace = stackTraceLines(cb.Error.StackTrace)
			}
		}
		ops = append(ops, op)
	}
	return ops
}

// customerInput returns the serialized customer event from the execution
// operation, following the cross-SDK convention of reading the first
// operation's InputPayload. The second result is false when no execution
// operation with a payload is present.
func customerInput(in *wire.InitialExecutionState) (string, bool) {
	if len(in.Operations) == 0 {
		return "", false
	}
	details := in.Operations[0].ExecutionDetails
	if details == nil || details.InputPayload == "" {
		return "", false
	}
	return details.InputPayload, true
}

// stackTraceLines splits the stack trace carried in the invocation payload
// into lines. The payload carries the trace as one string; the checkpoint
// API carries it as a list. An empty trace yields nil.
func stackTraceLines(trace string) []string {
	if trace == "" {
		return nil
	}
	return strings.Split(trace, "\n")
}
