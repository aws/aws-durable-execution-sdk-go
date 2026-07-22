package durable

// invocationInput is the payload the durable execution service delivers to
// a durable function invocation. It carries the execution identity, the
// checkpoint token for this invocation cycle, and the first page of the
// checkpointed operation log.
//
// The customer event is not a separate field: it travels as the serialized
// InputPayload of the execution operation inside the initial state.
type invocationInput struct {
	DurableExecutionArn   string                `json:"DurableExecutionArn"`
	CheckpointToken       string                `json:"CheckpointToken"`
	UpdatedOperationIds   []string              `json:"UpdatedOperationIds,omitempty"`
	InitialExecutionState initialExecutionState `json:"InitialExecutionState"`
}

// initialExecutionState is the operation log page embedded in the
// invocation payload. Further pages are fetched with
// GetDurableExecutionState using NextMarker.
//
// wireOperation is deliberately not the SDK's types.Operation: the SDK type
// has no JSON tags and carries time.Time fields that reject the payload's
// numeric timestamps, so the embedded page gets its own narrow decode
// shape.
type initialExecutionState struct {
	Operations []wireOperation `json:"Operations"`
	NextMarker string          `json:"NextMarker,omitempty"`
}

// wireOperation is the JSON shape of one checkpointed operation in the
// invocation payload, narrowed to the fields the engine consumes.
type wireOperation struct {
	Id                   string                    `json:"Id"`
	Status               string                    `json:"Status"`
	Type                 string                    `json:"Type,omitempty"`
	SubType              string                    `json:"SubType,omitempty"`
	Name                 string                    `json:"Name,omitempty"`
	ExecutionDetails     *wireExecutionDetails     `json:"ExecutionDetails,omitempty"`
	StepDetails          *wireStepDetails          `json:"StepDetails,omitempty"`
	ChainedInvokeDetails *wireChainedInvokeDetails `json:"ChainedInvokeDetails,omitempty"`
	ContextDetails       *wireContextDetails       `json:"ContextDetails,omitempty"`
	CallbackDetails      *wireCallbackDetails      `json:"CallbackDetails,omitempty"`
}

// wireExecutionDetails carries the execution operation's payload fields.
type wireExecutionDetails struct {
	// InputPayload is the serialized customer event for the execution.
	InputPayload string `json:"InputPayload,omitempty"`
}

// wireStepDetails carries a step operation's checkpointed attempt state.
// NextAttemptTimestamp is deliberately not decoded: the engine never
// consumes it (the backend owns the retry timer), and its numeric wire
// encoding does not fit time.Time.
type wireStepDetails struct {
	Attempt int            `json:"Attempt,omitempty"`
	Result  string         `json:"Result,omitempty"`
	Error   *wireStepError `json:"Error,omitempty"`
}

// wireStepError is the recorded failure of a step attempt.
type wireStepError struct {
	ErrorType    string `json:"ErrorType,omitempty"`
	ErrorMessage string `json:"ErrorMessage,omitempty"`
}

// wireChainedInvokeDetails carries a chained invoke's checkpointed outcome.
type wireChainedInvokeDetails struct {
	Result string         `json:"Result,omitempty"`
	Error  *wireFullError `json:"Error,omitempty"`
}

// wireContextDetails carries a child context's checkpointed outcome.
type wireContextDetails struct {
	Result         string         `json:"Result,omitempty"`
	ReplayChildren bool           `json:"ReplayChildren,omitempty"`
	Error          *wireFullError `json:"Error,omitempty"`
}

// wireFullError is a recorded failure including machine-readable data.
type wireFullError struct {
	ErrorType    string `json:"ErrorType,omitempty"`
	ErrorMessage string `json:"ErrorMessage,omitempty"`
	ErrorData    string `json:"ErrorData,omitempty"`
}

// wireCallbackDetails carries a callback operation's checkpointed outcome.
type wireCallbackDetails struct {
	CallbackId string         `json:"CallbackId,omitempty"`
	Result     string         `json:"Result,omitempty"`
	Error      *wireFullError `json:"Error,omitempty"`
}

func (in *initialExecutionState) toOperations() []*operation {
	ops := make([]*operation, 0, len(in.Operations))
	for _, w := range in.Operations {
		op := &operation{
			id:      w.Id,
			status:  operationStatus(w.Status),
			opType:  w.Type,
			subType: w.SubType,
			name:    w.Name,
		}
		if sd := w.StepDetails; sd != nil {
			op.step = &stepDetails{attempt: sd.Attempt, result: sd.Result}
			if sd.Error != nil {
				op.step.errType = sd.Error.ErrorType
				op.step.errMessage = sd.Error.ErrorMessage
			}
		}
		if id := w.ChainedInvokeDetails; id != nil {
			op.invoke = &invokeDetails{result: id.Result}
			if id.Error != nil {
				op.invoke.errType = id.Error.ErrorType
				op.invoke.errMessage = id.Error.ErrorMessage
				op.invoke.errData = id.Error.ErrorData
			}
		}
		if cd := w.ContextDetails; cd != nil {
			op.childCtx = &contextDetails{result: cd.Result, replayChildren: cd.ReplayChildren}
			if cd.Error != nil {
				op.childCtx.errType = cd.Error.ErrorType
				op.childCtx.errMessage = cd.Error.ErrorMessage
			}
		}
		if cb := w.CallbackDetails; cb != nil {
			op.callback = &callbackDetails{callbackID: cb.CallbackId, result: cb.Result}
			if cb.Error != nil {
				op.callback.errType = cb.Error.ErrorType
				op.callback.errMessage = cb.Error.ErrorMessage
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
func (in *initialExecutionState) customerInput() (string, bool) {
	if len(in.Operations) == 0 {
		return "", false
	}
	details := in.Operations[0].ExecutionDetails
	if details == nil || details.InputPayload == "" {
		return "", false
	}
	return details.InputPayload, true
}

// invocationResponse is what a durable invocation returns to Lambda: the
// terminal result, a failure, or PENDING when the execution suspends to
// resume in a later invocation. Result is a pre-serialized JSON string,
// double-encoded on the wire.
type invocationResponse struct {
	Status string     `json:"Status"`
	Result *string    `json:"Result,omitempty"`
	Error  *wireError `json:"Error,omitempty"`
}

// Invocation statuses returned to Lambda.
const (
	invocationSucceeded = "SUCCEEDED"
	invocationFailed    = "FAILED"
	invocationPending   = "PENDING"
)

// wireError is the serialized error shape in a FAILED invocation response.
type wireError struct {
	ErrorType    string `json:"ErrorType,omitempty"`
	ErrorMessage string `json:"ErrorMessage,omitempty"`
	ErrorData    string `json:"ErrorData,omitempty"`
	StackTrace   string `json:"StackTrace,omitempty"`
}
