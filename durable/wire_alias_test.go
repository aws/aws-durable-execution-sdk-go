package durable

import "github.com/aws/aws-durable-execution-sdk-go/durable/internal/wire"

// Test-only aliases for the invocation wire types. Production code imports
// the wire package directly; the tests in this package predate the move
// and build payloads with these shorter names.
type (
	invocationInput          = wire.InvocationInput
	initialExecutionState    = wire.InitialExecutionState
	wireOperation            = wire.Operation
	wireExecutionDetails     = wire.ExecutionDetails
	wireStepDetails          = wire.StepDetails
	wireChainedInvokeDetails = wire.ChainedInvokeDetails
	wireContextDetails       = wire.ContextDetails
	wireCallbackDetails      = wire.CallbackDetails
	wireStepError            = wire.ErrorObject
	wireFullError            = wire.ErrorObject
	wireError                = wire.ErrorObject
	flexTimestamp            = wire.Timestamp
	invocationResponse       = wire.InvocationResponse
)

const (
	invocationSucceeded = wire.StatusSucceeded
	invocationFailed    = wire.StatusFailed
	invocationPending   = wire.StatusPending
)
