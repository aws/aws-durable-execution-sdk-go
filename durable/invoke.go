package durable

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// operationSubTypeChainedInvoke is the wire subtype for invoke operations.
const operationSubTypeChainedInvoke = "ChainedInvoke"

// InvokeOption configures a single invoke operation.
type InvokeOption interface {
	applyInvoke(*invokeOptions)
}

// WithInvokePayloadSerdes overrides the serializer for the invoke's input
// payload.
func WithInvokePayloadSerdes(s Serdes) InvokeOption {
	return invokeOptionFunc(func(o *invokeOptions) { o.payloadSerdes = s })
}

// WithInvokeResultSerdes overrides the serializer for the invoke's result.
func WithInvokeResultSerdes(s Serdes) InvokeOption {
	return invokeOptionFunc(func(o *invokeOptions) { o.resultSerdes = s })
}

// WithTenantID sets the tenant identifier for a tenant-isolated invocation.
func WithTenantID(tenantID string) InvokeOption {
	return invokeOptionFunc(func(o *invokeOptions) { o.tenantID = tenantID })
}

type invokeOptions struct {
	payloadSerdes Serdes
	resultSerdes  Serdes
	tenantID      string
}

type invokeOptionFunc func(*invokeOptions)

func (f invokeOptionFunc) applyInvoke(o *invokeOptions) { f(o) }

// Invoke durably invokes another Lambda function and returns its result.
// The invoked function runs as its own durable execution: the calling
// execution suspends after starting it and resumes when it completes. The
// output type parameter is specified by the caller and the input type is
// inferred:
//
//	receipt, err := durable.Invoke[Receipt](ctx, "charge", paymentFnArn, order)
//
// functionID is a function name or ARN. Durable target functions require a
// version or alias qualifier. name identifies the operation for tracking
// and debugging; pass "" for an unnamed invoke.
//
// If the invoked function fails, Invoke returns an [*InvokeError].
func Invoke[O, I any](ctx Context, name, functionID string, input I, opts ...InvokeOption) (O, error) {
	var zero O
	ec, ok := ctx.(*execContext)
	if !ok {
		return zero, fmt.Errorf("durable: Invoke %q: Context was not created by the SDK", name)
	}

	options := invokeOptions{payloadSerdes: ec.serdes, resultSerdes: ec.serdes}
	for _, o := range opts {
		o.applyInvoke(&options)
	}

	id, err := ec.claimOperation()
	if err != nil {
		return zero, err
	}

	return runInvoke[O, I](ec, id, name, functionID, input, options)
}

// InvokeAsync is [Invoke], except that the result is delivered through the
// returned future.
//
// The operation's identity is claimed before InvokeAsync returns, so
// consecutive InvokeAsync calls from one goroutine are
// replay-deterministic. On invocation suspension, the returned future is
// settled with errSuspendExecution so goroutines blocked on [Future.Result]
// unwind.
func InvokeAsync[O, I any](ctx Context, name, functionID string, input I, opts ...InvokeOption) *Future[O] {
	ec, ok := ctx.(*execContext)
	if !ok {
		return newFailedFuture[O](fmt.Errorf("durable: InvokeAsync %q: Context was not created by the SDK", name))
	}

	options := invokeOptions{payloadSerdes: ec.serdes, resultSerdes: ec.serdes}
	for _, o := range opts {
		o.applyInvoke(&options)
	}

	id, err := ec.claimOperation()
	if err != nil {
		return newFailedFuture[O](err)
	}

	fut := newFuture[O]()
	registerFuture(ec.suspend, fut)

	tok := ec.suspend.registerBranchToken()
	go func() {
		defer tok.release()
		branch := ec.branch(currentGoroutineOwner())
		branch.branchTok = tok
		result, runErr := runInvoke[O, I](branch, id, name, functionID, input, options)
		fut.settle(result, runErr)
	}()

	return fut
}

// runInvoke performs the invoke logic for a previously-claimed operation ID.
// It is shared by both the blocking [Invoke] and the async [InvokeAsync].
func runInvoke[O, I any](ec *execContext, id, name, functionID string, input I, options invokeOptions) (O, error) {
	var zero O

	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(types.OperationTypeChainedInvoke), operationSubTypeChainedInvoke, name); err != nil {
		return zero, err
	}
	if op != nil {
		switch op.status {
		case statusSucceeded:
			if op.invoke == nil {
				return zero, fmt.Errorf("durable: invoke %q: checkpointed %s operation has no invoke details", name, op.status)
			}
			var out O
			if err := options.resultSerdes.Unmarshal(ec.serdesCtx(id), []byte(op.invoke.result), &out); err != nil {
				return zero, fmt.Errorf("durable: invoke %q: deserialize result: %w", name, err)
			}
			return out, nil

		case statusFailed, statusTimedOut, statusStopped:
			return zero, invokeErrorFromCheckpoint(name, functionID, op)

		case statusStarted, statusPending, statusReady, statusCancelled:
			// The invoked execution has not settled: keep waiting.
			ec.blocked.Store(true)
			ec.suspend.commitPending(ec.abandon)
			return zero, errSuspendExecution
		}
	}

	payload, err := options.payloadSerdes.Marshal(ec.serdesCtx(id), input)
	if err != nil {
		return zero, fmt.Errorf("durable: invoke %q: serialize payload: %w", name, err)
	}

	// Check the serialized input size before checkpointing START.
	if sizeErr := checkResultSize(payload, name); sizeErr != nil {
		return zero, sizeErr
	}

	update := types.OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    types.OperationTypeChainedInvoke,
		SubType: aws.String(operationSubTypeChainedInvoke),
		Action:  types.OperationActionStart,
		Payload: aws.String(string(payload)),
		ChainedInvokeOptions: &types.ChainedInvokeOptions{
			FunctionName: aws.String(functionID),
		},
	}
	if options.tenantID != "" {
		update.ChainedInvokeOptions.TenantId = aws.String(options.tenantID)
	}
	if name != "" {
		update.Name = aws.String(name)
	}
	if parent := ec.ids.prefix; parent != "" {
		update.ParentId = aws.String(hashID(parent))
	}
	if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
		return zero, err
	}

	// The invoked function runs as its own durable execution: suspend and
	// resume when it settles.
	ec.blocked.Store(true)
	ec.suspend.commitPending(ec.abandon)
	return zero, errSuspendExecution
}

// invokeErrorFromCheckpoint reconstructs the failure of a settled invoke
// operation.
func invokeErrorFromCheckpoint(name, functionID string, op *operation) *InvokeError {
	// Extract the checkpointed cause message.
	re := &replayedError{errType: "Error", message: "invoked function failed"}
	if op.invoke != nil {
		re = &replayedError{errType: op.invoke.errType, message: op.invoke.errMessage}
	}
	var cause error = re
	// Wrap sentinel errors so callers can use errors.Is. The sentinel is
	// added to the chain UNDER the replayedError so that both errors.Is
	// (for the sentinel) and the checkpointed message are accessible.
	var status OperationStatus
	switch op.status {
	case statusTimedOut:
		re.sentinel = ErrInvokeTimedOut
		status = OperationStatusTimedOut
	case statusStopped:
		re.sentinel = ErrExecutionStopped
		status = OperationStatusStopped
	case statusCancelled:
		re.sentinel = ErrExecutionCancelled
		status = OperationStatusCancelled
	case statusFailed:
		status = OperationStatusFailed
	}
	return &InvokeError{Name: name, FunctionID: functionID, Status: status, Err: cause}
}
