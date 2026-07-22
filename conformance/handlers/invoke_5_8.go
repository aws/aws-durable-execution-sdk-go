// Requirement 5-8: Invoke with tenantId (tenant-isolated invocation).
//
// From test-requirements/invoke/5-8.yaml:
//
//	description: Invoke with tenantId — invoke a target function with tenant isolation
//	  config
//	handler: |
//	  A handler that invokes a target function with a tenantId configuration option.
//	  The tenantId is passed to the backend for tenant-isolated invocation.
//	  The handler receives a structured input with tenantId and payload fields.
//	invocations: |
//	  - Handler invokes `context.invoke(targetFunctionName, event.payload, { tenantId:
//	    event.tenantId })`, SDK checkpoints ChainedInvokeStarted with tenantId and payload in
//	    the checkpoint, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because target function completed, SDK checkpoints
//	    ChainedInvokeSucceeded, execution succeeds.
//	Input:
//	  tenantId: ${TENANT_ID}
//	  payload: ${PAYLOAD}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	ExpectedExecutionHistory:
//	  ... ChainedInvokeStartedDetails.TenantId: ${TENANT_ID}
//
// The first requirement in this suite to need operations.WithInvokeTenantID
// (added this session - see invoke.go's own doc comment on that option
// for why it was previously unexposed even though the wire plumbing
// already existed end-to-end).
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

type invoke58Event struct {
	TenantID string `json:"tenantId"`
	Payload  string `json:"payload"`
}

func init() {
	Register("5-8", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_8Handler, config(client))
	})
}

func invoke5_8Handler(event invoke58Event, dc types.DurableContext) (string, error) {
	return operations.Invoke[string, string](
		dc, "invoke", echoTargetFunctionARN(), event.Payload,
		operations.WithInvokeTenantID[string, string](event.TenantID),
	)
}
