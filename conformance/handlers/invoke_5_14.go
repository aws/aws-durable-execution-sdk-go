// Requirement 5-14: Multiple sequential invokes.
//
// From test-requirements/invoke/5-14.yaml:
//
//	description: Multiple sequential invokes — two invokes in sequence, each
//	  suspends/resumes
//	handler: |
//	  A handler that invokes two target functions sequentially. The first invoke completes,
//	  then the second invoke is started. Both succeed.
//	invocations: |
//	  - Handler invokes `context.invoke(targetFunction1)`, SDK checkpoints
//	    ChainedInvokeStarted, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because first target completed, SDK replays
//	    ChainedInvokeSucceeded, handler invokes `context.invoke(targetFunction2)`, SDK
//	    checkpoints second ChainedInvokeStarted, invocation completes, execution suspends.
//	  - Replay 2: Re-invoked because second target completed, SDK replays both invokes,
//	    checkpoints second ChainedInvokeSucceeded, execution succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// "Two target functions" here means two SEPARATE operations.Invoke calls
// (each mints its own step ID via c.NextStepID() - see invoke.go - so
// each is checkpointed as an independent CHAINED_INVOKE operation with
// its own Id, matching the YAML's own distinct ${ID1}/${ID2} events) -
// not necessarily two distinct deployed Lambda functions. Both calls
// target the SAME real echo-target function (this suite's one deployed
// target - see echo-target/main.go's own doc comment on why one smart
// target serves every requirement in this suite), which is sufficient:
// every requirement's own ExpectedExecutionHistory asserts
// ChainedInvokeStartedDetails.FunctionName as a wildcard '*' (per this
// session's own briefing, confirmed by re-reading this exact YAML above),
// so which specific function each invoke targets is not itself asserted
// - only that there are genuinely two independent, sequential
// CHAINED_INVOKE operations.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// invoke514Result is this handler's returned output shape - the YAML
// asserts no specific Result field, only that both invokes' own
// ChainedInvokeSucceeded events occur.
type invoke514Result struct {
	First  string `json:"first"`
	Second string `json:"second"`
}

func init() {
	Register("5-14", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_14Handler, config(client))
	})
}

func invoke5_14Handler(event string, dc types.DurableContext) (invoke514Result, error) {
	first, err := operations.Invoke[string, string](dc, "invoke-1", echoTargetFunctionARN(), event)
	if err != nil {
		return invoke514Result{}, err
	}

	second, err := operations.Invoke[string, string](dc, "invoke-2", echoTargetFunctionARN(), event)
	if err != nil {
		return invoke514Result{}, err
	}

	return invoke514Result{First: first, Second: second}, nil
}
