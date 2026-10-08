// Command parallel-heterogeneous demonstrates ParallelMixed: each branch
// performs a different operation type (Step, Wait, Invoke) and returns its
// own result type, and the handler reads each result with that type.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

// Computation is the result of the compute branch.
type Computation struct {
	Expression string `json:"expression"`
	Value      int    `json:"value"`
}

// Output collects each branch result with its own type, and the completion
// reason. InvokeError holds the invoke branch error message when that
// branch failed.
type Output struct {
	Compute          Computation `json:"compute"`
	WaitedSeconds    int         `json:"waitedSeconds"`
	Invoked          string      `json:"invoked,omitempty"`
	InvokeError      string      `json:"invokeError,omitempty"`
	CompletionReason string      `json:"completionReason"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	compute := durable.NewTypedBranch("compute", func(ctx durable.Context) (Computation, error) {
		return durable.Step(ctx, "compute-value", func(_ durable.StepContext) (Computation, error) {
			return Computation{Expression: "7*6", Value: 42}, nil
		})
	})
	timer := durable.NewTypedBranch("timer", func(ctx durable.Context) (int, error) {
		if err := durable.Wait(ctx, "short-timer", time.Second); err != nil {
			return 0, err
		}
		return 1, nil
	})
	invoke := durable.NewTypedBranch("invoke", func(ctx durable.Context) (string, error) {
		result, err := durable.Invoke[string](ctx, "child-function", "target-handler", map[string]string{"key": "value"})
		if err != nil {
			return "", fmt.Errorf("invoke failed: %w", err)
		}
		return result, nil
	})

	res, err := durable.ParallelMixed(ctx, "heterogeneous-branches",
		[]durable.AnyBranch{compute, timer, invoke},
		// Tolerate a failed branch so every branch runs; the default
		// completion policy would stop the batch at the first failure.
		durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(3)}))
	if err != nil {
		return Output{}, err
	}
	out := Output{CompletionReason: res.Reason.String()}
	if out.Compute, err = compute.Result(res); err != nil {
		return Output{}, err
	}
	if out.WaitedSeconds, err = timer.Result(res); err != nil {
		return Output{}, err
	}
	// The deployed stack provides no companion function, so the invoke
	// branch fails in the cloud. Report its error instead of failing.
	if out.Invoked, err = invoke.Result(res); err != nil {
		out.InvokeError = err.Error()
	}
	return out, nil
}

func main() { durable.Start(handler) }
