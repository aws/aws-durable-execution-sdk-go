// Command child-ops-preservation demonstrates that child context
// operations survive a suspend/resume cycle. A child context runs two
// steps (one succeeds, one fails), the failure is caught, and a wait
// forces suspension. On resume, the execution succeeds — proving the
// child's operations were preserved across the boundary.
package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result confirms the execution completed and the branch status.
type Result struct {
	BranchFailed bool `json:"branchFailed"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	branchFailed := false

	_, err := durable.RunInChildContext(ctx, "failing-branch",
		func(child durable.Context) (any, error) {
			_, err := durable.Step(child, "child-step-ok",
				func(_ durable.StepContext) (string, error) {
					return "ok", nil
				},
				durable.WithRetry(durable.NoRetry()))
			if err != nil {
				return nil, err
			}

			_, err = durable.Step(child, "child-step-boom",
				func(_ durable.StepContext) (any, error) {
					return nil, fmt.Errorf("intentional child failure")
				},
				durable.WithRetry(durable.NoRetry()))
			return nil, err
		})

	if err != nil {
		var childErr *durable.ChildContextError
		if !errors.As(err, &childErr) {
			return Result{}, err
		}
		branchFailed = true
	}

	// Force suspend/resume: on resume the backend hands back a fresh
	// operations payload. The child context's operations must survive.
	if err := durable.Wait(ctx, "cooldown", 1*time.Second); err != nil {
		return Result{}, err
	}

	return Result{BranchFailed: branchFailed}, nil
}

func main() { durable.Start(handler) }
