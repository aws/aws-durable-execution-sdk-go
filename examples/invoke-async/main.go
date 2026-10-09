// Command invoke-async demonstrates [durable.InvokeAsync]: starting several
// invokes before awaiting any of them. Each stage of the order is sent to
// the invoke-simple-target function. The stages are independent, so all
// three invokes are started first and their results are read afterwards
// through the returned futures. The invoked functions run at the same time,
// unlike chained-invoke, where each [durable.Invoke] waits for the previous
// stage.
//
// Each InvokeAsync call claims its operation before it returns, so the
// three invokes keep the same identity on replay. [durable.Future.Result]
// blocks until its invoke settles and returns the decoded result, or an
// [*durable.InvokeError] when the invoked function fails. To combine the
// futures instead, pass them to [durable.All] or another combinator (see
// the future-* examples).
package main

import (
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input carries the target function name and the order to process.
type Input struct {
	TargetFunction string `json:"targetFunction"`
	OrderID        string `json:"orderId"`
}

// StageRequest is sent to the target function for one stage.
type StageRequest struct {
	OrderID string `json:"orderId"`
	Stage   string `json:"stage"`
}

// StageResult is returned by the target function.
type StageResult struct {
	OrderID string `json:"orderId"`
	Stage   string `json:"stage"`
	Status  string `json:"status"`
}

// Result lists the stage results in the order the stages were started.
type Result struct {
	OrderID string        `json:"orderId"`
	Stages  []StageResult `json:"stages"`
}

// stages are independent of each other, so they can run concurrently.
var stages = []string{"reserve-stock", "authorize-payment", "notify-customer"}

func handler(ctx durable.Context, event Input) (Result, error) {
	targetFunction := event.TargetFunction
	if targetFunction == "" {
		prefix := os.Getenv("FUNCTION_NAME_PREFIX")
		if prefix == "" {
			prefix = "v2-"
		}
		targetFunction = prefix + "go-invoke-simple-target:$LATEST"
	}

	orderID := event.OrderID
	if orderID == "" {
		orderID = "ORD-DEFAULT"
	}

	// Start every invoke before awaiting any of them.
	futures := make([]*durable.Future[StageResult], len(stages))
	for i, stage := range stages {
		futures[i] = durable.InvokeAsync[StageResult](ctx, stage, targetFunction,
			StageRequest{OrderID: orderID, Stage: stage})
	}

	results := make([]StageResult, len(futures))
	for i, f := range futures {
		// err is the invoke's failure, or the suspension signal when the
		// invocation ends before the invoke settles. Either way it is
		// returned unchanged.
		r, err := f.Result()
		if err != nil {
			return Result{}, err
		}
		results[i] = r
	}
	return Result{OrderID: orderID, Stages: results}, nil
}

func main() { durable.Start(handler) }
