// Command step_at_most_once_no_retry implements conformance requirement
// 1-17: a step with AtMostOncePerRetry semantics and no retry strategy
// crashes the runtime mid-attempt; on replay the SDK records a permanent
// failure without re-executing the step.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	return durable.Step(ctx, "at_most_once_flaky_step", func(_ durable.StepContext) (string, error) {
		fmt.Println(event)
		time.Sleep(time.Second) // allow logs to flush to CloudWatch
		os.Exit(1)              // simulate a Lambda runtime crash
		return "unreachable", nil
	}, durable.WithSemantics(durable.AtMostOncePerRetry), durable.WithRetry(durable.NoRetry()))
}

func main() {
	durable.Start(handler)
}
