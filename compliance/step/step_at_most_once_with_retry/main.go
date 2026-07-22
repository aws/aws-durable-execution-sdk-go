// Command step_at_most_once_with_retry implements conformance requirement
// 1-18: a step with AtMostOncePerRetry semantics and a retry strategy
// crashes on the first attempt and succeeds on the retry, tracking
// attempts in DynamoDB.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/attempts"
)

func handler(ctx durable.Context, event string) (string, error) {
	executionID := ctx.ExecutionArn()

	return durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		count, err := attempts.Increment(sc, executionID)
		if err != nil {
			return "", err
		}
		fmt.Println(event)
		time.Sleep(time.Second) // allow logs to flush to CloudWatch
		if count < 2 {
			os.Exit(1) // simulate a Lambda runtime crash on the first attempt
		}
		return "succeeded on second attempt", nil
	},
		durable.WithSemantics(durable.AtMostOncePerRetry),
		durable.WithRetry(durable.NewRetryStrategy(durable.RetryConfig{
			MaxAttempts:  3,
			InitialDelay: time.Second,
			Jitter:       durable.JitterNone,
		})))
}

func main() {
	durable.Start(handler)
}
