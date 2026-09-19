// Command target_non_durable is a plain Lambda function, not wrapped in a
// durable execution, deployed alongside the invoke suite as a non-durable
// invoke target.
package main

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
)

func handler(_ context.Context, _ any) (string, error) {
	return "non_durable_result", nil
}

func main() {
	lambda.Start(handler)
}
