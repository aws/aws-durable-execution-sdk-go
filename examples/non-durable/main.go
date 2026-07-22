// Command non-durable is a plain Lambda function that does NOT use the
// durable execution SDK. It demonstrates a standard handler for use as an
// Invoke target or baseline comparison.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
)

// Input controls the function behavior.
type Input struct {
	Failure bool `json:"failure"`
	WaitMs  int  `json:"waitMs"`
}

// Output is the structured response.
type Output struct {
	StatusCode int    `json:"statusCode"`
	Body       string `json:"body"`
}

func handler(_ context.Context, input Input) (Output, error) {
	if input.Failure {
		return Output{}, errors.New("this is a failure")
	}

	wait := time.Duration(input.WaitMs) * time.Millisecond
	if wait == 0 {
		wait = time.Second
	}
	time.Sleep(wait)

	body, _ := json.Marshal(map[string]string{"message": "Hello from Lambda!"})
	return Output{
		StatusCode: 200,
		Body:       string(body),
	}, nil
}

func main() { lambda.Start(handler) }
