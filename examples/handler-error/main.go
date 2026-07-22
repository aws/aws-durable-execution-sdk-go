// Command handler-error demonstrates how a handler-level error (thrown
// directly from the handler function, not from a step) is captured and
// returned as a structured FAILED result by the durable execution runtime.
// Expected terminal state: FAILED.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(_ durable.Context, _ any) (any, error) {
	return nil, errors.New("intentional handler failure")
}

func main() { durable.Start(handler) }
