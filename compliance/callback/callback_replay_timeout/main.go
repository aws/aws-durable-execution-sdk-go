// Command callback_replay_timeout implements conformance requirement 4-14:
// create callback with 3s timeout, catch timeout, wait, return.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	cb, err := durable.CreateCallback[string](ctx, name,
		durable.WithCallbackTimeout(3*time.Second))
	if err != nil {
		return "", err
	}
	result, cbErr := cb.Result()
	var outcome string
	if cbErr != nil {
		outcome = fmt.Sprintf("caught_timeout:%v", cbErr)
	} else {
		outcome = result
	}
	if err := durable.Wait(ctx, "after-cb", 2*time.Second); err != nil {
		return "", err
	}
	return outcome, nil
}

func main() { durable.Start(handler) }
