// Command callback_replay_failure implements conformance requirement 4-13:
// create callback, catch failure, wait, return error message.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	cb, err := durable.CreateCallback[string](ctx, name)
	if err != nil {
		return "", err
	}
	result, cbErr := cb.Result()
	var outcome string
	if cbErr != nil {
		outcome = fmt.Sprintf("caught_failure:%v", cbErr)
	} else {
		outcome = result
	}
	if err := durable.Wait(ctx, "after-cb", 2*time.Second); err != nil {
		return "", err
	}
	return outcome, nil
}

func main() { durable.Start(handler) }
