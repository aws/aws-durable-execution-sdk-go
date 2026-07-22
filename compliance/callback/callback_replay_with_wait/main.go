// Command callback_replay_with_wait implements conformance requirement
// 4-12: create callback, block on result, then wait 2s, return result.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	cb, err := durable.CreateCallback[string](ctx, name)
	if err != nil {
		return "", err
	}
	result, err := cb.Result()
	if err != nil {
		return "", err
	}
	if err := durable.Wait(ctx, "after-cb", 2*time.Second); err != nil {
		return "", err
	}
	return result, nil
}

func main() { durable.Start(handler) }
