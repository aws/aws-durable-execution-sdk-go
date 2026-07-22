// Command invoke_replay_rethrows implements conformance requirement 5-10:
// on replay, a failed invoke re-returns the recorded error without
// re-invoking the target; the error is caught and execution continues.
package main

import (
	"errors"
	"os"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event any) (string, error) {
	_, err := durable.Invoke[string](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), event)
	var invokeErr *durable.InvokeError
	if err != nil && !errors.As(err, &invokeErr) {
		return "", err
	}
	if err := durable.Wait(ctx, "", time.Second); err != nil {
		return "", err
	}
	return "caught and continued", nil
}

func main() {
	durable.Start(handler)
}
