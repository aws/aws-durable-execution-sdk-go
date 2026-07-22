// Command invoke_target_fails_caught implements conformance requirement
// 5-6: the invoked function fails, user code catches the InvokeError, and
// the execution succeeds with a fallback value.
package main

import (
	"errors"
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event any) (string, error) {
	_, err := durable.Invoke[string](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), event)
	var invokeErr *durable.InvokeError
	if err != nil && !errors.As(err, &invokeErr) {
		return "", err
	}
	return "fallback", nil
}

func main() {
	durable.Start(handler)
}
