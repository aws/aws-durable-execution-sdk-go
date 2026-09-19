// Command invoke_timeout_caught implements the caught invoke-timeout
// scenario: the invoked function runs past its execution timeout, user code
// recognizes [durable.ErrInvokeTimedOut], and the execution succeeds with a
// fallback value.
//
// The reference suites register this handler without a requirement number
// (the conformance runner defines none for it), so the runner deploys it
// but does not invoke it. The Go SDK has no per-invoke timeout option; the
// target's own execution timeout ends the chained invoke instead.
package main

import (
	"errors"
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event any) (string, error) {
	_, err := durable.Invoke[string](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), event)
	if err != nil && !errors.Is(err, durable.ErrInvokeTimedOut) {
		return "", err
	}
	return "fallback_result", nil
}

func main() {
	durable.Start(handler)
}
