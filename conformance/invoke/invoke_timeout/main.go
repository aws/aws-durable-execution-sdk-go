// Command invoke_timeout implements the invoke-timeout scenario: the
// invoked function runs past its execution timeout, the caller receives an
// error that unwraps to [durable.ErrInvokeTimedOut], and the execution
// fails.
//
// The reference suites register this handler without a requirement number
// (the conformance runner defines none for it), so the runner deploys it
// but does not invoke it. The Go SDK has no per-invoke timeout option; the
// target's own execution timeout ends the chained invoke instead.
package main

import (
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event any) (string, error) {
	return durable.Invoke[string](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), event)
}

func main() {
	durable.Start(handler)
}
