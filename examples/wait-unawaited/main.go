// Command wait-unawaited demonstrates starting a wait operation with
// [durable.WaitAsync] and never awaiting it. WaitAsync records the
// start of the wait before it returns. The function then returns its
// result without waiting for the wait to elapse.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	// Start a wait without awaiting it. Its start is recorded before
	// WaitAsync returns. Nothing awaits the future, so the invocation
	// does not wait for it to elapse.
	_ = durable.WaitAsync(ctx, "background-wait", 5*time.Second)

	return "result", nil
}

func main() { durable.Start(handler) }
