// Command invoke_replay_skips implements conformance requirement 5-9: on
// replay, a succeeded invoke returns its checkpointed result without
// re-invoking the target.
package main

import (
	"os"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event any) (any, error) {
	result, err := durable.Invoke[any](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), event)
	if err != nil {
		return nil, err
	}
	if err := durable.Wait(ctx, "", time.Second); err != nil {
		return nil, err
	}
	return result, nil
}

func main() {
	durable.Start(handler)
}
