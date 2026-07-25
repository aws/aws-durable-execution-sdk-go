// Command target_echo is the durable target function for the DAG invoke
// conformance scenario (10-10): it waits briefly (so the caller suspends)
// and echoes its input. It mirrors invoke/target_echo and builds to
// publish/target_echo/ (logical id TargetEcho).
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event any) (any, error) {
	if err := durable.Wait(ctx, "", time.Second); err != nil {
		return nil, err
	}
	return event, nil
}

func main() {
	durable.Start(handler)
}
