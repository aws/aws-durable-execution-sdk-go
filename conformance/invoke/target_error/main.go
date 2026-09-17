// Command target_error is the durable target function that waits briefly
// then fails, for invoke failure conformance tests.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	if err := durable.Wait(ctx, "", time.Second); err != nil {
		return "", err
	}
	return "", errors.New("target function error")
}

func main() {
	durable.Start(handler)
}
