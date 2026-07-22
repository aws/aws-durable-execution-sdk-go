// Command wait-basic demonstrates [durable.Wait]: suspending execution
// for a fixed duration without consuming compute resources.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	// Wait for 2 seconds — the backend suspends execution and resumes
	// in a new invocation when the duration elapses.
	if err := durable.Wait(ctx, "", 2*time.Second); err != nil {
		return "", err
	}
	return "Function Completed", nil
}

func main() { durable.Start(handler) }
