// Command child-context-checkpoint-size-limit demonstrates
// [durable.RunInChildContext] with payloads that straddle the 256KB
// checkpoint size limit. This verifies both the inline path (below
// limit) and the ReplayChildren path (above limit) are exercised.
package main

import (
	"fmt"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

const (
	// checkpointLimit matches the SDK's internal limit.
	checkpointLimit = 256 * 1024
	// iterations keeps the test small enough to avoid timeout while
	// straddling the limit boundary.
	iterations = 5
)

// Result confirms all iterations completed.
type Result struct {
	Success         bool `json:"success"`
	TotalIterations int  `json:"totalIterations"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	for i := 0; i < iterations; i++ {
		// Payload sizes straddle the limit: some below, some at, some above.
		payloadSize := checkpointLimit - (iterations / 2) + i
		name := fmt.Sprintf("boundary-test-%d", i)

		_, err := durable.RunInChildContext(ctx, name,
			func(_ durable.Context) (string, error) {
				return strings.Repeat("x", payloadSize), nil
			})
		if err != nil {
			return Result{}, err
		}
	}

	return Result{Success: true, TotalIterations: iterations}, nil
}

func main() { durable.Start(handler) }
