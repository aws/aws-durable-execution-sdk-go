// Command child-context-large-data demonstrates [durable.RunInChildContext]
// with a payload exceeding the 256KB checkpoint size limit. When the
// serialized result is larger than the limit, the SDK automatically uses
// ReplayChildren mode: the checkpoint stores only a marker, and the child
// body is re-executed on replay to reconstruct the value.
package main

import (
	"fmt"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result summarizes the large-data processing outcome.
type Result struct {
	Success           bool    `json:"success"`
	Summary           Summary `json:"summary"`
	DataIntegrityHash int     `json:"dataIntegrityHash"`
}

// Summary captures metadata about the child context work.
type Summary struct {
	TotalDataSize int  `json:"totalDataSize"`
	StepsExecuted int  `json:"stepsExecuted"`
	ChildUsed     bool `json:"childContextUsed"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// Use RunInChildContext to handle large data that would exceed 256k.
	largeResult, err := durable.RunInChildContext(ctx, "large-data-processor",
		func(child durable.Context) (string, error) {
			var parts []string

			for i := 1; i <= 5; i++ {
				part, err := durable.Step(child, fmt.Sprintf("generate-data-%d", i),
					func(_ durable.StepContext) (string, error) {
						// Each step generates ~55KB of data.
						return strings.Repeat("A", 55*1024), nil
					})
				if err != nil {
					return "", err
				}
				parts = append(parts, part)
			}

			return strings.Join(parts, ""), nil
		})
	if err != nil {
		return Result{}, err
	}

	// Simple integrity check: length should be ~275KB.
	return Result{
		Success: true,
		Summary: Summary{
			TotalDataSize: len(largeResult),
			StepsExecuted: 5,
			ChildUsed:     true,
		},
		DataIntegrityHash: len(largeResult),
	}, nil
}

func main() { durable.Start(handler) }
