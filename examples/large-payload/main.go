// Command large-payload demonstrates handling of payloads that exceed the
// 256KB checkpoint size limit. When a child context result is too large to
// store inline, the SDK uses the ReplayChildren offload path: on replay the
// checkpoint stores only a marker and the child body re-executes to
// reconstruct the value.
package main

import (
	"fmt"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result captures metadata about the large-payload processing.
type Result struct {
	Success    bool `json:"success"`
	TotalSize  int  `json:"totalSize"`
	ChunkCount int  `json:"chunkCount"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// RunInChildContext with a result exceeding 256KB triggers the
	// ReplayChildren offload path: on replay the child body re-executes
	// rather than deserializing the oversized checkpoint.
	largeResult, err := durable.RunInChildContext(ctx, "large-payload-processor",
		func(child durable.Context) (string, error) {
			var chunks []string

			// Generate 6 chunks of ~50KB each ≈ 300KB total (>256KB limit).
			for i := 1; i <= 6; i++ {
				chunk, err := durable.Step(child, fmt.Sprintf("chunk-%d", i),
					func(_ durable.StepContext) (string, error) {
						return strings.Repeat("X", 50*1024), nil
					})
				if err != nil {
					return "", err
				}
				chunks = append(chunks, chunk)
			}

			return strings.Join(chunks, ""), nil
		})
	if err != nil {
		return Result{}, err
	}

	return Result{
		Success:    true,
		TotalSize:  len(largeResult),
		ChunkCount: 6,
	}, nil
}

func main() { durable.Start(handler) }
