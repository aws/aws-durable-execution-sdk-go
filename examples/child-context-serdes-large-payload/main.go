// Command child-context-serdes-large-payload demonstrates
// [durable.RunInChildContext] with [durable.NewFileSystemSerdes] handling a
// payload that exceeds the 256KB checkpoint size limit. The filesystem serdes
// offloads the serialized value to a file and stores only a small reference
// envelope in the checkpoint, keeping checkpoint size bounded regardless of
// payload size.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// payloadSize is 300KB, comfortably above the 256KB checkpoint threshold.
const payloadSize = 300 * 1024

// Output captures the round-trip verification of the large payload.
type Output struct {
	PayloadLength int    `json:"payloadLength"`
	PayloadHash   string `json:"payloadHash"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	basePath := os.Getenv("SERDES_BASE_PATH")
	if basePath == "" {
		basePath = "/tmp/durable-serdes"
	}

	serdes := durable.NewFileSystemSerdes(basePath)

	result, err := durable.RunInChildContext(ctx, "large-serdes-child",
		func(child durable.Context) (string, error) {
			return durable.Step(child, "generate-payload",
				func(_ durable.StepContext) (string, error) {
					// Deterministic payload: repeating pattern that is
					// trivially verifiable but exceeds 256KB.
					return strings.Repeat("ABCDEFGHIJ", payloadSize/10), nil
				})
		},
		durable.WithChildSerdes(serdes))
	if err != nil {
		return Output{}, fmt.Errorf("child context: %w", err)
	}

	hash := sha256.Sum256([]byte(result))

	return Output{
		PayloadLength: len(result),
		PayloadHash:   hex.EncodeToString(hash[:]),
	}, nil
}

func main() { durable.Start(handler) }
