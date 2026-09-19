// Command serde-filesystem-overflow demonstrates the filesystem serdes in
// [durable.FileSystemSerdesModeOverflow]. In this mode a value small enough
// for the checkpoint is stored inline, as JSON, and only a value that would
// exceed the checkpoint size limit is written to a file, with the checkpoint
// holding a reference to that file. The default mode,
// [durable.FileSystemSerdesModeAlways], writes every value to a file.
//
// Overflow mode suits a workload where most results are small and a few
// are large: the small ones stay readable in the operation log and cost no
// file I/O, and the large ones still fit.
//
// The handler checkpoints one value on each side of the threshold and then
// combines them, so the result proves both round trips.
package main

import (
	"os"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// largeSize is 300KB, above the checkpoint size limit, so the large
// document overflows to a file.
const largeSize = 300 * 1024

// Record is the small value. Its inline JSON is a few dozen bytes.
type Record struct {
	OrderID string  `json:"orderId"`
	Total   float64 `json:"total"`
}

// Output is the handler result.
type Output struct {
	SmallOrderID string `json:"smallOrderId"`
	LargeLength  int    `json:"largeLength"`
}

// basePath returns where the serdes writes overflowed files.
//
// The filesystem serdes documents that Lambda's /tmp is the wrong place for
// production use. /tmp belongs to one execution environment. A replay in a
// different execution environment cannot read a file written there, so a
// step whose checkpoint names such a file fails on replay. Production
// points the serdes at a durable, shared mount such as EFS or S3 Files.
//
// This example uses a local temporary directory anyway. It has no wait,
// callback, or invoke, so in normal operation it runs in a single
// invocation: each step returns its result deserialized from the bytes the
// serdes just wrote, and the file is written and read back inside that one
// invocation. The test pins the single-invocation claim. The one case a
// local directory does not cover is an invocation interrupted after
// large-document is checkpointed and before the handler returns; the replay
// that follows then runs in a fresh execution environment and fails to read
// the file. A handler that suspends, or one that must survive an
// interruption, needs a real mount.
func basePath() string {
	if p := os.Getenv("SERDES_BASE_PATH"); p != "" {
		return p
	}
	return "/tmp/durable-serdes-overflow"
}

func handler(ctx durable.Context, _ any) (Output, error) {
	serdes := durable.NewFileSystemSerdes(basePath(), durable.FileSystemSerdesConfig{
		Mode: durable.FileSystemSerdesModeOverflow,
	})
	if err := durable.ConfigureSerdes(ctx, durable.SerdesConfig{Serdes: serdes}); err != nil {
		return Output{}, err
	}

	// Small result: stored inline in the checkpoint as {"data": "<json>"}.
	// No file is written.
	small, err := durable.Step(ctx, "small-record", func(_ durable.StepContext) (Record, error) {
		return Record{OrderID: "ORD-42", Total: 19.99}, nil
	})
	if err != nil {
		return Output{}, err
	}

	// Large result: the inline envelope would exceed the checkpoint size
	// limit, so the serdes writes the value to a file and the checkpoint
	// stores {"file": "<path>"}.
	large, err := durable.Step(ctx, "large-document", func(_ durable.StepContext) (string, error) {
		return strings.Repeat("L", largeSize), nil
	})
	if err != nil {
		return Output{}, err
	}

	// Both values have already round-tripped through the serdes: Step
	// returns the value deserialized from the bytes it just checkpointed,
	// the small one from the inline envelope and the large one from the
	// overflow file.
	return durable.Step(ctx, "combine", func(_ durable.StepContext) (Output, error) {
		return Output{SmallOrderID: small.OrderID, LargeLength: len(large)}, nil
	})
}

func main() { durable.Start(handler) }
