// Command serde-preview-truncation demonstrates [durable.BuildPreview]
// attached to a filesystem serdes through
// [durable.FileSystemSerdesConfig.GeneratePreview]. The step result is
// offloaded to a file and the checkpoint envelope carries a compact
// preview of it next to the file reference. The preview uses
// [durable.PreviewIncludeAll] with one excluded field and a small
// MaxPreviewBytes, so only the first few fields fit and the preview is
// marked truncated.
//
// A preview is advisory metadata for the operation log. Excluding a field
// from the preview does not remove it from the offloaded file.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Record is the offloaded step result. Its fields are declared in the
// order the preview visits them, so the small ones come first and the
// large ones are dropped by the byte cap.
type Record struct {
	ID             string         `json:"id"`
	Region         string         `json:"region"`
	Tier           string         `json:"tier"`
	InternalSecret string         `json:"internalSecret"`
	Notes          string         `json:"notes"`
	History        []HistoryEntry `json:"history"`
}

// HistoryEntry is one element of Record.History.
type HistoryEntry struct {
	Event string `json:"event"`
}

// Output is the handler result.
type Output struct {
	ID          string `json:"id"`
	Tier        string `json:"tier"`
	NotesLength int    `json:"notesLength"`
}

// previewMaxBytes caps the preview so that the notes and history fields
// do not fit.
const previewMaxBytes = 128

// newSerdes returns the filesystem serdes with the preview generator this
// example demonstrates.
func newSerdes(basePath string) durable.Serdes {
	return durable.NewFileSystemSerdes(basePath, durable.FileSystemSerdesConfig{
		GeneratePreview: func(v any) map[string]any {
			return durable.BuildPreview(v, durable.PreviewConfig{
				Mode:            durable.PreviewIncludeAll,
				Exclude:         []durable.PreviewField{{Name: "internalSecret"}},
				MaxPreviewBytes: previewMaxBytes,
			})
		},
	})
}

// basePath returns where the serdes writes its files.
//
// The filesystem serdes documents that Lambda's /tmp is the wrong place for
// production use: a replay can land on a different execution environment,
// which cannot read a file written to another environment's /tmp. Production
// points the serdes at a durable, shared mount such as EFS or S3 Files.
//
// This example never reads across invocations. A step returns its result
// deserialized from the bytes the serdes just wrote, so the file is written
// and read back inside one invocation. That makes a local temporary
// directory sufficient here. A handler that resumes after a wait or a
// callback needs a real mount.
func basePath() string {
	if p := os.Getenv("SERDES_BASE_PATH"); p != "" {
		return p
	}
	return "/tmp/durable-serdes-preview-truncation"
}

func handler(ctx durable.Context, _ any) (Output, error) {
	if err := durable.ConfigureSerdes(ctx, durable.SerdesConfig{Serdes: newSerdes(basePath())}); err != nil {
		return Output{}, err
	}

	record, err := durable.Step(ctx, "build-record", func(_ durable.StepContext) (Record, error) {
		history := make([]HistoryEntry, 50)
		for i := range history {
			history[i] = HistoryEntry{Event: fmt.Sprintf("evt-%d", i)}
		}
		return Record{
			ID:             "acct-123",
			Region:         "us-west-2",
			Tier:           "gold",
			InternalSecret: "do-not-log-me",
			Notes:          strings.Repeat("N", 500),
			History:        history,
		}, nil
	})
	if err != nil {
		return Output{}, err
	}

	// record has already round-tripped through the serdes: Step returns the
	// value deserialized from the file it just wrote.
	return durable.Step(ctx, "read-record", func(_ durable.StepContext) (Output, error) {
		return Output{ID: record.ID, Tier: record.Tier, NotesLength: len(record.Notes)}, nil
	})
}

func main() { durable.Start(handler) }
