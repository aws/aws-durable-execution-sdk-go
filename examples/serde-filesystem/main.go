// Command serde-filesystem demonstrates the filesystem serdes in its
// default mode, [durable.FileSystemSerdesModeAlways]. Every operation
// result is written to a file under the base path, and the checkpoint
// stores only a reference to that file. The checkpoint stays small however
// large the result is, so a large payload never fills the operation log.
//
// The example also sets two options that go with that mode:
//
//   - [durable.FileSystemPathEncodingHash] lays files out under a
//     fixed-length, filesystem-safe directory derived from the execution
//     ARN instead of the readable default layout.
//   - [durable.FileSystemSerdesConfig.GeneratePreview] stores a compact
//     preview of each offloaded value next to the file reference, so the
//     operation log shows what was stored without reading the file. The
//     preview includes the report's id and status and masks the owner's
//     email address.
//
// A preview is advisory metadata for the operation log. Masking a field in
// the preview does not redact it from the offloaded file.
package main

import (
	"os"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// bodyRepeat is how many times the report body repeats its marker. The
// body is about 140KB, large enough that storing it inline would be a poor
// fit for the checkpoint.
const bodyRepeat = 20000

// Report is the offloaded step result.
type Report struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	OwnerEmail string `json:"ownerEmail"`
	// Body is the large part of the report. It is kept out of the preview.
	Body string `json:"body"`
}

// Output is the handler result.
type Output struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	BodyLength int    `json:"bodyLength"`
}

// newSerdes returns the filesystem serdes this example demonstrates.
func newSerdes(basePath string) durable.Serdes {
	return durable.NewFileSystemSerdes(basePath, durable.FileSystemSerdesConfig{
		// Always is the default; it is spelled out here because the mode
		// is the point of the example.
		Mode:         durable.FileSystemSerdesModeAlways,
		PathEncoding: durable.FileSystemPathEncodingHash,
		GeneratePreview: func(v any) map[string]any {
			return durable.BuildPreview(v, durable.PreviewConfig{
				Mode:    durable.PreviewExcludeAll,
				Include: []durable.PreviewField{{Name: "id"}, {Name: "status"}},
				// Present in the preview, with the value replaced by the
				// mask string.
				Mask: []durable.PreviewField{{Name: "ownerEmail"}},
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
	return "/tmp/durable-serdes-filesystem"
}

func handler(ctx durable.Context, _ any) (Output, error) {
	if err := durable.ConfigureSerdes(ctx, durable.SerdesConfig{Serdes: newSerdes(basePath())}); err != nil {
		return Output{}, err
	}

	// The serdes writes the report to a file. The checkpoint stores
	// {"file": "<path>", "preview": {...}} and nothing of the body.
	report, err := durable.Step(ctx, "generate-report", func(_ durable.StepContext) (Report, error) {
		return Report{
			ID:         "RPT-001",
			Status:     "generated",
			OwnerEmail: "owner@example.com",
			Body:       strings.Repeat("REPORT-", bodyRepeat),
		}, nil
	})
	if err != nil {
		return Output{}, err
	}

	// report has already round-tripped through the serdes: Step returns
	// the value deserialized from the file it just wrote. This small
	// result is written to a file as well, because Always mode offloads
	// every value regardless of size.
	return durable.Step(ctx, "summarize-report", func(_ durable.StepContext) (Output, error) {
		return Output{
			ID:         report.ID,
			Status:     report.Status,
			BodyLength: len(report.Body),
		}, nil
	})
}

func main() { durable.Start(handler) }
