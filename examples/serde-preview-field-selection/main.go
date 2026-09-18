// Command serde-preview-field-selection demonstrates [durable.BuildPreview]
// with [durable.PreviewExcludeAll]: nothing appears in the preview unless a
// selector includes it. It shows name matching at any depth
// ([durable.FieldMatchAnywhere], the default), exact dot-path matching
// ([durable.FieldMatchPath]), and masking, where a sensitive field is
// present in the preview but its value is replaced by a mask string.
//
// A preview is advisory metadata for the operation log. Masking a field in
// the preview does not redact it from the offloaded file.
package main

import (
	"os"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Profile is the offloaded step result.
type Profile struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	// Email at the root has the same field name as Customer.Email. The
	// path selector "customer.email" must not pick it up.
	Email    string   `json:"email"`
	Customer Customer `json:"customer"`
	// AuditLog is a large body that PreviewExcludeAll keeps out entirely.
	AuditLog string `json:"auditLog"`
}

// Customer is the nested record inside Profile.
type Customer struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	SSN   string `json:"ssn"`
}

// Output is the handler result.
type Output struct {
	ID            string `json:"id"`
	Email         string `json:"email"`
	CustomerEmail string `json:"customerEmail"`
	AuditLength   int    `json:"auditLength"`
}

// newSerdes returns the filesystem serdes with the preview generator this
// example demonstrates.
func newSerdes(basePath string) durable.Serdes {
	return durable.NewFileSystemSerdes(basePath, durable.FileSystemSerdesConfig{
		GeneratePreview: func(v any) map[string]any {
			return durable.BuildPreview(v, durable.PreviewConfig{
				Mode: durable.PreviewExcludeAll,
				Include: []durable.PreviewField{
					// Match id wherever it appears in the tree.
					{Name: "id"},
					// Match only this exact nested path, not any other
					// field named email.
					{Name: "customer.email", Match: durable.FieldMatchPath},
				},
				// Shown, but with the value replaced by the mask string.
				Mask: []durable.PreviewField{{Name: "ssn"}},
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
	return "/tmp/durable-serdes-preview-field-selection"
}

func handler(ctx durable.Context, _ any) (Output, error) {
	if err := durable.ConfigureSerdes(ctx, durable.SerdesConfig{Serdes: newSerdes(basePath())}); err != nil {
		return Output{}, err
	}

	profile, err := durable.Step(ctx, "build-profile", func(_ durable.StepContext) (Profile, error) {
		return Profile{
			ID:     "cust-9",
			Status: "active",
			Email:  "decoy@example.com",
			Customer: Customer{
				ID:    "cust-9",
				Email: "person@example.com",
				SSN:   "123-45-6789",
			},
			AuditLog: strings.Repeat("A", 2000),
		}, nil
	})
	if err != nil {
		return Output{}, err
	}

	// profile has already round-tripped through the serdes: Step returns
	// the value deserialized from the file it just wrote.
	return durable.Step(ctx, "read-profile", func(_ durable.StepContext) (Output, error) {
		return Output{
			ID:            profile.ID,
			Email:         profile.Email,
			CustomerEmail: profile.Customer.Email,
			AuditLength:   len(profile.AuditLog),
		}, nil
	})
}

func main() { durable.Start(handler) }
