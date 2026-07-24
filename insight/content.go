package insight

// ContentConfig controls what content is included in emitted records and
// how it is formatted.
type ContentConfig struct {
	// IncludeInput controls whether execution input is included in
	// records. Defaults to true.
	IncludeInput bool

	// IncludeOutput controls whether execution output is included in
	// records. Defaults to true.
	IncludeOutput bool

	// IncludeOperationResults controls whether operation result fields
	// are included. Defaults to true.
	IncludeOperationResults bool

	// IncludeOperationErrors controls whether operation error fields
	// are included. Defaults to true.
	IncludeOperationErrors bool

	// IncludeExecutionError controls whether the execution-level error
	// is included. Defaults to true.
	IncludeExecutionError bool

	// MaxContentLength is the maximum length for content fields. Content
	// exceeding this length is truncated. A value of 0 disables
	// truncation.
	MaxContentLength int

	// OperationFilter, if non-nil, is called for each operation. Only
	// operations for which it returns true are included in the record.
	OperationFilter func(OperationRecord) bool

	// Redactor, if non-nil, is applied to content field values before
	// they are stored. Use this to mask sensitive data.
	Redactor func(string) string
}

// DefaultContentConfig returns a ContentConfig with sensible defaults:
// all content included, 10 KB max content length.
func DefaultContentConfig() ContentConfig {
	return ContentConfig{
		IncludeInput:            true,
		IncludeOutput:           true,
		IncludeOperationResults: true,
		IncludeOperationErrors:  true,
		IncludeExecutionError:   true,
		MaxContentLength:        10 * 1024, // 10 KB
	}
}

// BuildContentField creates a ContentField from a raw value, applying
// redaction and truncation per the config.
func (c ContentConfig) BuildContentField(value string) *ContentField {
	if value == "" {
		return nil
	}

	if c.Redactor != nil {
		value = c.Redactor(value)
	}

	if c.MaxContentLength > 0 && len(value) > c.MaxContentLength {
		cf := truncateContent(value, c.MaxContentLength)
		return &cf
	}

	return &ContentField{Value: value}
}

// ApplyToRecord filters and transforms a record's content fields in place
// based on the configuration.
func (c ContentConfig) ApplyToRecord(r *Record) {
	if !c.IncludeInput {
		r.Input = nil
	}
	if !c.IncludeOutput {
		r.Output = nil
	}
	if !c.IncludeExecutionError {
		r.Error = nil
	}

	if c.OperationFilter != nil || !c.IncludeOperationResults || !c.IncludeOperationErrors {
		filtered := r.Operations[:0]
		for _, op := range r.Operations {
			if c.OperationFilter != nil && !c.OperationFilter(op) {
				continue
			}
			if !c.IncludeOperationResults {
				op.Result = nil
			}
			if !c.IncludeOperationErrors {
				op.Error = nil
			}
			filtered = append(filtered, op)
		}
		r.Operations = filtered
	}
}
