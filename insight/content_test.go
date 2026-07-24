package insight

import (
	"strings"
	"testing"
)

func TestDefaultContentConfig(t *testing.T) {
	cfg := DefaultContentConfig()

	if !cfg.IncludeInput {
		t.Error("IncludeInput should default to true")
	}
	if !cfg.IncludeOutput {
		t.Error("IncludeOutput should default to true")
	}
	if !cfg.IncludeOperationResults {
		t.Error("IncludeOperationResults should default to true")
	}
	if !cfg.IncludeOperationErrors {
		t.Error("IncludeOperationErrors should default to true")
	}
	if !cfg.IncludeExecutionError {
		t.Error("IncludeExecutionError should default to true")
	}
	if cfg.MaxContentLength != 10*1024 {
		t.Errorf("MaxContentLength = %d, want %d", cfg.MaxContentLength, 10*1024)
	}
}

func TestContentConfig_BuildContentField(t *testing.T) {
	tests := []struct {
		name      string
		cfg       ContentConfig
		value     string
		wantNil   bool
		wantValue string
		wantTrunc bool
	}{
		{
			name:    "empty value returns nil",
			cfg:     DefaultContentConfig(),
			value:   "",
			wantNil: true,
		},
		{
			name:      "value within limit",
			cfg:       ContentConfig{MaxContentLength: 100},
			value:     "short",
			wantValue: "short",
			wantTrunc: false,
		},
		{
			name:      "value exceeds limit is truncated",
			cfg:       ContentConfig{MaxContentLength: 5},
			value:     "toolongvalue",
			wantTrunc: true,
		},
		{
			name:      "zero max means no truncation",
			cfg:       ContentConfig{MaxContentLength: 0},
			value:     strings.Repeat("x", 100000),
			wantValue: strings.Repeat("x", 100000),
			wantTrunc: false,
		},
		{
			name: "redactor is applied",
			cfg: ContentConfig{
				MaxContentLength: 0,
				Redactor: func(s string) string {
					return strings.ReplaceAll(s, "secret", "***")
				},
			},
			value:     "my secret data",
			wantValue: "my *** data",
			wantTrunc: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.BuildContentField(tt.value)
			if tt.wantNil {
				if got != nil {
					t.Errorf("expected nil, got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("expected non-nil ContentField")
			}
			if tt.wantValue != "" && got.Value != tt.wantValue {
				t.Errorf("Value = %q, want %q", got.Value, tt.wantValue)
			}
			if got.Truncated != tt.wantTrunc {
				t.Errorf("Truncated = %v, want %v", got.Truncated, tt.wantTrunc)
			}
		})
	}
}

func TestContentConfig_BuildContentField_RedactThenTruncate(t *testing.T) {
	cfg := ContentConfig{
		MaxContentLength: 10,
		Redactor: func(s string) string {
			return strings.ReplaceAll(s, "x", "XX")
		},
	}

	// "xxhello" -> redacted to "XXXXhello" (9 chars, within limit)
	got := cfg.BuildContentField("xxhello")
	if got == nil {
		t.Fatal("expected non-nil")
	}
	if got.Value != "XXXXhello" {
		t.Errorf("Value = %q, want %q", got.Value, "XXXXhello")
	}
	if got.Truncated {
		t.Error("should not be truncated (9 chars < 10 limit)")
	}
}

func TestContentConfig_ApplyToRecord_ExcludeInput(t *testing.T) {
	r := &Record{
		Input:  &ContentField{Value: "input-data"},
		Output: &ContentField{Value: "output-data"},
		Error:  &ErrorRecord{Type: "TestError", Message: "boom"},
	}
	cfg := ContentConfig{IncludeOutput: true, IncludeExecutionError: true}
	cfg.ApplyToRecord(r)

	if r.Input != nil {
		t.Error("Input should be excluded")
	}
	if r.Output == nil {
		t.Error("Output should be preserved")
	}
	if r.Error == nil {
		t.Error("Error should be preserved")
	}
}

func TestContentConfig_ApplyToRecord_ExcludeOutput(t *testing.T) {
	r := &Record{
		Input:  &ContentField{Value: "input-data"},
		Output: &ContentField{Value: "output-data"},
	}
	cfg := ContentConfig{IncludeInput: true}
	cfg.ApplyToRecord(r)

	if r.Input == nil {
		t.Error("Input should be preserved")
	}
	if r.Output != nil {
		t.Error("Output should be excluded")
	}
}

func TestContentConfig_ApplyToRecord_ExcludeExecutionError(t *testing.T) {
	r := &Record{
		Error: &ErrorRecord{Type: "Err", Message: "bad"},
	}
	cfg := ContentConfig{}
	cfg.ApplyToRecord(r)

	if r.Error != nil {
		t.Error("Error should be excluded when IncludeExecutionError is false")
	}
}

func TestContentConfig_ApplyToRecord_ExcludeOperationErrors(t *testing.T) {
	r := &Record{
		Operations: []OperationRecord{
			{ID: "1", Name: "a", Error: &ErrorRecord{Type: "E", Message: "m"}},
			{ID: "2", Name: "b", Status: "SUCCEEDED"},
		},
	}
	cfg := ContentConfig{IncludeInput: true, IncludeOutput: true, IncludeExecutionError: true, IncludeOperationResults: true}
	cfg.ApplyToRecord(r)

	for _, op := range r.Operations {
		if op.Error != nil {
			t.Errorf("operation %s should have Error stripped", op.ID)
		}
	}
}

func TestContentConfig_ApplyToRecord_ExcludeOperationResults(t *testing.T) {
	r := &Record{
		Operations: []OperationRecord{
			{ID: "1", Name: "a", Result: &ContentField{Value: "res"}},
			{ID: "2", Name: "b"},
		},
	}
	cfg := ContentConfig{
		IncludeInput:           true,
		IncludeOutput:          true,
		IncludeExecutionError:  true,
		IncludeOperationErrors: true,
	}
	cfg.ApplyToRecord(r)

	for _, op := range r.Operations {
		if op.Result != nil {
			t.Errorf("operation %s should have Result stripped", op.ID)
		}
	}
}

func TestContentConfig_ApplyToRecord_OperationFilter(t *testing.T) {
	r := &Record{
		Operations: []OperationRecord{
			{ID: "1", Name: "keep", Type: "STEP"},
			{ID: "2", Name: "drop", Type: "STEP"},
			{ID: "3", Name: "keep-also", Type: "STEP"},
		},
	}
	cfg := DefaultContentConfig()
	cfg.OperationFilter = func(op OperationRecord) bool {
		return op.Name != "drop"
	}
	cfg.ApplyToRecord(r)

	if len(r.Operations) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(r.Operations))
	}
	for _, op := range r.Operations {
		if op.Name == "drop" {
			t.Error("filtered operation should not be present")
		}
	}
}

func TestContentConfig_ApplyToRecord_AllIncluded(t *testing.T) {
	r := &Record{
		Input:  &ContentField{Value: "in"},
		Output: &ContentField{Value: "out"},
		Error:  &ErrorRecord{Type: "E", Message: "m"},
		Operations: []OperationRecord{
			{ID: "1", Error: &ErrorRecord{Type: "E", Message: "m"}, Result: &ContentField{Value: "r"}},
		},
	}
	cfg := DefaultContentConfig()
	cfg.ApplyToRecord(r)

	if r.Input == nil {
		t.Error("Input should be preserved")
	}
	if r.Output == nil {
		t.Error("Output should be preserved")
	}
	if r.Error == nil {
		t.Error("Error should be preserved")
	}
	if r.Operations[0].Error == nil {
		t.Error("Operation Error should be preserved")
	}
	if r.Operations[0].Result == nil {
		t.Error("Operation Result should be preserved")
	}
}
