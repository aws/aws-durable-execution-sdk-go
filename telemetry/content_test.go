package telemetry

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// allContent returns a ContentConfig that includes every content field.
// Tests that assert on operation results use it, because the default
// excludes them.
func allContent() ContentConfig {
	c := DefaultContentConfig()
	c.IncludeOperationResults = true
	return c
}

// jsonString returns s encoded as a JSON string literal.
func jsonString(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func TestDefaultContentConfig(t *testing.T) {
	cfg := DefaultContentConfig()

	if !cfg.IncludeInput {
		t.Error("IncludeInput should default to true")
	}
	if !cfg.IncludeOutput {
		t.Error("IncludeOutput should default to true")
	}
	if cfg.IncludeOperationResults {
		t.Error("IncludeOperationResults should default to false")
	}
	if !cfg.IncludeOperationErrors {
		t.Error("IncludeOperationErrors should default to true")
	}
	if !cfg.IncludeExecutionError {
		t.Error("IncludeExecutionError should default to true")
	}
}

// TestContentConfig_MarshalContent_PreservesJSONTypes verifies that a
// typed handler value is marshaled once and stored as that JSON value. A
// Go string becomes a JSON string, not raw text.
func TestContentConfig_MarshalContent_PreservesJSONTypes(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"object", map[string]any{"value": 41}, `{"value":41}`},
		{"array", []int{1, 2, 3}, `[1,2,3]`},
		{"string", "hello", `"hello"`},
		{"empty string", "", `""`},
		{"number", 42, `42`},
		{"float", 1.5, `1.5`},
		{"boolean", true, `true`},
		{"typed nil pointer", (*struct{ A int })(nil), `null`},
		{"struct", struct {
			Value int `json:"value"`
		}{42}, `{"value":42}`},
	}
	cfg := DefaultContentConfig()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cfg.marshalContent(tt.value)
			if string(got) != tt.want {
				t.Errorf("marshalContent(%v) = %s, want %s", tt.value, got, tt.want)
			}
		})
	}
}

func TestContentConfig_MarshalContent_OmitsNilAndUnmarshalable(t *testing.T) {
	cfg := DefaultContentConfig()
	if got := cfg.marshalContent(nil); got != nil {
		t.Errorf("marshalContent(nil) = %s, want nil", got)
	}
	if got := cfg.marshalContent(make(chan int)); got != nil {
		t.Errorf("marshalContent(chan) = %s, want nil", got)
	}
}

// TestContentConfig_ContentValue verifies the wire-text path used for
// operation results: valid JSON is kept, anything else becomes a JSON
// string, and the Redactor's output goes through the same check.
func TestContentConfig_ContentValue(t *testing.T) {
	tests := []struct {
		name     string
		redactor func(string) string
		text     string
		want     string
	}{
		{name: "empty text omits", text: "", want: ""},
		{name: "json object kept", text: `{"a":1}`, want: `{"a":1}`},
		{name: "json array kept", text: `[1,"two",null]`, want: `[1,"two",null]`},
		{name: "json string kept", text: `"quoted"`, want: `"quoted"`},
		{name: "json number kept", text: `42`, want: `42`},
		{name: "json boolean kept", text: `false`, want: `false`},
		{name: "json null kept", text: `null`, want: `null`},
		{name: "non-json becomes json string", text: `plain text`, want: `"plain text"`},
		{name: "truncated json becomes json string", text: `{"a":`, want: `"{\"a\":"`},
		{
			name:     "redactor applied to json text",
			redactor: func(s string) string { return strings.ReplaceAll(s, "secret", "***") },
			text:     `{"token":"secret"}`,
			want:     `{"token":"***"}`,
		},
		{
			name:     "redactor returning non-json becomes json string",
			redactor: func(string) string { return "[REDACTED]" },
			text:     `{"token":"secret"}`,
			want:     `"[REDACTED]"`,
		},
		{
			name:     "redactor returning invalid json becomes json string",
			redactor: func(s string) string { return s[:len(s)-1] },
			text:     `{"token":"secret"}`,
			want:     `"{\"token\":\"secret\""`,
		},
		{
			name:     "redactor returning empty omits",
			redactor: func(string) string { return "" },
			text:     `{"token":"secret"}`,
			want:     "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := ContentConfig{Redactor: tt.redactor}
			got := cfg.contentValue(tt.text)
			if tt.want == "" {
				if got != nil {
					t.Errorf("contentValue(%q) = %s, want nil", tt.text, got)
				}
				return
			}
			if string(got) != tt.want {
				t.Errorf("contentValue(%q) = %s, want %s", tt.text, got, tt.want)
			}
			if !json.Valid(got) {
				t.Errorf("contentValue(%q) = %s is not valid JSON", tt.text, got)
			}
		})
	}
}

// TestContentConfig_NoLengthCap verifies that content is never cut by
// length. The exporter size limiter is the only bound on a record.
func TestContentConfig_NoLengthCap(t *testing.T) {
	cfg := DefaultContentConfig()
	big := strings.Repeat("x", 1<<20)
	in := cfg.marshalContent(big)
	if string(in) != jsonString(big) {
		t.Errorf("1 MiB input was altered: len %d", len(in))
	}
	res := cfg.contentValue(`"` + big + `"`)
	if len(res) != len(big)+2 {
		t.Errorf("1 MiB result was altered: len %d", len(res))
	}
}

// TestPlugin_Content_RedactorAppliesToInputOutputAndResults drives the
// plugin end to end and asserts that the Redactor sees the input, the
// output, and each operation result, and that the record carries the
// redacted JSON values with no wrapper object.
func TestPlugin_Content_RedactorAppliesToInputOutputAndResults(t *testing.T) {
	ctx := t.Context()
	var seen []string
	content := allContent()
	content.Redactor = func(text string) string {
		seen = append(seen, text)
		return strings.ReplaceAll(text, "secret", "***")
	}
	exp := &capturingExporter{}
	factory := New(Config{Content: content, Exporters: []Exporter{exp}})
	start := time.Now().Add(-time.Minute)
	info := durable.InvocationHookInfo{
		ExecutionArn:            "arn:aws:lambda:us-west-2:123456789012:function:fn:$LATEST/durable-execution/name/id",
		ExecutionInput:          map[string]string{"password": "secret"},
		ExecutionStartTimestamp: start,
	}
	plugin := startTestPlugin(t, ctx, factory, info)
	plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
		ID: "op-1", Name: "increment", Type: "STEP", SubType: "Step",
		Status: durable.PluginOperationSucceeded, Attempt: 1,
		StartTimestamp: start, EndTimestamp: start.Add(time.Second),
		Result: `{"value":"secret"}`,
	})
	plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
		ID: "op-2", Name: "raw", Type: "STEP", SubType: "Step",
		Status: durable.PluginOperationSucceeded, Attempt: 1,
		StartTimestamp: start, EndTimestamp: start.Add(time.Second),
		Result: `not json secret`,
	})
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "secret output",
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	rec := records[0]
	if string(rec.Input) != `{"password":"***"}` {
		t.Errorf("Input = %s, want redacted object", rec.Input)
	}
	if string(rec.Output) != `"*** output"` {
		t.Errorf("Output = %s, want redacted JSON string", rec.Output)
	}
	if len(rec.Operations) != 2 {
		t.Fatalf("got %d operations, want 2", len(rec.Operations))
	}
	if string(rec.Operations[0].Result) != `{"value":"***"}` {
		t.Errorf("op-1 Result = %s, want redacted object", rec.Operations[0].Result)
	}
	if string(rec.Operations[1].Result) != `"not json ***"` {
		t.Errorf("op-2 Result = %s, want redacted JSON string", rec.Operations[1].Result)
	}
	wantSeen := []string{`{"password":"secret"}`, `{"value":"secret"}`, `not json secret`, `"secret output"`}
	if strings.Join(seen, "\n") != strings.Join(wantSeen, "\n") {
		t.Errorf("redactor saw %q, want %q", seen, wantSeen)
	}

	// The emitted JSON carries the values directly, not {value: ...}.
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded struct {
		Input      map[string]any   `json:"input"`
		Output     string           `json:"output"`
		Operations []map[string]any `json:"operations"`
	}
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Input["password"] != "***" || decoded.Output != "*** output" {
		t.Errorf("decoded input/output = %v / %q", decoded.Input, decoded.Output)
	}
	if res, ok := decoded.Operations[0]["result"].(map[string]any); !ok || res["value"] != "***" {
		t.Errorf("decoded op-1 result = %v, want object", decoded.Operations[0]["result"])
	}
	if decoded.Operations[1]["result"] != "not json ***" {
		t.Errorf("decoded op-2 result = %v, want string", decoded.Operations[1]["result"])
	}
}

// TestPlugin_Content_ResultsExcludedByDefault verifies the default
// ContentConfig omits operation results and never calls the Redactor on
// them, while input and output are still included.
func TestPlugin_Content_ResultsExcludedByDefault(t *testing.T) {
	ctx := t.Context()
	var seen []string
	content := DefaultContentConfig()
	content.Redactor = func(text string) string {
		seen = append(seen, text)
		return text
	}
	exp := &capturingExporter{}
	factory := New(Config{Content: content, Exporters: []Exporter{exp}})
	plugin := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{
		ExecutionArn:   "arn:test",
		ExecutionInput: map[string]int{"value": 41},
	})
	plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
		ID: "op-1", Name: "increment", Type: "STEP",
		Status: durable.PluginOperationSucceeded, Result: `{"value":42}`,
	})
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: map[string]int{"value": 42},
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	rec := records[0]
	if string(rec.Input) != `{"value":41}` || string(rec.Output) != `{"value":42}` {
		t.Errorf("input/output = %s / %s", rec.Input, rec.Output)
	}
	if len(rec.Operations) != 1 || rec.Operations[0].Result != nil {
		t.Errorf("operations = %+v, want one operation without a result", rec.Operations)
	}
	if len(seen) != 2 {
		t.Errorf("redactor saw %q, want only input and output", seen)
	}

	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded struct {
		Operations []map[string]any `json:"operations"`
	}
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if _, present := decoded.Operations[0]["result"]; present {
		t.Errorf("result key present in %v", decoded.Operations[0])
	}
}

// TestPlugin_Content_OmittedContentHasNoKey verifies that excluded content
// leaves no key in the emitted JSON and never reaches the Redactor.
func TestPlugin_Content_OmittedContentHasNoKey(t *testing.T) {
	ctx := t.Context()
	exp := &capturingExporter{}
	redactorCalls := 0
	content := ContentConfig{
		IncludeOperationErrors: true,
		IncludeExecutionError:  true,
		Redactor: func(text string) string {
			redactorCalls++
			return text
		},
	}
	factory := New(Config{Content: content, Exporters: []Exporter{exp}})
	plugin := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{
		ExecutionArn:   "arn:test",
		ExecutionInput: map[string]int{"value": 41},
	})
	plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
		ID: "op-1", Name: "increment", Type: "STEP",
		Status: durable.PluginOperationSucceeded, Result: `{"value":42}`,
	})
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: map[string]int{"value": 42},
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	b, err := json.Marshal(records[0])
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for _, key := range []string{"input", "output"} {
		if _, present := decoded[key]; present {
			t.Errorf("%s key present in %v", key, decoded)
		}
	}
	ops, ok := decoded["operations"].([]any)
	if !ok || len(ops) != 1 {
		t.Fatalf("operations = %v, want one entry", decoded["operations"])
	}
	op, ok := ops[0].(map[string]any)
	if !ok {
		t.Fatalf("operations[0] = %v, want an object", ops[0])
	}
	if _, present := op["result"]; present {
		t.Errorf("result key present in %v", op)
	}
	if redactorCalls != 0 {
		t.Errorf("redactor called %d times for excluded content, want 0", redactorCalls)
	}
}

// TestPlugin_Content_LargeInputIsNotCut verifies that a large input
// reaches an exporter without a size limit whole. Only exporters that
// implement SizeLimiter bound the record.
func TestPlugin_Content_LargeInputIsNotCut(t *testing.T) {
	ctx := t.Context()
	exp := &capturingExporter{}
	factory := New(Config{Exporters: []Exporter{exp}})
	big := strings.Repeat("x", 1<<20)
	plugin := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{
		ExecutionArn:   "arn:test",
		ExecutionInput: big,
	})
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: big,
	})
	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	if string(records[0].Input) != jsonString(big) || string(records[0].Output) != jsonString(big) {
		t.Errorf("input len %d, output len %d, want %d each", len(records[0].Input), len(records[0].Output), len(big)+2)
	}
}

func TestContentConfig_ApplyToRecord_ExcludeInput(t *testing.T) {
	r := &Record{
		Input:  json.RawMessage(`"input-data"`),
		Output: json.RawMessage(`"output-data"`),
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
		Input:  json.RawMessage(`"input-data"`),
		Output: json.RawMessage(`"output-data"`),
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
			t.Errorf("operation %s should have Error removed", op.ID)
		}
	}
}

func TestContentConfig_ApplyToRecord_ExcludeOperationResults(t *testing.T) {
	r := &Record{
		Operations: []OperationRecord{
			{ID: "1", Name: "a", Result: json.RawMessage(`"res"`)},
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
			t.Errorf("operation %s should have Result removed", op.ID)
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
		Input:  json.RawMessage(`"in"`),
		Output: json.RawMessage(`"out"`),
		Error:  &ErrorRecord{Type: "E", Message: "m"},
		Operations: []OperationRecord{
			{ID: "1", Error: &ErrorRecord{Type: "E", Message: "m"}, Result: json.RawMessage(`"r"`)},
		},
	}
	cfg := allContent()
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
