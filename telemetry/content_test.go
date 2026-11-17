package telemetry

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// withResults returns a ContentConfig that records the result of every
// operation with one of the given names, unchanged. Tests that assert on
// operation results use it, because results are omitted by default.
func withResults(names ...string) ContentConfig {
	var c ContentConfig
	for _, name := range names {
		c.Operations.Overrides = append(c.Operations.Overrides, OperationOverride{
			OperationName: name,
			Result:        func(v any) any { return v },
		})
	}
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

// TestContentConfig_ZeroValue_IncludesInputOutputOmitsResults verifies the
// defaults: input and output are recorded unchanged, results are not.
func TestContentConfig_ZeroValue_IncludesInputOutputOmitsResults(t *testing.T) {
	var cfg ContentConfig
	if got := cfg.inputValue(map[string]int{"value": 41}); string(got) != `{"value":41}` {
		t.Errorf("inputValue = %s, want the value", got)
	}
	if got := cfg.outputValue("done"); string(got) != `"done"` {
		t.Errorf("outputValue = %s, want JSON string", got)
	}
	if got := cfg.resultValue("increment", `{"value":42}`); got != nil {
		t.Errorf("resultValue without override = %s, want nil", got)
	}
}

// TestContentConfig_TypedValue_PreservesJSONTypes verifies that a typed
// handler value is marshaled once and stored as that JSON value. A Go
// string becomes a JSON string, not raw text.
func TestContentConfig_TypedValue_PreservesJSONTypes(t *testing.T) {
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
	var cfg ContentConfig
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cfg.inputValue(tt.value); string(got) != tt.want {
				t.Errorf("inputValue(%v) = %s, want %s", tt.value, got, tt.want)
			}
			if got := cfg.outputValue(tt.value); string(got) != tt.want {
				t.Errorf("outputValue(%v) = %s, want %s", tt.value, got, tt.want)
			}
		})
	}
}

func TestContentConfig_TypedValue_OmitsNilAndUnmarshalable(t *testing.T) {
	var cfg ContentConfig
	if got := cfg.inputValue(nil); got != nil {
		t.Errorf("inputValue(nil) = %s, want nil", got)
	}
	if got := cfg.inputValue(make(chan int)); got != nil {
		t.Errorf("inputValue(chan) = %s, want nil", got)
	}
}

// TestContentConfig_TypedValue_TypedNilRecordsNull verifies that a typed
// nil is recorded as the JSON text null rather than omitted. A handler
// declared with a pointer result type that returns nil produces exactly
// this value, and the SDK checkpoints it as null.
func TestContentConfig_TypedValue_TypedNilRecordsNull(t *testing.T) {
	type out struct{ OK bool }
	var cfg ContentConfig
	tests := []struct {
		name  string
		value any
	}{
		{"nil pointer", (*out)(nil)},
		{"nil map", map[string]any(nil)},
		{"nil slice", []string(nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cfg.outputValue(tt.value); string(got) != `null` {
				t.Errorf("outputValue(%s) = %q, want null", tt.name, got)
			}
		})
	}
}

// TestContentConfig_Transform_PreservesLargeIntegers verifies that an
// int64 above 2^53 survives a transform unchanged. The transform receives
// the number as json.Number, so no float64 conversion rounds it.
func TestContentConfig_Transform_PreservesLargeIntegers(t *testing.T) {
	type order struct {
		ID        int64  `json:"id"`
		CreatedNs int64  `json:"createdNs"`
		Secret    string `json:"secret"`
	}
	in := order{ID: 9007199254740993, CreatedNs: 1762990123456789012, Secret: "x"}
	redact := func(v any) any {
		m, ok := v.(map[string]any)
		if !ok {
			return "unexpected shape"
		}
		delete(m, "secret")
		return m
	}
	cfg := ContentConfig{
		InputTransform:  func(v any) any { return v },
		OutputTransform: redact,
		Operations: ContentOperations{Overrides: []OperationOverride{
			{OperationName: "step", Result: redact},
		}},
	}
	// A transform re-marshals a map, so keys come back sorted.
	const text = `{"id":9007199254740993,"createdNs":1762990123456789012,"secret":"x"}`
	const wantFull = `{"createdNs":1762990123456789012,"id":9007199254740993,"secret":"x"}`
	const wantRedacted = `{"createdNs":1762990123456789012,"id":9007199254740993}`
	if got := cfg.inputValue(in); string(got) != wantFull {
		t.Errorf("inputValue = %s, want %s", got, wantFull)
	}
	if got := cfg.outputValue(in); string(got) != wantRedacted {
		t.Errorf("outputValue = %s, want %s", got, wantRedacted)
	}
	if got := cfg.resultValue("step", text); string(got) != wantRedacted {
		t.Errorf("resultValue = %s, want %s", got, wantRedacted)
	}
}

func TestContentConfig_Exclude_OmitsAndSkipsTransform(t *testing.T) {
	calls := 0
	cfg := ContentConfig{
		ExcludeInput:    true,
		InputTransform:  func(v any) any { calls++; return v },
		ExcludeOutput:   true,
		OutputTransform: func(v any) any { calls++; return v },
	}
	if got := cfg.inputValue("in"); got != nil {
		t.Errorf("inputValue = %s, want nil", got)
	}
	if got := cfg.outputValue("out"); got != nil {
		t.Errorf("outputValue = %s, want nil", got)
	}
	if calls != 0 {
		t.Errorf("transforms called %d times for excluded content, want 0", calls)
	}
}

// TestContentConfig_Transform_ReceivesDecodedValue verifies the shape the
// transform sees for each JSON type and that its return value is what the
// record carries.
func TestContentConfig_Transform_ReceivesDecodedValue(t *testing.T) {
	type payload struct {
		Password string `json:"password"`
		User     string `json:"user"`
	}
	tests := []struct {
		name     string
		value    any
		wantSeen any
		fn       func(any) any
		want     string
	}{
		{
			name:     "struct arrives as map and a key can be deleted",
			value:    payload{Password: "secret", User: "harry"},
			wantSeen: map[string]any{"password": "secret", "user": "harry"},
			fn: func(v any) any {
				m, ok := v.(map[string]any)
				if !ok {
					return "not a map"
				}
				delete(m, "password")
				return m
			},
			want: `{"user":"harry"}`,
		},
		{
			name:     "string arrives as string",
			value:    "secret",
			wantSeen: "secret",
			fn:       func(any) any { return "***" },
			want:     `"***"`,
		},
		{
			name:     "number arrives as json.Number",
			value:    41,
			wantSeen: json.Number("41"),
			fn: func(v any) any {
				n, ok := v.(json.Number)
				if !ok {
					return "not a json.Number"
				}
				i, err := n.Int64()
				if err != nil {
					return err.Error()
				}
				return i + 1
			},
			want: `42`,
		},
		{
			name:     "array arrives as []any",
			value:    []string{"a", "b"},
			wantSeen: []any{"a", "b"},
			fn: func(v any) any {
				a, ok := v.([]any)
				if !ok {
					return "not a slice"
				}
				return len(a)
			},
			want: `2`,
		},
		{
			name:     "boolean arrives as bool",
			value:    true,
			wantSeen: true,
			fn: func(v any) any {
				b, ok := v.(bool)
				if !ok {
					return "not a bool"
				}
				return !b
			},
			want: `false`,
		},
		{
			name:     "transform may replace the whole value",
			value:    map[string]int{"value": 41},
			wantSeen: map[string]any{"value": json.Number("41")},
			fn:       func(any) any { return map[string]string{"redacted": "yes"} },
			want:     `{"redacted":"yes"}`,
		},
		{
			name:     "transform returning nil records null",
			value:    "x",
			wantSeen: "x",
			fn:       func(any) any { return nil },
			want:     `null`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seenJSON []byte
			var seenType string
			cfg := ContentConfig{InputTransform: func(v any) any {
				// Snapshot before tt.fn, which may mutate v in place.
				seenJSON, _ = json.Marshal(v)
				seenType = fmt.Sprintf("%T", v)
				return tt.fn(v)
			}}
			got := cfg.inputValue(tt.value)
			if string(got) != tt.want {
				t.Errorf("inputValue = %s, want %s", got, tt.want)
			}
			wantJSON, _ := json.Marshal(tt.wantSeen)
			if string(seenJSON) != string(wantJSON) || seenType != fmt.Sprintf("%T", tt.wantSeen) {
				t.Errorf("transform saw %s (%s), want %s (%T)", seenJSON, seenType, wantJSON, tt.wantSeen)
			}
		})
	}
}

// TestContentConfig_Transform_FailureOmitsField verifies that a transform
// that panics, or returns a value that cannot be marshaled, omits the
// field. The raw value is never recorded in its place.
func TestContentConfig_Transform_FailureOmitsField(t *testing.T) {
	tests := []struct {
		name string
		fn   func(any) any
	}{
		{"panics", func(any) any { panic("boom") }},
		{"panics with error", func(any) any { panic(errors.New("boom")) }},
		{"returns unmarshalable", func(any) any { return make(chan int) }},
		{"panics on unexpected shape", func(v any) any {
			if _, ok := v.(map[string]any); !ok {
				panic(fmt.Sprintf("unexpected %T", v))
			}
			return v
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := ContentConfig{
				InputTransform:  tt.fn,
				OutputTransform: tt.fn,
				Operations: ContentOperations{Overrides: []OperationOverride{
					{OperationName: "step", Result: tt.fn},
				}},
			}
			if got := cfg.inputValue("secret"); got != nil {
				t.Errorf("inputValue = %s, want nil", got)
			}
			if got := cfg.outputValue("secret"); got != nil {
				t.Errorf("outputValue = %s, want nil", got)
			}
			if got := cfg.resultValue("step", `"secret"`); got != nil {
				t.Errorf("resultValue = %s, want nil", got)
			}
		})
	}
}

// TestContentConfig_ResultValue verifies the wire-text path: the override
// transform receives valid JSON decoded, and anything else as a string.
func TestContentConfig_ResultValue(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		wantSeen any
		want     string
	}{
		{"empty text omits", "", nil, ""},
		{"json object", `{"a":1}`, map[string]any{"a": json.Number("1")}, `{"a":1}`},
		{"json array", `[1,"two",null]`, []any{json.Number("1"), "two", nil}, `[1,"two",null]`},
		{"json string", `"quoted"`, "quoted", `"quoted"`},
		{"json number", `42`, json.Number("42"), `42`},
		{"large integer keeps digits", `9007199254740993`, json.Number("9007199254740993"), `9007199254740993`},
		{"json boolean", `false`, false, `false`},
		{"json null", `null`, nil, `null`},
		{"non-json arrives as string", `plain text`, "plain text", `"plain text"`},
		{"cut json arrives as string", `{"a":`, `{"a":`, `"{\"a\":"`},
		{"trailing data arrives as string", `1 2`, "1 2", `"1 2"`},
		{"surrounding whitespace decodes", ` 7 `, json.Number("7"), `7`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seen any
			called := false
			cfg := ContentConfig{Operations: ContentOperations{Overrides: []OperationOverride{{
				OperationName: "step",
				Result: func(v any) any {
					called = true
					seen = v
					return v
				},
			}}}}
			got := cfg.resultValue("step", tt.text)
			if tt.want == "" {
				if got != nil || called {
					t.Errorf("resultValue(%q) = %s (called=%v), want nil and no call", tt.text, got, called)
				}
				return
			}
			if string(got) != tt.want {
				t.Errorf("resultValue(%q) = %s, want %s", tt.text, got, tt.want)
			}
			if !json.Valid(got) {
				t.Errorf("resultValue(%q) = %s is not valid JSON", tt.text, got)
			}
			seenJSON, _ := json.Marshal(seen)
			wantJSON, _ := json.Marshal(tt.wantSeen)
			if string(seenJSON) != string(wantJSON) {
				t.Errorf("transform saw %v (%T), want %v (%T)", seen, seen, tt.wantSeen, tt.wantSeen)
			}
		})
	}
}

func TestContentConfig_ResultValue_RequiresOverrideWithResult(t *testing.T) {
	cfg := ContentConfig{Operations: ContentOperations{Overrides: []OperationOverride{
		{OperationName: "no-result"},
		{OperationName: "other", Result: func(v any) any { return v }},
	}}}
	if got := cfg.resultValue("no-result", `1`); got != nil {
		t.Errorf("override without Result recorded %s, want nil", got)
	}
	if got := cfg.resultValue("unlisted", `1`); got != nil {
		t.Errorf("unlisted operation recorded %s, want nil", got)
	}
	if got := cfg.resultValue("other", `1`); string(got) != `1` {
		t.Errorf("listed operation recorded %s, want 1", got)
	}
}

func TestContentOperations_LastOverrideWins(t *testing.T) {
	cfg := ContentConfig{Operations: ContentOperations{Overrides: []OperationOverride{
		{OperationName: "step", Result: func(any) any { return "first" }},
		{OperationName: "step", Result: func(any) any { return "second" }},
	}}}
	if got := cfg.resultValue("step", `1`); string(got) != `"second"` {
		t.Errorf("resultValue = %s, want the last override's value", got)
	}
	cfg.Operations.Overrides = append(cfg.Operations.Overrides, OperationOverride{OperationName: "step", Exclude: true})
	r := &Record{Operations: []OperationRecord{{ID: "1", Name: "step"}}}
	cfg.applyOperations(r)
	if len(r.Operations) != 0 {
		t.Errorf("operation kept despite a later Exclude override: %+v", r.Operations)
	}
}

func TestContentConfig_ApplyOperations_ExcludeDropsOperation(t *testing.T) {
	ops := []OperationRecord{
		{ID: "1", Name: "keep", Type: "STEP"},
		{ID: "2", Name: "drop", Type: "STEP"},
		{ID: "3", Name: "keep-also", Type: "STEP"},
	}
	r := &Record{Operations: ops}
	cfg := ContentConfig{Operations: ContentOperations{Overrides: []OperationOverride{
		{OperationName: "drop", Exclude: true},
	}}}
	cfg.applyOperations(r)

	if len(r.Operations) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(r.Operations))
	}
	for _, op := range r.Operations {
		if op.Name == "drop" {
			t.Error("excluded operation should not be present")
		}
	}
	// The caller's slice is untouched.
	if ops[1].Name != "drop" || len(ops) != 3 {
		t.Errorf("applyOperations modified the caller's backing array: %+v", ops)
	}
}

func TestContentConfig_ApplyOperations_ExcludeErrors(t *testing.T) {
	r := &Record{
		Error: &ErrorRecord{Type: "E", Message: "execution"},
		Operations: []OperationRecord{
			{ID: "1", Name: "a", Error: &ErrorRecord{Type: "E", Message: "m"}},
			{ID: "2", Name: "b", Status: "SUCCEEDED"},
		},
	}
	cfg := ContentConfig{Operations: ContentOperations{ExcludeErrors: true}}
	cfg.applyOperations(r)

	for _, op := range r.Operations {
		if op.Error != nil {
			t.Errorf("operation %s should have Error removed", op.ID)
		}
	}
	if r.Error == nil {
		t.Error("execution-level error must always be kept")
	}
}

func TestContentConfig_ApplyOperations_ZeroValueKeepsEverything(t *testing.T) {
	r := &Record{
		Input:  json.RawMessage(`"in"`),
		Output: json.RawMessage(`"out"`),
		Error:  &ErrorRecord{Type: "E", Message: "m"},
		Operations: []OperationRecord{
			{ID: "1", Name: "a", Error: &ErrorRecord{Type: "E", Message: "m"}, Result: json.RawMessage(`"r"`)},
		},
	}
	var cfg ContentConfig
	cfg.applyOperations(r)

	if r.Input == nil || r.Output == nil || r.Error == nil {
		t.Error("record-level fields should be preserved")
	}
	if len(r.Operations) != 1 || r.Operations[0].Error == nil || r.Operations[0].Result == nil {
		t.Errorf("operation fields should be preserved: %+v", r.Operations)
	}
}

// TestContentConfig_NoLengthCap verifies that content is never cut by
// length. The exporter size limiter is the only bound on a record.
func TestContentConfig_NoLengthCap(t *testing.T) {
	cfg := withResults("step")
	big := strings.Repeat("x", 1<<20)
	in := cfg.inputValue(big)
	if string(in) != jsonString(big) {
		t.Errorf("1 MiB input was altered: len %d", len(in))
	}
	res := cfg.resultValue("step", `"`+big+`"`)
	if len(res) != len(big)+2 {
		t.Errorf("1 MiB result was altered: len %d", len(res))
	}
}

// TestPlugin_Content_TransformsApplyToInputOutputAndResults drives the
// plugin end to end and asserts that each transform sees its own value,
// that an opted-in result is recorded, and that the record carries the
// transformed JSON values with no wrapper object.
func TestPlugin_Content_TransformsApplyToInputOutputAndResults(t *testing.T) {
	ctx := t.Context()
	var seen []string
	note := func(label string, v any) {
		b, _ := json.Marshal(v)
		seen = append(seen, label+":"+string(b))
	}
	mask := func(v any) any {
		if m, ok := v.(map[string]any); ok {
			for k, val := range m {
				if val == "secret" {
					m[k] = "***"
				}
			}
			return m
		}
		if s, ok := v.(string); ok {
			return strings.ReplaceAll(s, "secret", "***")
		}
		return v
	}
	content := ContentConfig{
		InputTransform:  func(v any) any { note("input", v); return mask(v) },
		OutputTransform: func(v any) any { note("output", v); return mask(v) },
		Operations: ContentOperations{Overrides: []OperationOverride{
			{OperationName: "increment", Result: func(v any) any { note("increment", v); return mask(v) }},
			{OperationName: "raw", Result: func(v any) any { note("raw", v); return mask(v) }},
		}},
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
	plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
		ID: "op-3", Name: "unlisted", Type: "STEP", SubType: "Step",
		Status: durable.PluginOperationSucceeded, Attempt: 1,
		StartTimestamp: start, EndTimestamp: start.Add(time.Second),
		Result: `"secret"`,
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
		t.Errorf("Input = %s, want masked object", rec.Input)
	}
	if string(rec.Output) != `"*** output"` {
		t.Errorf("Output = %s, want masked JSON string", rec.Output)
	}
	if len(rec.Operations) != 3 {
		t.Fatalf("got %d operations, want 3", len(rec.Operations))
	}
	if string(rec.Operations[0].Result) != `{"value":"***"}` {
		t.Errorf("op-1 Result = %s, want masked object", rec.Operations[0].Result)
	}
	if string(rec.Operations[1].Result) != `"not json ***"` {
		t.Errorf("op-2 Result = %s, want masked JSON string", rec.Operations[1].Result)
	}
	if rec.Operations[2].Result != nil {
		t.Errorf("op-3 Result = %s, want nil for an operation without an override", rec.Operations[2].Result)
	}
	wantSeen := []string{
		`input:{"password":"secret"}`,
		`increment:{"value":"secret"}`,
		`raw:"not json secret"`,
		`output:"secret output"`,
	}
	if strings.Join(seen, "\n") != strings.Join(wantSeen, "\n") {
		t.Errorf("transforms saw %q, want %q", seen, wantSeen)
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
	if _, present := decoded.Operations[2]["result"]; present {
		t.Errorf("result key present on op-3: %v", decoded.Operations[2])
	}
}

func TestPlugin_Content_ExcludedOperationSkipsResultTransform(t *testing.T) {
	ctx := t.Context()
	excludedCalls, includedCalls := 0, 0
	content := ContentConfig{Operations: ContentOperations{Overrides: []OperationOverride{
		{
			OperationName: "excluded",
			Exclude:       true,
			Result:        func(v any) any { excludedCalls++; return v },
		},
		{
			OperationName: "included",
			Result:        func(any) any { includedCalls++; return "transformed" },
		},
	}}}
	exp := &capturingExporter{}
	factory := New(Config{Content: content, Exporters: []Exporter{exp}})
	plugin := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{
		ExecutionArn: "arn:test",
	})
	for _, name := range []string{"excluded", "included"} {
		plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
			ID: name, Name: name, Type: "STEP",
			Status: durable.PluginOperationSucceeded, Result: `{"value":"secret"}`,
		})
	}
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		Status: durable.PluginInvocationSucceeded,
	})

	if excludedCalls != 0 {
		t.Errorf("excluded result transform called %d times, want 0", excludedCalls)
	}
	if includedCalls != 1 {
		t.Errorf("included result transform called %d times, want 1", includedCalls)
	}
	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	ops := records[0].Operations
	if len(ops) != 1 || ops[0].Name != "included" {
		t.Fatalf("operations = %+v, want only the included operation", ops)
	}
	if string(ops[0].Result) != `"transformed"` {
		t.Errorf("included result = %s, want transformed JSON string", ops[0].Result)
	}
}

// TestPlugin_Content_ResultsOmittedByDefault verifies the zero-value
// ContentConfig omits operation results while input and output are
// included, and that the omitted result leaves no key.
func TestPlugin_Content_ResultsOmittedByDefault(t *testing.T) {
	ctx := t.Context()
	exp := &capturingExporter{}
	factory := New(Config{Exporters: []Exporter{exp}})
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

// TestPlugin_Content_ExcludedContentHasNoKey verifies that excluded input
// and output leave no key in the emitted JSON and that the execution error
// is recorded even when operation errors are excluded.
func TestPlugin_Content_ExcludedContentHasNoKey(t *testing.T) {
	ctx := t.Context()
	exp := &capturingExporter{}
	content := ContentConfig{
		ExcludeInput:  true,
		ExcludeOutput: true,
		Operations:    ContentOperations{ExcludeErrors: true},
	}
	factory := New(Config{Content: content, Exporters: []Exporter{exp}})
	plugin := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{
		ExecutionArn:   "arn:test",
		ExecutionInput: map[string]int{"value": 41},
	})
	plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
		ID: "op-1", Name: "increment", Type: "STEP",
		Status: durable.PluginOperationFailed, Error: errors.New("step failed"),
	})
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		Status:         durable.PluginInvocationFailed,
		ExecutionError: errors.New("execution failed"),
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
	if _, present := decoded["error"]; !present {
		t.Errorf("execution error missing from %v", decoded)
	}
	ops, ok := decoded["operations"].([]any)
	if !ok || len(ops) != 1 {
		t.Fatalf("operations = %v, want one entry", decoded["operations"])
	}
	op, ok := ops[0].(map[string]any)
	if !ok {
		t.Fatalf("operations[0] = %v, want an object", ops[0])
	}
	for _, key := range []string{"result", "error"} {
		if _, present := op[key]; present {
			t.Errorf("%s key present in %v", key, op)
		}
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

// TestPlugin_Content_ResultTransformRunsOncePerResultText verifies that
// hooks reporting an unchanged result reuse the transformed value, and
// that a changed result text runs the transform again.
func TestPlugin_Content_ResultTransformRunsOncePerResultText(t *testing.T) {
	ctx := t.Context()
	calls := 0
	content := ContentConfig{Operations: ContentOperations{Overrides: []OperationOverride{{
		OperationName: "step",
		Result: func(v any) any {
			calls++
			return map[string]any{"seen": v}
		},
	}}}}
	exp := &capturingExporter{}
	factory := New(Config{Content: content, Exporters: []Exporter{exp}})
	start := time.Now().Add(-time.Minute)
	op := durable.OperationHookInfo{
		ID: "op-1", Name: "step", Type: "STEP",
		Status: durable.PluginOperationSucceeded, Attempt: 1,
		StartTimestamp: start, EndTimestamp: start.Add(time.Second),
		Result: `"first"`,
	}
	plugin := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{
		ExecutionArn: "arn:test",
		Operations:   map[string]durable.OperationHookInfo{op.ID: op},
	})
	plugin.OnOperationEnd(ctx, op)
	plugin.OnOperationChange(ctx, durable.OperationChangeHookInfo{
		UpdatedOperations: map[string]durable.OperationHookInfo{op.ID: op},
	})
	if calls != 1 {
		t.Fatalf("after three hooks with the same result, transform called %d times, want 1", calls)
	}

	op.Result = `"second"`
	plugin.OnOperationChange(ctx, durable.OperationChangeHookInfo{
		UpdatedOperations: map[string]durable.OperationHookInfo{op.ID: op},
	})
	if calls != 2 {
		t.Fatalf("after a changed result, transform called %d times, want 2", calls)
	}
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		Status: durable.PluginInvocationSucceeded,
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	ops := records[0].Operations
	if len(ops) != 1 {
		t.Fatalf("got %d operations, want 1", len(ops))
	}
	if string(ops[0].Result) != `{"seen":"second"}` {
		t.Errorf("Result = %s, want the transform of the changed text", ops[0].Result)
	}
	if calls != 2 {
		t.Errorf("after emission, transform called %d times, want 2", calls)
	}
}

// TestPlugin_Content_SampledOutSkipsTransforms verifies that a sampled-out
// invocation runs none of the content transforms. Its record is never
// emitted, so their results would be discarded.
func TestPlugin_Content_SampledOutSkipsTransforms(t *testing.T) {
	ctx := t.Context()
	inputCalls, outputCalls, resultCalls := 0, 0, 0
	content := ContentConfig{
		InputTransform:  func(v any) any { inputCalls++; return v },
		OutputTransform: func(v any) any { outputCalls++; return v },
		Operations: ContentOperations{Overrides: []OperationOverride{{
			OperationName: "step",
			Result:        func(v any) any { resultCalls++; return v },
		}}},
	}
	// At this rate the sampling threshold is 0 buckets, so every ARN
	// samples out.
	const rate = 1e-9
	arn := "arn:aws:lambda:us-east-1:123456789012:function:f:1"
	if sampledIn(arn, rate) {
		t.Fatalf("ARN %s samples in at rate %v", arn, rate)
	}
	exp := &capturingExporter{}
	factory := New(Config{
		SamplingRate: rate,
		EmitMode:     EmitAlways,
		Content:      content,
		Exporters:    []Exporter{exp},
	})
	op := durable.OperationHookInfo{
		ID: "op-1", Name: "step", Type: "STEP",
		Status: durable.PluginOperationSucceeded, Result: `{"value":1}`,
	}
	plugin := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{
		ExecutionArn:   arn,
		ExecutionInput: map[string]int{"value": 0},
		Operations:     map[string]durable.OperationHookInfo{op.ID: op},
	})
	plugin.OnOperationStart(ctx, op)
	plugin.OnOperationEnd(ctx, op)
	plugin.OnOperationChange(ctx, durable.OperationChangeHookInfo{
		UpdatedOperations: map[string]durable.OperationHookInfo{op.ID: op},
	})
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: map[string]int{"value": 1},
	})

	if inputCalls != 0 || outputCalls != 0 || resultCalls != 0 {
		t.Errorf("transform calls: input=%d output=%d result=%d, want all 0",
			inputCalls, outputCalls, resultCalls)
	}
	if records := exp.snapshot(); len(records) != 0 {
		t.Errorf("got %d records for a sampled-out invocation, want 0", len(records))
	}
}
