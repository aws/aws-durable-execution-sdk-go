package telemetry

import (
	"encoding/json"
	"regexp"
	"testing"
	"time"
)

// timestampPattern is the shape every record timestamp must have: UTC,
// exactly three fraction digits, and a Z suffix.
var timestampPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)

func TestFormatTimestamp(t *testing.T) {
	est := time.FixedZone("EST", -5*60*60)
	cases := []struct {
		name string
		in   time.Time
		want string
	}{
		{"utc with nanoseconds", time.Date(2026, 10, 24, 2, 22, 35, 88_160_746, time.UTC), "2026-10-24T02:22:35.088Z"},
		{"whole second keeps three zeros", time.Date(2026, 10, 24, 2, 22, 35, 0, time.UTC), "2026-10-24T02:22:35.000Z"},
		{"trailing zero fraction keeps three digits", time.Date(2026, 10, 24, 2, 22, 35, 500_000_000, time.UTC), "2026-10-24T02:22:35.500Z"},
		{"non-utc converted to utc", time.Date(2026, 10, 23, 21, 22, 35, 7_000_000, est), "2026-10-24T02:22:35.007Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatTimestamp(tc.in); got != tc.want {
				t.Errorf("formatTimestamp(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRecord_MarshalJSON_TimestampKeysAndFormat(t *testing.T) {
	est := time.FixedZone("EST", -5*60*60)
	start := time.Date(2026, 10, 23, 21, 22, 35, 0, est)
	end := start.Add(1234 * time.Millisecond)
	emitted := end.Add(7 * time.Millisecond)
	opStart := start.Add(10 * time.Millisecond)
	opEnd := start.Add(20 * time.Millisecond)

	r := Record{
		RecordType: RecordType, SchemaVersion: SchemaVersion,
		ExecutionArn:   "arn:test",
		Status:         StatusSucceeded,
		StartTimestamp: &start,
		EndTimestamp:   &end,
		EmittedAt:      emitted,
		Operations: []OperationRecord{{
			ID: "op-1", Type: "STEP", Status: "SUCCEEDED",
			StartTimestamp: &opStart, EndTimestamp: &opEnd,
		}},
	}

	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	want := map[string]string{
		"startTime": "2026-10-24T02:22:35.000Z",
		"endTime":   "2026-10-24T02:22:36.234Z",
		"emittedAt": "2026-10-24T02:22:36.241Z",
	}
	for key, value := range want {
		got, ok := decoded[key].(string)
		if !ok || got != value {
			t.Errorf("%s = %v, want %q", key, decoded[key], value)
		}
	}
	for _, old := range []string{"startTimestamp", "endTimestamp"} {
		if _, present := decoded[old]; present {
			t.Errorf("record still carries renamed key %q", old)
		}
	}

	ops, ok := decoded["operations"].([]any)
	if !ok || len(ops) != 1 {
		t.Fatalf("operations = %v, want one element", decoded["operations"])
	}
	op, ok := ops[0].(map[string]any)
	if !ok {
		t.Fatalf("operation = %T, want an object", ops[0])
	}
	if got := op["startTime"]; got != "2026-10-24T02:22:35.010Z" {
		t.Errorf("operation startTime = %v", got)
	}
	if got := op["endTime"]; got != "2026-10-24T02:22:35.020Z" {
		t.Errorf("operation endTime = %v", got)
	}
	for _, old := range []string{"startTimestamp", "endTimestamp"} {
		if _, present := op[old]; present {
			t.Errorf("operation still carries renamed key %q", old)
		}
	}
	for _, key := range []string{"startTime", "endTime"} {
		value, _ := op[key].(string)
		if !timestampPattern.MatchString(value) {
			t.Errorf("operation %s = %q does not match %s", key, value, timestampPattern)
		}
	}
}

// TestRecord_MarshalJSON_OmitsNilTimestamps verifies that the shadow
// string fields in MarshalJSON keep omitempty semantics for the pointer
// timestamps.
func TestRecord_MarshalJSON_OmitsNilTimestamps(t *testing.T) {
	r := Record{ExecutionArn: "arn:test", EmittedAt: time.Now(), Operations: []OperationRecord{{ID: "op-1"}}}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded struct {
		StartTime  *string          `json:"startTime"`
		EndTime    *string          `json:"endTime"`
		EmittedAt  string           `json:"emittedAt"`
		Operations []map[string]any `json:"operations"`
	}
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.StartTime != nil || decoded.EndTime != nil {
		t.Errorf("nil timestamps were emitted: startTime=%v endTime=%v", decoded.StartTime, decoded.EndTime)
	}
	if !timestampPattern.MatchString(decoded.EmittedAt) {
		t.Errorf("emittedAt = %q does not match %s", decoded.EmittedAt, timestampPattern)
	}
	for _, key := range []string{"startTime", "endTime"} {
		if _, present := decoded.Operations[0][key]; present {
			t.Errorf("operation emitted nil %s", key)
		}
	}
}

// TestRecord_MarshalJSON_RoundTrips verifies that json.Unmarshal into
// Record still parses the three-digit timestamps MarshalJSON writes.
func TestRecord_MarshalJSON_RoundTrips(t *testing.T) {
	start := time.Date(2026, 10, 24, 2, 22, 35, 88_000_000, time.UTC)
	end := start.Add(time.Second)
	emitted := end.Add(time.Millisecond)
	r := Record{
		ExecutionArn: "arn:test", StartTimestamp: &start, EndTimestamp: &end, EmittedAt: emitted,
		Operations: []OperationRecord{{ID: "op-1", StartTimestamp: &start, EndTimestamp: &end}},
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var back Record
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if back.StartTimestamp == nil || !back.StartTimestamp.Equal(start) {
		t.Errorf("StartTimestamp = %v, want %v", back.StartTimestamp, start)
	}
	if back.EndTimestamp == nil || !back.EndTimestamp.Equal(end) {
		t.Errorf("EndTimestamp = %v, want %v", back.EndTimestamp, end)
	}
	if !back.EmittedAt.Equal(emitted) {
		t.Errorf("EmittedAt = %v, want %v", back.EmittedAt, emitted)
	}
	if len(back.Operations) != 1 || back.Operations[0].StartTimestamp == nil || !back.Operations[0].StartTimestamp.Equal(start) {
		t.Errorf("operation timestamps did not round-trip: %+v", back.Operations)
	}
}

// TestRecord_MarshalJSON_IntegersStayIntegers verifies that every integer
// field renders without a fraction or exponent.
func TestRecord_MarshalJSON_IntegersStayIntegers(t *testing.T) {
	d := int64(0)
	r := Record{
		ExecutionArn: "arn:test", EmittedAt: time.Now(), DurationMs: &d, DroppedOperations: 3,
		Operations: []OperationRecord{{ID: "op-1", DurationMs: &d, Attempt: 1}},
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, want := range []string{`"durationMs":0`, `"droppedOperations":3`, `"attempt":1`} {
		if !regexp.MustCompile(regexp.QuoteMeta(want) + `[,}]`).Match(b) {
			t.Errorf("%s not found as an integer in %s", want, b)
		}
	}
}
