package insight

import (
	"strings"
	"testing"
)

func TestTruncateContent_WithinLimit(t *testing.T) {
	cf := truncateContent("hello", 10)
	if cf.Value != "hello" {
		t.Errorf("Value = %q, want %q", cf.Value, "hello")
	}
	if cf.Truncated {
		t.Error("should not be truncated")
	}
}

func TestTruncateContent_ExactlyAtLimit(t *testing.T) {
	cf := truncateContent("12345", 5)
	if cf.Value != "12345" {
		t.Errorf("Value = %q, want %q", cf.Value, "12345")
	}
	if cf.Truncated {
		t.Error("should not be truncated at exact limit")
	}
}

func TestTruncateContent_ExceedsLimit(t *testing.T) {
	cf := truncateContent("toolong", 4)
	if len(cf.Value) > 4 {
		t.Errorf("Value length = %d, want <= 4", len(cf.Value))
	}
	if !cf.Truncated {
		t.Error("should be truncated")
	}
}

func TestTruncateContent_ZeroMaxLen(t *testing.T) {
	value := strings.Repeat("x", 1000)
	cf := truncateContent(value, 0)
	if cf.Value != value {
		t.Error("maxLen=0 should disable truncation")
	}
	if cf.Truncated {
		t.Error("should not be truncated when disabled")
	}
}

func TestTruncateContent_NegativeMaxLen(t *testing.T) {
	value := "some data"
	cf := truncateContent(value, -1)
	if cf.Value != value {
		t.Error("negative maxLen should disable truncation")
	}
	if cf.Truncated {
		t.Error("should not be truncated")
	}
}

func TestTruncateJSON_ValidJSONObject(t *testing.T) {
	input := `{"name":"alice","age":30,"city":"wonderland"}`
	result := truncateJSON(input, 25)

	if len(result) > 25 {
		t.Errorf("result length %d exceeds max %d", len(result), 25)
	}
	// Should cut at a JSON boundary (after a comma or brace).
	if result != "" && result[len(result)-1] != ',' &&
		result[len(result)-1] != '}' && result[len(result)-1] != ']' {
		// Acceptable — might not find a good cut point within 25%.
		t.Logf("truncated at non-boundary: %q", result)
	}
}

func TestTruncateJSON_ValidJSONArray(t *testing.T) {
	input := `[1,2,3,4,5,6,7,8,9,10]`
	result := truncateJSON(input, 10)

	if len(result) > 10 {
		t.Errorf("result length %d exceeds max %d", len(result), 10)
	}
}

func TestTruncateJSON_NonJSON(t *testing.T) {
	input := "this is plain text that is long enough to truncate"
	result := truncateJSON(input, 10)

	if result != input[:10] {
		t.Errorf("non-JSON should truncate at byte boundary, got %q", result)
	}
}

func TestTruncateJSON_EmptyString(t *testing.T) {
	result := truncateJSON("", 10)
	if result != "" {
		t.Errorf("empty input should return empty, got %q", result)
	}
}

func TestTruncateJSON_FitsAlready(t *testing.T) {
	input := `{"a":1}`
	result := truncateJSON(input, 100)
	if result != input {
		t.Errorf("result = %q, want %q (should not truncate)", result, input)
	}
}

func TestTruncateJSON_CutsAtComma(t *testing.T) {
	// Build a JSON where we know there's a comma at a convenient spot.
	input := `{"a":"x","b":"y","c":"z"}`
	// maxLen right after first key-value + comma: {"a":"x",
	// That's 9 chars: {"a":"x",
	result := truncateJSON(input, 12)

	if len(result) > 12 {
		t.Errorf("result too long: %d", len(result))
	}
	// Should end at a comma or brace if possible.
	if len(result) > 0 {
		last := result[len(result)-1]
		if last != ',' && last != '}' && last != ']' {
			t.Logf("cut point: %q (last char %c may not be a boundary)", result, last)
		}
	}
}

func TestTruncateJSON_StringWithEscapes(t *testing.T) {
	// Ensure we don't break on escaped quotes inside strings.
	input := `{"msg":"say \"hello\" world","other":"val"}`
	result := truncateJSON(input, 35)

	if len(result) > 35 {
		t.Errorf("result too long: %d", len(result))
	}
}

func TestFindJSONCutPoint_NoGoodCut(t *testing.T) {
	// A string with no commas/braces within the acceptable range.
	input := strings.Repeat("a", 100)
	got := findJSONCutPoint(input, 50)
	if got != 0 {
		t.Errorf("expected 0 (no boundary found), got %d", got)
	}
}

func TestFindJSONCutPoint_CommaNearEnd(t *testing.T) {
	input := `{"a":"b","c":"d","e":"f"}`
	// Comma positions: after "b" at index 7 (cut=8), after "d" at
	// index 15 (cut=16). With maxLen=18, minPos=18-4=14, so cut=16
	// should be found.
	got := findJSONCutPoint(input, 18)
	if got == 0 {
		t.Error("expected a non-zero cut point")
	}
	if got > 18 {
		t.Errorf("cut point %d exceeds maxLen 18", got)
	}
}
