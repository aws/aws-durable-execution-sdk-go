package pluginlog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Record is one emitted JSON line, decoded. Numbers decode as float64.
type Record map[string]any

// Parse decodes the JSON lines in r. A line that is not a JSON object is
// returned as a Record with a single "message" field, the way the
// conformance runner treats a plain log line.
func Parse(r io.Reader) ([]Record, error) {
	var out []Record
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec Record
		if strings.HasPrefix(line, "{") && json.Unmarshal([]byte(line), &rec) == nil {
			out = append(out, rec)
			continue
		}
		out = append(out, Record{"message": line})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("pluginlog: scan records: %w", err)
	}
	return out, nil
}

// Matches reports whether every field of matcher is present in rec with an
// equal value. Strings compare exactly, booleans by identity, numbers by
// value, the same rules the conformance runner applies.
func Matches(rec Record, matcher map[string]any) bool {
	for k, want := range matcher {
		got, ok := rec[k]
		if !ok || !valueEqual(want, got) {
			return false
		}
	}
	return true
}

func valueEqual(want, got any) bool {
	switch w := want.(type) {
	case string:
		return strings.TrimSpace(fmt.Sprint(got)) == strings.TrimSpace(w)
	case bool:
		g, ok := got.(bool)
		return ok && g == w
	case int:
		return numberEqual(float64(w), got)
	case float64:
		return numberEqual(w, got)
	}
	return fmt.Sprint(want) == fmt.Sprint(got)
}

func numberEqual(want float64, got any) bool {
	switch g := got.(type) {
	case float64:
		return g == want
	case int:
		return float64(g) == want
	}
	return false
}

// Count returns how many records match matcher.
func Count(records []Record, matcher map[string]any) int {
	n := 0
	for _, r := range records {
		if Matches(r, matcher) {
			n++
		}
	}
	return n
}

// First returns the index of the first record that matches matcher, or -1.
func First(records []Record, matcher map[string]any) int {
	for i, r := range records {
		if Matches(r, matcher) {
			return i
		}
	}
	return -1
}
