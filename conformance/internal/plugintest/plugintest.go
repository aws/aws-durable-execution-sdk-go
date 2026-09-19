// Package plugintest holds the assertions the plugin handler tests share.
package plugintest

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
)

// buffer is a goroutine-safe bytes.Buffer.
type buffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *buffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// RecordTo redirects e into a buffer and returns a function that decodes
// everything emitted so far. Handler tests call it before running the
// handler under the local runner.
func RecordTo(t *testing.T, e *pluginlog.Emitter) func() []pluginlog.Record {
	t.Helper()
	buf := &buffer{}
	e.SetWriter(buf)
	return func() []pluginlog.Record {
		recs, err := pluginlog.Parse(strings.NewReader(buf.String()))
		if err != nil {
			t.Fatalf("parse plugin records: %v", err)
		}
		return recs
	}
}

// ExpectCount fails the test unless exactly want records match matcher.
func ExpectCount(t *testing.T, records []pluginlog.Record, matcher map[string]any, want int) {
	t.Helper()
	if got := pluginlog.Count(records, matcher); got != want {
		t.Errorf("%s: got %d matching records, want %d\nrecords:\n%s", describe(matcher), got, want, dump(records))
	}
}

// ExpectMinCount fails the test unless at least want records match
// matcher.
func ExpectMinCount(t *testing.T, records []pluginlog.Record, matcher map[string]any, want int) {
	t.Helper()
	if got := pluginlog.Count(records, matcher); got < want {
		t.Errorf("%s: got %d matching records, want at least %d\nrecords:\n%s", describe(matcher), got, want, dump(records))
	}
}

// ExpectAfter fails the test unless every record matching matcher appears
// after every record matching anchor, and both match at least once.
func ExpectAfter(t *testing.T, records []pluginlog.Record, matcher, anchor map[string]any) {
	t.Helper()
	lastAnchor := -1
	firstMatch := -1
	for i, r := range records {
		if pluginlog.Matches(r, anchor) {
			lastAnchor = i
		}
		if pluginlog.Matches(r, matcher) && firstMatch < 0 {
			firstMatch = i
		}
	}
	if lastAnchor < 0 || firstMatch < 0 {
		t.Errorf("%s after %s: anchor or match absent\nrecords:\n%s", describe(matcher), describe(anchor), dump(records))
		return
	}
	if firstMatch < lastAnchor {
		t.Errorf("%s appears before %s\nrecords:\n%s", describe(matcher), describe(anchor), dump(records))
	}
}

// ExpectAllStamped fails the test when a record lacks the
// durableExecutionArn field, or when the records do not all carry the same
// ARN.
func ExpectAllStamped(t *testing.T, records []pluginlog.Record) {
	t.Helper()
	if len(records) == 0 {
		t.Error("no plugin records emitted")
		return
	}
	first, _ := records[0]["durableExecutionArn"].(string)
	if !strings.HasPrefix(first, "arn:") {
		t.Errorf("first record carries no execution ARN: %v", records[0])
	}
	for _, r := range records {
		if got, _ := r["durableExecutionArn"].(string); got != first {
			t.Errorf("record stamped with %q, want %q: %v", got, first, r)
		}
	}
}

func describe(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, m[k]))
	}
	return strings.Join(parts, " ")
}

func dump(records []pluginlog.Record) string {
	var sb strings.Builder
	for _, r := range records {
		sb.WriteString("  ")
		sb.WriteString(describe(r))
		sb.WriteString("\n")
	}
	return sb.String()
}
