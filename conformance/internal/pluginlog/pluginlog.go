// Package pluginlog writes the JSON records that the plugin conformance
// suite asserts on.
//
// The conformance runner reads a function's CloudWatch log group and
// keeps only the records whose top-level durableExecutionArn (or
// executionArn) field names the execution under test. It then matches each
// expectation against the top-level fields of those records. So a plugin
// writes each record as one raw JSON object per line, and stamps every
// record with the execution ARN its factory received in the invocation
// info. The Lambda runtime forwards a line that is already a JSON object
// unchanged, so the fields stay top-level. Records go to standard error,
// the stream the SDK logger writes to, so a plugin record and an SDK log
// record keep their relative order in the log group.
package pluginlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// None is the literal the suite expects in place of an absent value.
const None = "NONE"

// Emitter writes plugin records. It is safe for concurrent use: plugin
// hooks of concurrent branches run on different goroutines. The Emitter
// holds no execution state. Each record carries its own ARN, passed to
// EmitWithArn by the plugin whose factory received it, so overlapping
// invocations that share one Emitter cannot stamp each other's records.
type Emitter struct {
	mu sync.Mutex
	w  io.Writer
}

// New returns an Emitter that writes to w, or to standard error when w is
// nil.
func New(w io.Writer) *Emitter {
	if w == nil {
		w = os.Stderr
	}
	return &Emitter{w: w}
}

// SetWriter redirects the Emitter's output. Tests use it to capture
// records.
func (e *Emitter) SetWriter(w io.Writer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.w = w
}

// EmitWithArn writes record as one JSON line stamped with arn. An empty arn
// omits the field; the Emitter never invents a value.
func (e *Emitter) EmitWithArn(record map[string]any, arn string) {
	line := make(map[string]any, len(record)+1)
	for k, v := range record {
		line[k] = v
	}
	if arn != "" {
		line["durableExecutionArn"] = arn
	}
	b, err := json.Marshal(line)
	if err != nil {
		// Every value the handlers pass is a string, bool, or integer, so
		// this cannot happen; surface it rather than drop the record.
		b = []byte(fmt.Sprintf(`{"plugin":"CONFPLUGIN","error":%q}`, err.Error()))
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	_, _ = e.w.Write(append(b, '\n'))
}

// Upper returns s in upper case, the form the suite uses for operation type
// tokens.
func Upper(s string) string { return strings.ToUpper(s) }

// OrNone returns s, or None when s is empty.
func OrNone(s string) string {
	if s == "" {
		return None
	}
	return s
}

// Terminal reports whether status is a terminal invocation status.
func Terminal(status durable.PluginInvocationStatus) bool {
	return status == durable.PluginInvocationSucceeded || status == durable.PluginInvocationFailed
}

// OperationTerminal reports whether status is a terminal operation status.
func OperationTerminal(status durable.PluginOperationStatus) bool {
	switch status {
	case durable.PluginOperationSucceeded, durable.PluginOperationFailed,
		durable.PluginOperationTimedOut, durable.PluginOperationStopped,
		durable.PluginOperationCancelled:
		return true
	}
	return false
}

// ErrorMessage returns the message the suite expects for err: None for a
// nil error, else the message the operation recorded. A [durable.StepError]
// wraps the step body's error; its Message field is that error's message.
func ErrorMessage(err error) string {
	if err == nil {
		return None
	}
	var stepErr *durable.StepError
	if errors.As(err, &stepErr) {
		return stepErr.Message
	}
	return err.Error()
}

// Outcome returns the SUCCEEDED or FAILED token for a terminal operation
// status.
func Outcome(status durable.PluginOperationStatus) string {
	if status == durable.PluginOperationSucceeded {
		return string(durable.PluginAttemptSucceeded)
	}
	return string(durable.PluginAttemptFailed)
}

// InfoRecord returns the canonical dump of info that the field-shape
// requirements (10-20 and later) assert on: every field of
// OperationHookInfo under its camelCase name, with hook as the record's
// hook. A field the SDK left unset is omitted rather than written as a
// zero value, because the requirements assert the presence of each key:
// an absent key is the signal that the info lacks the field. A string
// field is written when non-empty, a time when non-zero, the attempt when
// positive, the error when non-nil. Type and status tokens are written as
// the SDK reports them; the suite upper-cases both, and the Go tokens are
// already upper case. Timestamps are RFC 3339 in UTC.
func InfoRecord(hook string, info durable.OperationHookInfo) map[string]any {
	rec := map[string]any{
		"plugin":   "CONFPLUGIN",
		"hook":     hook,
		"id":       info.ID,
		"type":     Upper(info.Type),
		"isReplay": info.IsReplay,
	}
	if info.Name != "" {
		rec["name"] = info.Name
	}
	if info.SubType != "" {
		rec["subType"] = info.SubType
	}
	if info.ParentID != "" {
		rec["parentId"] = info.ParentID
	}
	if info.Status != "" {
		rec["status"] = string(info.Status)
	}
	if !info.StartTimestamp.IsZero() {
		rec["startTimestamp"] = info.StartTimestamp.UTC().Format(time.RFC3339Nano)
	}
	if !info.EndTimestamp.IsZero() {
		rec["endTimestamp"] = info.EndTimestamp.UTC().Format(time.RFC3339Nano)
	}
	if info.Result != "" {
		rec["result"] = info.Result
	}
	if info.Error != nil {
		rec["error"] = ErrorMessage(info.Error)
	}
	if info.Attempt > 0 {
		rec["attempt"] = info.Attempt
	}
	return rec
}
