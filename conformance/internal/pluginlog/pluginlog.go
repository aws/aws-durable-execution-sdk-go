// Package pluginlog writes the JSON records that the plugin conformance
// suite asserts on.
//
// The conformance runner reads a function's CloudWatch log group and
// keeps only the records whose top-level durableExecutionArn (or
// executionArn) field names the execution under test. It then matches each
// expectation against the top-level fields of those records. So a plugin
// writes each record as one raw JSON object per line, and stamps every
// record with the execution ARN it captured from the invocation-start
// hook. The Lambda runtime forwards a line that is already a JSON object
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

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// None is the literal the suite expects in place of an absent value.
const None = "NONE"

// Emitter writes plugin records. It is safe for concurrent use: plugin
// hooks of concurrent branches run on different goroutines.
type Emitter struct {
	mu  sync.Mutex
	w   io.Writer
	arn string
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

// Capture stores the execution ARN that later records are stamped with.
// Call it from OnInvocationStart with the ARN the hook info carries.
func (e *Emitter) Capture(arn string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.arn = arn
}

// Emit writes record as one JSON line, adding durableExecutionArn when an
// ARN has been captured. The field is omitted when no ARN is known; the
// Emitter never invents a value.
func (e *Emitter) Emit(record map[string]any) {
	e.mu.Lock()
	arn := e.arn
	e.mu.Unlock()
	e.EmitWithArn(record, arn)
}

// EmitWithArn writes record as one JSON line stamped with arn. An empty arn
// omits the field.
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
