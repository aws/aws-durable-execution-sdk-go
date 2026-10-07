package durable_test

// The SDK writes JSON to a checkpoint with <, > and & kept literal, not
// escaped as \u003c, \u003e and \u0026. Two tests verify it:
//   - TestJSONSerdesDoesNotHTMLEscape marshals a value through JSONSerdes and
//     asserts the output keeps < > & literal.
//   - TestStoredStepPayloadKeepsHTMLLiteral runs a step whose result contains
//     < > &, captures the step SUCCEED checkpoint payload with a fake
//     ExecutionClient, and asserts the stored payload keeps them literal.
// Both run without AWS.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// htmlValue marshals to {"html":"<b>a</b> & c >","nested":null}. It carries
// every HTML-escaped byte and a nil to prove nil stays null.
type htmlValue struct {
	HTML   string  `json:"html"`
	Nested *string `json:"nested"`
}

const rawHTML = `<b>a</b> & c >`

func assertNoHTMLEscape(t *testing.T, where, got string) {
	t.Helper()
	for _, esc := range []string{`\u003c`, `\u003e`, `\u0026`} {
		if strings.Contains(got, esc) {
			t.Errorf("%s: output HTML-escapes %q: %s", where, esc, got)
		}
	}
	if !strings.Contains(got, rawHTML) {
		t.Errorf("%s: output does not keep %q literal: %s", where, rawHTML, got)
	}
}

func TestJSONSerdesDoesNotHTMLEscape(t *testing.T) {
	v := htmlValue{HTML: rawHTML}
	out, err := durable.JSONSerdes.Marshal(context.Background(), durable.SerdesContext{}, v)
	if err != nil {
		t.Fatalf("JSONSerdes.Marshal: %v", err)
	}
	t.Logf("JSONSerdes output: %s", out)
	assertNoHTMLEscape(t, "JSONSerdes.Marshal", string(out))
	// Required invariants that must be preserved: nil stays null, map keys
	// stay sorted. Both already hold on main.
	if !strings.Contains(string(out), `"nested":null`) {
		t.Errorf("JSONSerdes.Marshal: nil field not encoded as null: %s", out)
	}
	m, err := durable.JSONSerdes.Marshal(context.Background(), durable.SerdesContext{},
		map[string]int{"b": 2, "a": 1, "c": 3})
	if err != nil {
		t.Fatalf("JSONSerdes.Marshal map: %v", err)
	}
	if string(m) != `{"a":1,"b":2,"c":3}` {
		t.Errorf("JSONSerdes.Marshal: map keys not sorted: %s", m)
	}
}

// captureClient is a fake ExecutionClient that records the step SUCCEED payload.
type captureClient struct {
	mu      sync.Mutex
	n       int
	payload string
	found   bool
}

func (c *captureClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, nil
}

func (c *captureClient) Checkpoint(_ context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, u := range in.Updates {
		if u.Type == durable.OperationTypeStep && u.Action == durable.OperationActionSucceed && u.Payload != nil {
			c.payload = *u.Payload
			c.found = true
		}
	}
	c.n++
	return durable.CheckpointOutput{CheckpointToken: "tok-" + string(rune('a'+c.n))}, nil
}

func htmlExecInput() []byte {
	b, _ := json.Marshal(map[string]any{
		"DurableExecutionArn": "arn:aws:lambda:us-west-2:account:function:fn:$LATEST/durable-execution/e/1",
		"CheckpointToken":     "tok-0",
		"InitialExecutionState": map[string]any{
			"Operations": []map[string]any{{
				"Id": "exec", "Type": "EXECUTION", "Status": "STARTED",
				"ExecutionDetails": map[string]any{"InputPayload": "{}"},
			}},
			"NextMarker": "",
		},
	})
	return b
}

func TestStoredStepPayloadKeepsHTMLLiteral(t *testing.T) {
	c := &captureClient{}
	// The step result type is htmlValue, so the handler output type is
	// htmlValue too.
	h := func(ctx durable.Context, _ struct{}) (htmlValue, error) {
		return durable.Step(ctx, "s", func(durable.StepContext) (htmlValue, error) {
			return htmlValue{HTML: rawHTML}, nil
		})
	}
	if _, err := durable.Wrap(h, durable.WithExecutionClient(c))(context.Background(), htmlExecInput()); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !c.found {
		t.Fatalf("no step SUCCEED payload captured")
	}
	t.Logf("stored step payload: %s", c.payload)
	assertNoHTMLEscape(t, "stored step SUCCEED payload", c.payload)
}
