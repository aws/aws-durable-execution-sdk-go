package durable

import (
	"context"
	"strings"
	"testing"
)

func TestDurableHandlerInvokeLifecycle(t *testing.T) {
	// Payload for a first invocation: execution operation only, carrying
	// the customer event.
	payload := `{
		"DurableExecutionArn": "arn:aws:lambda:us-west-2:123456789012:function:fn:1/durable-execution/abc",
		"CheckpointToken": "token-0",
		"InitialExecutionState": {
			"Operations": [{
				"Id": "1",
				"Status": "STARTED",
				"ExecutionDetails": {"InputPayload": "\"hello\""}
			}]
		}
	}`

	tests := []struct {
		name    string
		handler Handler[string, string]
		want    string
	}{
		{
			name: "handler result becomes SUCCEEDED with double-encoded result",
			handler: func(_ Context, event string) (string, error) {
				return event + " world", nil
			},
			want: `{"Status":"SUCCEEDED","Result":"\"hello world\""}`,
		},
		{
			name: "handler error becomes FAILED with error object",
			handler: func(_ Context, _ string) (string, error) {
				return "", &StepError{Name: "s", Attempts: 2, Err: context.DeadlineExceeded}
			},
			want: `{"Status":"FAILED","Error":{"ErrorType":"StepError","ErrorMessage":"durable: step \"s\" failed after 2 attempts: context deadline exceeded"}}`,
		},
		{
			name: "suspension becomes PENDING",
			handler: func(_ Context, _ string) (string, error) {
				return "", errSuspendExecution
			},
			want: `{"Status":"PENDING"}`,
		},
		{
			name: "handler panic becomes FAILED, not a hang",
			handler: func(_ Context, _ string) (string, error) {
				panic("boom")
			},
			want: `{"Status":"FAILED","Error":{"ErrorType":"Error","ErrorMessage":"durable: handler panicked: boom"}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := Wrap(tt.handler, withLambdaAPI(&fakeLambda{}))
			got, err := h(context.Background(), []byte(payload))
			if err != nil {
				t.Fatalf("Invoke() error: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("Invoke() = %s\nwant       %s", got, tt.want)
			}
		})
	}
}

func TestDurableHandlerReceivesEventAndContext(t *testing.T) {
	var gotEvent string
	var gotArn string
	var wasReplaying bool
	h := Wrap(func(ctx Context, event string) (string, error) {
		gotEvent = event
		gotArn = ctx.ExecutionArn()
		wasReplaying = ctx.IsReplaying()
		return "", nil
	}, withLambdaAPI(&fakeLambda{}))

	payload := `{
		"DurableExecutionArn": "arn:test",
		"CheckpointToken": "token-0",
		"InitialExecutionState": {
			"Operations": [{
				"Id": "1",
				"Status": "STARTED",
				"ExecutionDetails": {"InputPayload": "\"evt\""}
			}]
		}
	}`
	if _, err := h(context.Background(), []byte(payload)); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	if gotEvent != "evt" {
		t.Errorf("handler event = %q, want %q", gotEvent, "evt")
	}
	if gotArn != "arn:test" {
		t.Errorf("ExecutionArn() = %q, want %q", gotArn, "arn:test")
	}
	if wasReplaying {
		t.Error("IsReplaying() on first invocation = true, want false")
	}
}

func TestDurableHandlerAssemblesRemainingPages(t *testing.T) {
	// The embedded page points at marker "1"; the fake serves page 1 with
	// one more checkpointed operation, which must flip the context into
	// replay mode.
	fake := &fakeLambda{statePages: [][]Operation{
		{},
		{opWire(hashID("1"), OperationStatusSucceeded)},
	}}
	var wasReplaying bool
	h := Wrap(func(ctx Context, _ string) (string, error) {
		wasReplaying = ctx.IsReplaying()
		return "", nil
	}, withLambdaAPI(fake))

	payload := `{
		"DurableExecutionArn": "arn:test",
		"CheckpointToken": "token-0",
		"InitialExecutionState": {
			"Operations": [{
				"Id": "1",
				"Status": "STARTED",
				"ExecutionDetails": {"InputPayload": "\"evt\""}
			}],
			"NextMarker": "1"
		}
	}`
	if _, err := h(context.Background(), []byte(payload)); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	if !wasReplaying {
		t.Error("IsReplaying() with paged-in checkpointed operation = false, want true")
	}
}

func TestDurableHandlerInvokeRejectsBadInput(t *testing.T) {
	h := Wrap(func(_ Context, event string) (string, error) {
		return event, nil
	})

	tests := []struct {
		name    string
		payload string
		wantIn  string
	}{
		{
			name:    "malformed json",
			payload: `{`,
			wantIn:  "parse invocation input",
		},
		{
			name:    "missing durable fields",
			payload: `{"foo": "bar"}`,
			wantIn:  "DurableConfig",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := h(context.Background(), []byte(tt.payload))
			if err == nil || !strings.Contains(err.Error(), tt.wantIn) {
				t.Errorf("Invoke(%s) error = %v, want containing %q", tt.name, err, tt.wantIn)
			}
		})
	}
}

func TestInitialExecutionStateToOperations(t *testing.T) {
	in := initialExecutionState{
		Operations: []wireOperation{
			{Id: "1", Status: "SUCCEEDED"},
			{Id: "1-1", Status: "STARTED"},
		},
	}
	ops := in.toOperations()
	if len(ops) != 2 {
		t.Fatalf("toOperations() returned %d, want 2", len(ops))
	}
	if ops[0].id != "1" || ops[0].status != statusSucceeded {
		t.Errorf("ops[0] = %+v, want id 1 SUCCEEDED", ops[0])
	}
	if ops[1].id != "1-1" || ops[1].status != statusStarted {
		t.Errorf("ops[1] = %+v, want id 1-1 STARTED", ops[1])
	}
}

func TestInitialExecutionStateCustomerInput(t *testing.T) {
	tests := []struct {
		name   string
		state  initialExecutionState
		want   string
		wantOK bool
	}{
		{
			name: "input payload on the execution operation",
			state: initialExecutionState{Operations: []wireOperation{
				{Id: "1", Status: "STARTED", ExecutionDetails: &wireExecutionDetails{InputPayload: `{"a":1}`}},
			}},
			want:   `{"a":1}`,
			wantOK: true,
		},
		{
			name:   "no operations",
			state:  initialExecutionState{},
			wantOK: false,
		},
		{
			name: "operation without execution details",
			state: initialExecutionState{Operations: []wireOperation{
				{Id: "1", Status: "STARTED"},
			}},
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.state.customerInput()
			if ok != tt.wantOK {
				t.Fatalf("customerInput() ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("customerInput() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRespondShapes(t *testing.T) {
	result := `{"ok":true}`
	tests := []struct {
		name string
		in   invocationResponse
		want string
	}{
		{
			name: "pending omits result and error",
			in:   invocationResponse{Status: invocationPending},
			want: `{"Status":"PENDING"}`,
		},
		{
			name: "succeeded carries pre-serialized result string",
			in:   invocationResponse{Status: invocationSucceeded, Result: &result},
			want: `{"Status":"SUCCEEDED","Result":"{\"ok\":true}"}`,
		},
		{
			name: "failed carries error object",
			in: invocationResponse{
				Status: invocationFailed,
				Error:  &wireError{ErrorType: "StepError", ErrorMessage: "boom"},
			},
			want: `{"Status":"FAILED","Error":{"ErrorType":"StepError","ErrorMessage":"boom"}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := respond(tt.in)
			if err != nil {
				t.Fatalf("respond() error: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("respond() = %s, want %s", got, tt.want)
			}
		})
	}
}
