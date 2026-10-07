package durable_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	smithy "github.com/aws/smithy-go"
)

// The service applies a size limit to each kind of checkpoint payload. It
// rejects a larger payload with an InvalidParameterValueException. These
// limits and messages are the ones the service returned in us-west-2.
const (
	serviceStepPayloadLimit = 262144  // a step result or a WaitForCondition state
	serviceInvokeInputLimit = 1048576 // a chained invoke input

	serviceStepPayloadMessage = "STEP output payload size must be less than or equal to 262144 bytes."
	serviceInvokeInputMessage = "CHAINED_INVOKE input payload size must be less than or equal to 1048576 bytes."
)

// serviceLimitClient is an ExecutionClient that applies the service's
// payload limits. It accepts every checkpoint the service accepts. It
// rejects a payload over the limit for its kind with the service's error.
type serviceLimitClient struct {
	mu       sync.Mutex
	largest  int    // size of the largest payload sent
	rejected string // message of the rejection, if any
}

func (c *serviceLimitClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, nil
}

func (c *serviceLimitClient) Checkpoint(_ context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, u := range in.Updates {
		size := len(aws.ToString(u.Payload))
		c.largest = max(c.largest, size)
		msg := ""
		switch {
		case u.Type == durable.OperationTypeStep && size > serviceStepPayloadLimit:
			msg = serviceStepPayloadMessage
		case u.Type == durable.OperationTypeChainedInvoke && size > serviceInvokeInputLimit:
			msg = serviceInvokeInputMessage
		}
		if msg != "" {
			c.rejected = msg
			return durable.CheckpointOutput{}, &smithy.GenericAPIError{
				Code:    "InvalidParameterValueException",
				Message: msg,
				Fault:   smithy.FaultClient,
			}
		}
	}
	return durable.CheckpointOutput{CheckpointToken: "tok-next"}, nil
}

func (c *serviceLimitClient) sent() (largest int, rejected string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.largest, c.rejected
}

// invocationResponse holds the fields of an invocation's response that the
// tests read.
type invocationResponse struct {
	Status string
	Error  *struct {
		ErrorType    string
		ErrorMessage string
	}
}

// invokeOnce runs the first invocation of a new execution against client
// and returns the parsed response.
func invokeOnce[O any](t *testing.T, h durable.Handler[struct{}, O], client durable.ExecutionClient) invocationResponse {
	t.Helper()
	event, err := json.Marshal(map[string]any{
		"DurableExecutionArn": "execution-1",
		"CheckpointToken":     "tok-0",
		"InitialExecutionState": map[string]any{
			"Operations": []map[string]any{{
				"Id": "exec", "Type": "EXECUTION", "Status": "STARTED",
				"ExecutionDetails": map[string]any{"InputPayload": "{}"},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := durable.Wrap(h, durable.WithExecutionClient(client))(ctx, event)
	if err != nil {
		t.Fatalf("invocation returned an error: %v", err)
	}
	var r invocationResponse
	if err := json.Unmarshal(out, &r); err != nil {
		t.Fatalf("response %s: %v", out, err)
	}
	return r
}

// The SDK applies no size limit of its own to an invoke input. A
// 900000-byte input is under the service's invoke input limit, so the
// service accepts the START checkpoint. The execution then waits for the
// target.
func TestInvokeInputUnderServiceLimitIsSent(t *testing.T) {
	client := &serviceLimitClient{}
	input := strings.Repeat("x", 900000)
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.Invoke[string](ctx, "call", "target:$LATEST", input)
	}

	r := invokeOnce(t, h, client)

	if r.Status != "PENDING" {
		t.Fatalf("status = %s, error = %+v; want PENDING", r.Status, r.Error)
	}
	// The serialized input is a JSON string, two quote bytes longer.
	if largest, _ := client.sent(); largest != len(input)+2 {
		t.Errorf("largest payload sent = %d bytes, want %d", largest, len(input)+2)
	}
}

// The SDK applies no size limit of its own to a step result, a
// WaitForCondition state, or an invoke input. It sends each payload as it
// is. The service rejects a payload over its limit. The rejection fails the
// execution, so the invocation responds FAILED even though the handler
// discards the operation's error and returns a result. The operation's
// function runs once.
func TestPayloadOverServiceLimitFailsExecution(t *testing.T) {
	// 819200 bytes is over the service's step limit and over the SDK's
	// 768000-byte checkpoint batch limit.
	big := strings.Repeat("x", 819200)
	check := func(runs *atomic.Int64) func(durable.StepContext, string) (string, error) {
		return func(durable.StepContext, string) (string, error) {
			runs.Add(1)
			return big, nil
		}
	}
	tests := []struct {
		name     string
		run      func(ctx durable.Context, runs *atomic.Int64) error
		wantMsg  string
		wantRuns int64
	}{
		{
			name: "step result",
			run: func(ctx durable.Context, runs *atomic.Int64) error {
				_, err := durable.Step(ctx, "big", func(durable.StepContext) (string, error) {
					runs.Add(1)
					return big, nil
				})
				return err
			},
			wantMsg:  serviceStepPayloadMessage,
			wantRuns: 1,
		},
		{
			name: "condition state when the strategy continues",
			run: func(ctx durable.Context, runs *atomic.Int64) error {
				_, err := durable.WaitForCondition(ctx, "poll", check(runs), durable.ConditionConfig[string]{
					WaitStrategy: func(string, int) durable.WaitDecision {
						return durable.WaitDecision{Continue: true, Delay: time.Second}
					},
				})
				return err
			},
			wantMsg:  serviceStepPayloadMessage,
			wantRuns: 1,
		},
		{
			name: "condition state when the strategy stops",
			run: func(ctx durable.Context, runs *atomic.Int64) error {
				_, err := durable.WaitForCondition(ctx, "poll", check(runs), durable.ConditionConfig[string]{
					WaitStrategy: func(string, int) durable.WaitDecision {
						return durable.WaitDecision{Continue: false}
					},
				})
				return err
			},
			wantMsg:  serviceStepPayloadMessage,
			wantRuns: 1,
		},
		{
			name: "invoke input",
			run: func(ctx durable.Context, _ *atomic.Int64) error {
				_, err := durable.Invoke[string](ctx, "call", "target:$LATEST", strings.Repeat("x", 1100000))
				return err
			},
			wantMsg:  serviceInvokeInputMessage,
			wantRuns: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &serviceLimitClient{}
			var runs atomic.Int64
			h := func(ctx durable.Context, _ struct{}) (string, error) {
				_ = tc.run(ctx, &runs)
				return "caught", nil
			}

			r := invokeOnce(t, h, client)

			if _, rejected := client.sent(); rejected != tc.wantMsg {
				t.Errorf("service rejection = %q, want %q", rejected, tc.wantMsg)
			}
			if r.Status != "FAILED" || r.Error == nil {
				t.Fatalf("status = %s, error = %+v; want FAILED", r.Status, r.Error)
			}
			if r.Error.ErrorType != "CheckpointError" || !strings.Contains(r.Error.ErrorMessage, tc.wantMsg) {
				t.Errorf("error = %s %q; want CheckpointError with the service's message", r.Error.ErrorType, r.Error.ErrorMessage)
			}
			if got := runs.Load(); got != tc.wantRuns {
				t.Errorf("function ran %d times, want %d", got, tc.wantRuns)
			}
		})
	}
}
