package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// reportRule makes finishingClient report one recorded operation with a new
// status, in the response to the first checkpoint request that carries an
// update of type onType with action onAction (and name onName, when set).
// The reported operation is the one last updated under reportName, or, when
// reportName is empty, the one last updated with type reportType.
type reportRule struct {
	onType     durable.OperationType
	onAction   durable.OperationAction
	onName     string
	reportType durable.OperationType
	reportName string
	status     durable.OperationStatus
	fired      bool
}

// finishingClient is a fake ExecutionClient. Like the service, it reports
// an operation's new status in the response to a later checkpoint request.
type finishingClient struct {
	mu        sync.Mutex
	tokens    int
	callbacks int
	byType    map[durable.OperationType]durable.OperationUpdate
	byName    map[string]durable.OperationUpdate
	rules     []*reportRule
}

func (c *finishingClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, nil
}

func (c *finishingClient) Checkpoint(_ context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byType == nil {
		c.byType = map[durable.OperationType]durable.OperationUpdate{}
		c.byName = map[string]durable.OperationUpdate{}
	}
	var state []durable.Operation
	for _, u := range in.Updates {
		c.byType[u.Type] = u
		if u.Name != nil {
			c.byName[*u.Name] = u
		}
		if u.Type == durable.OperationTypeCallback && u.Action == durable.OperationActionStart {
			c.callbacks++ // the service returns the callback ID in the START response
			state = append(state, durable.Operation{
				Id: u.Id, Type: u.Type, SubType: u.SubType, Name: u.Name, ParentId: u.ParentId,
				Status:          durable.OperationStatusStarted,
				CallbackDetails: &durable.CallbackDetails{CallbackId: ptr(fmt.Sprintf("cb-%d", c.callbacks))},
			})
		}
		for _, r := range c.rules {
			if r.fired || u.Type != r.onType || u.Action != r.onAction || (r.onName != "" && (u.Name == nil || *u.Name != r.onName)) {
				continue
			}
			s, ok := c.byType[r.reportType]
			if r.reportName != "" {
				s, ok = c.byName[r.reportName]
			}
			if ok {
				r.fired = true
				state = append(state, finished(s, r.status, c.callbacks))
			}
		}
	}
	c.tokens++
	return durable.CheckpointOutput{CheckpointToken: fmt.Sprintf("tok-%d", c.tokens), NewExecutionState: state}, nil
}

func (c *finishingClient) reported() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.rules {
		if !r.fired {
			return false
		}
	}
	return true
}

func ptr(s string) *string { return &s }

// freshRules copies rules with their fired flags cleared. Several cases
// share one rule list, and a rule fires once per client.
func freshRules(rules []*reportRule) []*reportRule {
	out := make([]*reportRule, len(rules))
	for i, r := range rules {
		c := *r
		c.fired = false
		out[i] = &c
	}
	return out
}

// finished builds the record the service returns for the operation last
// updated as s once it has reached status.
func finished(s durable.OperationUpdate, status durable.OperationStatus, callbacks int) durable.Operation {
	op := durable.Operation{Id: s.Id, Type: s.Type, SubType: s.SubType, Name: s.Name, ParentId: s.ParentId, Status: status}
	switch s.Type {
	case durable.OperationTypeWait:
		op.WaitDetails = &durable.WaitDetails{}
	case durable.OperationTypeCallback:
		op.CallbackDetails = &durable.CallbackDetails{CallbackId: ptr(fmt.Sprintf("cb-%d", callbacks)), Result: ptr(`"approved"`)}
	case durable.OperationTypeChainedInvoke:
		op.ChainedInvokeDetails = &durable.ChainedInvokeDetails{Result: ptr(`"invoked"`)}
		if status == durable.OperationStatusFailed {
			op.ChainedInvokeDetails = &durable.ChainedInvokeDetails{Error: &durable.ErrorObject{
				ErrorType:    ptr("InvalidParameterValueException"),
				ErrorMessage: ptr("You cannot invoke a durable function using an unqualified ARN."),
			}}
		}
	case durable.OperationTypeStep:
		// A step due for its next attempt keeps the payload of its last
		// update. For a condition check that payload is the checkpointed state.
		op.StepDetails = &durable.StepDetails{Attempt: 1, Result: s.Payload}
	}
	return op
}

// runOnce runs one invocation and returns the status the handler answers.
func runOnce(h durable.Handler[struct{}, string], client durable.ExecutionClient) string {
	in, _ := json.Marshal(map[string]any{
		"DurableExecutionArn": "arn:aws:lambda:us-west-2:123:function:fn:$LATEST/durable-execution/e/1",
		"CheckpointToken":     "tok-0",
		"InitialExecutionState": map[string]any{
			"Operations": []map[string]any{{"Id": "exec", "Type": "EXECUTION", "Status": "STARTED",
				"ExecutionDetails": map[string]any{"InputPayload": "{}"}}},
			"NextMarker": "",
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := durable.Wrap(h, durable.WithExecutionClient(client))(ctx, in)
	if err != nil {
		return "INVOCATION_ERROR: " + err.Error()
	}
	var r struct{ Status string }
	_ = json.Unmarshal(out, &r)
	return r.Status
}

// longStep is a step named "work" that outlasts the operation the handler awaits.
func longStep(c durable.Context) error {
	_, err := durable.Step(c, "work", func(durable.StepContext) (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "worked", nil
	})
	return err
}

// awaitedFinishedCase is one case of
// TestAwaitedOperationFinishedDuringInvocation.
type awaitedFinishedCase struct {
	name    string
	rules   []*reportRule
	want    string
	handler durable.Handler[struct{}, string]
}

func TestAwaitedOperationFinishedDuringInvocation(t *testing.T) {
	for _, tc := range awaitedFinishedCases() {
		t.Run(tc.name, func(t *testing.T) {
			client := &finishingClient{rules: freshRules(tc.rules)}
			got := runOnce(tc.handler, client)
			if !client.reported() {
				t.Fatal("the fake client never reported the operation, so the case did not run as intended")
			}
			if got != tc.want {
				t.Errorf("status = %s, want %s", got, tc.want)
			}
		})
	}
}

// awaitedFinishedCases returns the ten cases of the reproduction.
func awaitedFinishedCases() []awaitedFinishedCase {
	onWorkDone := func(reportType durable.OperationType, reportName string, status durable.OperationStatus) []*reportRule {
		return []*reportRule{{onType: durable.OperationTypeStep, onAction: durable.OperationActionSucceed, onName: "work",
			reportType: reportType, reportName: reportName, status: status}}
	}
	waitDone := onWorkDone(durable.OperationTypeWait, "", durable.OperationStatusSucceeded)
	return []awaitedFinishedCase{
		{"01 WaitAsync, then a step, then Result", waitDone, "SUCCEEDED",
			func(ctx durable.Context, _ struct{}) (string, error) {
				pause := durable.WaitAsync(ctx, "pause", time.Second)
				if err := longStep(ctx); err != nil {
					return "", err
				}
				_, err := pause.Result(ctx)
				return "done", err
			}},
		{"02 WaitForCallback whose submitter completes the callback",
			[]*reportRule{{onType: durable.OperationTypeStep, onAction: durable.OperationActionSucceed,
				reportType: durable.OperationTypeCallback, status: durable.OperationStatusSucceeded}}, "SUCCEEDED",
			func(ctx durable.Context, _ struct{}) (string, error) {
				return durable.WaitForCallback[string](ctx, "approval", func(durable.StepContext, string) error { return nil })
			}},
		{"03 InvokeAsync, then a step, then Result",
			onWorkDone(durable.OperationTypeChainedInvoke, "", durable.OperationStatusSucceeded), "SUCCEEDED",
			func(ctx durable.Context, _ struct{}) (string, error) {
				inv := durable.InvokeAsync[string](ctx, "inv", "target:$LATEST", "x")
				time.Sleep(50 * time.Millisecond) // the invoke START reaches the client first
				if err := longStep(ctx); err != nil {
					return "", err
				}
				return inv.Result(ctx)
			}},
		{"04 Invoke whose START response already reports it failed",
			[]*reportRule{{onType: durable.OperationTypeChainedInvoke, onAction: durable.OperationActionStart,
				reportType: durable.OperationTypeChainedInvoke, status: durable.OperationStatusFailed}}, "FAILED",
			func(ctx durable.Context, _ struct{}) (string, error) {
				return durable.Invoke[string](ctx, "inv", "target", "x")
			}},
		{"05 Wait in a Parallel branch while another branch runs a step", waitDone, "SUCCEEDED",
			func(ctx durable.Context, _ struct{}) (string, error) {
				_, err := durable.Parallel(ctx, "p", []durable.Branch[string]{
					{Name: "short", Func: func(c durable.Context) (string, error) {
						return "waited", durable.Wait(c, "pause", time.Second)
					}},
					{Name: "long", Func: func(c durable.Context) (string, error) { return "worked", longStep(c) }},
				})
				return "done", err
			}},
		{"06 Wait in a Map item while another item runs a step", waitDone, "SUCCEEDED",
			func(ctx durable.Context, _ struct{}) (string, error) {
				_, err := durable.Map(ctx, "m", []string{"short", "long"},
					func(c durable.Context, item string, _ int) (string, error) {
						if item == "short" {
							return "waited", durable.Wait(c, "pause", time.Second)
						}
						return "worked", longStep(c)
					}, durable.WithMaxConcurrency(2))
				return "done", err
			}},
		{"07 Wait in a Go branch while the handler runs a step", waitDone, "SUCCEEDED",
			func(ctx durable.Context, _ struct{}) (string, error) {
				bg := durable.Go(ctx, "bg", func(c durable.Context) (string, error) {
					return "waited", durable.Wait(c, "pause", time.Second)
				})
				time.Sleep(50 * time.Millisecond) // the wait START reaches the client first
				if err := longStep(ctx); err != nil {
					return "", err
				}
				return bg.Result(ctx)
			}},
		{"08 Join over a WaitAsync and a StepAsync", waitDone, "SUCCEEDED",
			func(ctx durable.Context, _ struct{}) (string, error) {
				pause := durable.WaitAsync(ctx, "pause", time.Second)
				work := durable.StepAsync(ctx, "work", func(durable.StepContext) (string, error) {
					time.Sleep(200 * time.Millisecond)
					return "worked", nil
				})
				if err := durable.Join(ctx, "both", []durable.Awaitable{pause, work}); err != nil {
					return "", err
				}
				return work.Result(ctx)
			}},
		{"09 StepAsync retry whose next attempt becomes due while another step runs",
			onWorkDone("", "flaky", durable.OperationStatusReady), "SUCCEEDED",
			func(ctx durable.Context, _ struct{}) (string, error) {
				strategy, err := durable.NewRetryStrategy(durable.RetryConfig{MaxAttempts: 2, InitialDelay: time.Second, Jitter: durable.JitterNone})
				if err != nil {
					return "", err
				}
				flaky := durable.StepAsync(ctx, "flaky", func(sc durable.StepContext) (string, error) {
					if sc.Attempt() == 1 {
						return "", errors.New("transient")
					}
					return "recovered", nil
				}, durable.WithRetry(strategy))
				time.Sleep(50 * time.Millisecond) // attempt 1 and its RETRY reach the client first
				if err := longStep(ctx); err != nil {
					return "", err
				}
				return flaky.Result(ctx)
			}},
		{"10 WaitForCondition whose next check becomes due while a step runs",
			onWorkDone("", "until-ready", durable.OperationStatusReady), "SUCCEEDED",
			func(ctx durable.Context, _ struct{}) (string, error) {
				poll := durable.Go(ctx, "poll", func(c durable.Context) (int, error) {
					return durable.WaitForCondition(c, "until-ready",
						func(_ durable.StepContext, n int) (int, error) { return n + 1, nil },
						durable.ConditionConfig[int]{InitialState: 0, WaitStrategy: func(n int, _ int) durable.WaitDecision {
							return durable.WaitDecision{Continue: n < 2, Delay: time.Second}
						}})
				})
				time.Sleep(50 * time.Millisecond) // the first check and its RETRY reach the client first
				if err := longStep(ctx); err != nil {
					return "", err
				}
				n, err := poll.Result(ctx)
				return fmt.Sprint(n), err
			}},
	}
}
