package durable_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// recordingClient is a fake ExecutionClient that answers every checkpoint
// and records each update. Like the service, it returns the callback ID in
// the response to a callback START. It reports no other change.
type recordingClient struct {
	mu        sync.Mutex
	tokens    int
	callbacks int
	updates   []durable.OperationUpdate
	ids       map[string]string // callback operation ID → callback ID
}

func (c *recordingClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, nil
}

func (c *recordingClient) Checkpoint(_ context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ids == nil {
		c.ids = map[string]string{}
	}
	var state []durable.Operation
	for _, u := range in.Updates {
		c.updates = append(c.updates, u)
		if u.Type == durable.OperationTypeCallback && u.Action == durable.OperationActionStart {
			c.callbacks++
			id := fmt.Sprintf("cb-%d", c.callbacks)
			c.ids[*u.Id] = id
			state = append(state, durable.Operation{
				Id: u.Id, Type: u.Type, SubType: u.SubType, Name: u.Name, ParentId: u.ParentId,
				Status:          durable.OperationStatusStarted,
				CallbackDetails: &durable.CallbackDetails{CallbackId: ptr(id)},
			})
		}
	}
	c.tokens++
	return durable.CheckpointOutput{CheckpointToken: fmt.Sprintf("tok-%d", c.tokens), NewExecutionState: state}, nil
}

// replayRecord is the record the service holds for one operation after the
// updates a recordingClient saw.
type replayRecord struct {
	update   durable.OperationUpdate // the last update
	status   string
	attempts int
	payload  *string
}

// records folds the recorded updates into one record per operation, in
// order of first appearance.
func (c *recordingClient) records() ([]string, map[string]*replayRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var order []string
	recs := map[string]*replayRecord{}
	for _, u := range c.updates {
		id := *u.Id
		r := recs[id]
		if r == nil {
			r = &replayRecord{}
			recs[id] = r
			order = append(order, id)
		}
		r.update = u
		switch u.Action {
		case durable.OperationActionStart:
			r.status = "STARTED"
			r.attempts++
		case durable.OperationActionSucceed:
			r.status = "SUCCEEDED"
		case durable.OperationActionFail:
			r.status = "FAILED"
		case durable.OperationActionRetry:
			r.status = "PENDING"
		}
		if u.Payload != nil {
			r.payload = u.Payload
		}
	}
	return order, recs
}

// initialOperations returns the wire records of the operations recorded by
// c, without the operations that the rule trigger matches and without
// their terminal ancestors, so that in a replay the trigger runs live while
// the awaited operation starts out in the initial state as not terminal.
func initialOperations(c *recordingClient, trigger *reportRule) []map[string]any {
	order, recs := c.records()
	drop := map[string]bool{}
	for _, id := range order {
		r := recs[id]
		u := r.update
		if u.Type != trigger.onType || (trigger.onName != "" && (u.Name == nil || *u.Name != trigger.onName)) {
			continue
		}
		if trigger.onAction == durable.OperationActionSucceed && r.status != "SUCCEEDED" {
			continue
		}
		drop[id] = true
		for p := u.ParentId; p != nil; {
			pr := recs[*p]
			if pr == nil || (pr.status != "SUCCEEDED" && pr.status != "FAILED") {
				break
			}
			drop[*p] = true
			p = pr.update.ParentId
		}
	}
	var ops []map[string]any
	for _, id := range order {
		if drop[id] {
			continue
		}
		r := recs[id]
		u := r.update
		op := map[string]any{"Id": id, "Type": string(u.Type), "Status": r.status}
		if u.SubType != nil {
			op["SubType"] = *u.SubType
		}
		if u.Name != nil {
			op["Name"] = *u.Name
		}
		if u.ParentId != nil {
			op["ParentId"] = *u.ParentId
		}
		switch u.Type {
		case durable.OperationTypeStep:
			d := map[string]any{"Attempt": r.attempts}
			if r.payload != nil {
				d["Result"] = *r.payload
			}
			op["StepDetails"] = d
		case durable.OperationTypeCallback:
			op["CallbackDetails"] = map[string]any{"CallbackId": c.ids[id]}
		case durable.OperationTypeContext:
			if r.payload != nil {
				op["ContextDetails"] = map[string]any{"Result": *r.payload}
			}
		}
		ops = append(ops, op)
	}
	return ops
}

// seed records every update of c as last seen by f, so a rule of f can
// report an operation that was started before f's invocation.
func seed(f *finishingClient, c *recordingClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f.byType = map[durable.OperationType]durable.OperationUpdate{}
	f.byName = map[string]durable.OperationUpdate{}
	for _, u := range c.updates {
		f.byType[u.Type] = u
		if u.Name != nil {
			f.byName[*u.Name] = u
		}
	}
	f.callbacks = c.callbacks
	f.tokens = c.tokens
}

// notTerminalIn reports whether ops holds the operation id with status
// STARTED or PENDING.
func notTerminalIn(ops []map[string]any, id string) bool {
	for _, op := range ops {
		if op["Id"] == id {
			return op["Status"] == "STARTED" || op["Status"] == "PENDING"
		}
	}
	return false
}

// runWithState runs one invocation whose initial state holds ops after the
// execution operation, and returns the status the handler answers.
func runWithState(h durable.Handler[struct{}, string], client durable.ExecutionClient, ops []map[string]any) string {
	all := append([]map[string]any{{"Id": "exec", "Type": "EXECUTION", "Status": "STARTED",
		"ExecutionDetails": map[string]any{"InputPayload": "{}"}}}, ops...)
	in, _ := json.Marshal(map[string]any{
		"DurableExecutionArn":   "arn:aws:lambda:us-west-2:123:function:fn:$LATEST/durable-execution/e/1",
		"CheckpointToken":       "tok-r",
		"InitialExecutionState": map[string]any{"Operations": all, "NextMarker": ""},
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

// TestAwaitedOperationNotTerminalInInitialState runs each case of
// TestAwaitedOperationFinishedDuringInvocation with the awaited operation
// already in the initial execution state, not terminal: a STARTED wait,
// callback, or invoke, or a step retry or condition check in PENDING. A
// first invocation that reports nothing records the operations; the second
// starts from those records, minus the operation whose checkpoint triggers
// the report, so that operation runs live and its checkpoint response
// reports the awaited operation finished. The second invocation must
// return the listed status.
//
// Case 04 awaits an invoke whose START response reports the failure. A
// replayed invoke sends no START, so its variant awaits the invoke in a Go
// branch while the handler runs a step, and the step's checkpoint response
// reports the failure.
func TestAwaitedOperationNotTerminalInInitialState(t *testing.T) {
	onWorkDone := func(reportType durable.OperationType, reportName string, status durable.OperationStatus) []*reportRule {
		return []*reportRule{{onType: durable.OperationTypeStep, onAction: durable.OperationActionSucceed, onName: "work",
			reportType: reportType, reportName: reportName, status: status}}
	}
	cases := map[string]struct {
		rules   []*reportRule
		want    string
		handler durable.Handler[struct{}, string]
	}{
		"04 Invoke replayed STARTED in a Go branch while the handler runs a step": {
			onWorkDone(durable.OperationTypeChainedInvoke, "", durable.OperationStatusFailed), "FAILED",
			func(ctx durable.Context, _ struct{}) (string, error) {
				inv := durable.Go(ctx, "bg", func(c durable.Context) (string, error) {
					return durable.Invoke[string](c, "inv", "target", "x")
				})
				time.Sleep(50 * time.Millisecond) // the invoke START reaches the client first
				if err := longStep(ctx); err != nil {
					return "", err
				}
				return inv.Result(ctx)
			},
		},
	}
	// The other nine cases reuse the reproduction's handlers and rules.
	for _, tc := range awaitedFinishedCases() {
		if tc.name[:2] == "04" {
			continue
		}
		cases[tc.name] = struct {
			rules   []*reportRule
			want    string
			handler durable.Handler[struct{}, string]
		}{tc.rules, tc.want, tc.handler}
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			first := &recordingClient{}
			if got := runOnce(tc.handler, first); got != "PENDING" {
				t.Fatalf("first invocation status = %s, want PENDING", got)
			}
			ops := initialOperations(first, tc.rules[0])
			client := &finishingClient{rules: freshRules(tc.rules)}
			seed(client, first)
			// The operation the rule reports is in the initial state and
			// not terminal there.
			r := tc.rules[0]
			awaited, ok := client.byType[r.reportType]
			if r.reportName != "" {
				awaited, ok = client.byName[r.reportName]
			}
			if !ok || !notTerminalIn(ops, *awaited.Id) {
				t.Fatalf("the awaited operation is not in the initial state as not terminal: %v", ops)
			}
			got := runWithState(tc.handler, client, ops)
			if !client.reported() {
				t.Fatal("the fake client never reported the operation, so the case did not run as intended")
			}
			if got != tc.want {
				t.Errorf("status = %s, want %s", got, tc.want)
			}
		})
	}
}
