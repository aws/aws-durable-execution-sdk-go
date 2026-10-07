// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	smithy "github.com/aws/smithy-go"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

var input = Input{OrderID: "ORD-12345"}

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	out, err := durabletest.ResultAs[Output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	if out != (Output{OrderID: "ORD-12345", PaidBy: "invoice"}) {
		t.Errorf("unexpected output %+v", out)
	}

	extest.AssertSignature(t, result, extest.Ordered)
}

// fakeService stands in for the Lambda service behind serviceClient. It
// accepts every checkpoint and records its updates in the AWS SDK's types,
// or, with failWith set, rejects every checkpoint with that error.
type fakeService struct {
	mu       sync.Mutex
	failWith error
	calls    int
	updates  []types.OperationUpdate
}

func (f *fakeService) GetDurableExecutionState(context.Context, *lambdasvc.GetDurableExecutionStateInput, ...func(*lambdasvc.Options)) (*lambdasvc.GetDurableExecutionStateOutput, error) {
	return &lambdasvc.GetDurableExecutionStateOutput{}, nil
}

func (f *fakeService) CheckpointDurableExecution(_ context.Context, in *lambdasvc.CheckpointDurableExecutionInput, _ ...func(*lambdasvc.Options)) (*lambdasvc.CheckpointDurableExecutionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failWith != nil {
		return nil, f.failWith
	}
	f.updates = append(f.updates, in.Updates...)
	return &lambdasvc.CheckpointDurableExecutionOutput{CheckpointToken: aws.String(fmt.Sprintf("token-%d", f.calls))}, nil
}

// failedUpdates returns the FAIL updates the service received, keyed by
// operation name, with the ErrorType each recorded.
func (f *fakeService) failedUpdates() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]string{}
	for _, u := range f.updates {
		if u.Action == types.OperationActionFail && u.Error != nil {
			out[aws.ToString(u.Name)] = aws.ToString(u.Error.ErrorType)
		}
	}
	return out
}

// logRecords decodes the JSON log lines in buf whose message is msg.
func logRecords(t *testing.T, buf *bytes.Buffer, msg string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var r map[string]any
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		if r["msg"] == msg || r["message"] == msg {
			records = append(records, r)
		}
	}
	return records
}

// newRunner runs handler on serviceClient over svc. The injected client
// replaces the runner's in-memory client, so svc receives the checkpoints
// and the runner records no operations. Each invocation starts from the
// state the runner records, which is empty, so only a handler that
// finishes in one invocation runs to completion; this one does. Client
// records go to clientLog and handler records to handlerLog.
func newRunner(svc *fakeService, clientLog, handlerLog *bytes.Buffer) *durabletest.LocalRunner[Input, Output] {
	client := &serviceClient{api: svc, logger: slog.New(slog.NewJSONHandler(clientLog, nil))}
	return durabletest.NewLocalRunner(handler,
		durable.WithExecutionClient(client),
		durable.WithLogHandler(slog.NewJSONHandler(handlerLog, nil)))
}

// TestServiceClient runs the handler on serviceClient. The service
// receives both failures, and the client logs each one with the type
// ErrorFromObject rebuilt.
func TestServiceClient(t *testing.T) {
	svc := &fakeService{}
	var clientLog, handlerLog bytes.Buffer
	result, err := newRunner(svc, &clientLog, &handlerLog).RunUntilComplete(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	// The step records the error its body returned. The child context
	// records the StepError that escaped it.
	failed := svc.failedUpdates()
	if failed["charge-card"] != "Error" || failed["pay-by-card"] != "StepError" {
		t.Errorf("unexpected FAIL updates %v", failed)
	}

	records := logRecords(t, &clientLog, "recording failure")
	got := map[string]bool{}
	for _, r := range records {
		op, _ := r["operation"].(string)
		stepFailure, _ := r["stepFailure"].(bool)
		got[op] = stepFailure
	}
	// Only the child context's record rebuilds as a *durable.StepError.
	if len(records) != 2 || got["charge-card"] || !got["pay-by-card"] {
		t.Errorf("unexpected failure records %v", records)
	}

	// The runner's in-memory client was replaced, so it recorded no
	// operations: the golden is empty. The service recorded them instead.
	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.service-client.golden")
}

// TestCheckpointRejected has the service reject every checkpoint with a
// client fault, which the SDK classifies as execution-scoped: the
// execution fails with the CheckpointError and the handler logs the
// classification.
func TestCheckpointRejected(t *testing.T) {
	svc := &fakeService{failWith: &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized", Fault: smithy.FaultClient}}
	var clientLog, handlerLog bytes.Buffer
	result, err := newRunner(svc, &clientLog, &handlerLog).RunUntilComplete(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil || result.Error.Type != "CheckpointError" {
		t.Errorf("expected a CheckpointError, got %+v", result.Error)
	}

	records := logRecords(t, &handlerLog, "checkpoint failed, the execution will fail")
	if len(records) != 1 || records[0]["level"] != "ERROR" ||
		records[0]["scope"] != string(durable.ErrorScopeExecution) || records[0]["retryable"] != false {
		t.Errorf("unexpected handler records %v", records)
	}

	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.service-client.golden")
}

// TestCheckpointThrottled has the service throttle every checkpoint, which
// the SDK classifies as invocation-scoped and retryable: the invocation
// ends with the error, and the execution resumes in a new one.
func TestCheckpointThrottled(t *testing.T) {
	svc := &fakeService{failWith: &smithy.GenericAPIError{Code: "TooManyRequestsException", Message: "rate exceeded", Fault: smithy.FaultClient}}
	var clientLog, handlerLog bytes.Buffer
	runner := newRunner(svc, &clientLog, &handlerLog)

	// The local runner reports an invocation that ends with an error as a
	// runner error.
	_, err := runner.RunUntilComplete(input)
	if err == nil {
		t.Fatal("expected the invocation to end with an error")
	}
	if !durable.IsCheckpointRetryable(err) {
		t.Errorf("expected a retryable checkpoint failure, got %v", err)
	}

	records := logRecords(t, &handlerLog, "checkpoint failed, the execution resumes in a new invocation")
	if len(records) != 1 || records[0]["level"] != "WARN" ||
		records[0]["scope"] != string(durable.ErrorScopeInvocation) || records[0]["retryable"] != true {
		t.Errorf("unexpected handler records %v", records)
	}

	// The throttling clears. Invoking the execution again, as the service
	// would, completes it.
	svc.mu.Lock()
	svc.failWith = nil
	svc.mu.Unlock()
	result, err := runner.RunUntilComplete(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}

	extest.AssertSignatureFile(t, result, extest.Ordered, "testdata/signature.service-client.golden")
}
