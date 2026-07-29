// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// TestOperationsFromEventsMergesLifecycle verifies that multiple events for
// one operation fold into a single record carrying the latest status while
// retaining details contributed by earlier events, and that events without
// an operation ID are skipped.
func TestOperationsFromEventsMergesLifecycle(t *testing.T) {
	events := []types.Event{
		{
			EventType: types.EventTypeInvocationCompleted,
			// No Id: describes an invocation, not an operation.
		},
		{
			Id:        aws.String("cb-1"),
			Name:      aws.String("approval"),
			SubType:   aws.String("Callback"),
			EventType: types.EventTypeCallbackStarted,
			CallbackStartedDetails: &types.CallbackStartedDetails{
				CallbackId: aws.String("token-abc"),
			},
		},
		{
			Id:        aws.String("cb-1"),
			EventType: types.EventTypeCallbackSucceeded,
			CallbackSucceededDetails: &types.CallbackSucceededDetails{
				Result: &types.EventResult{Payload: aws.String(`"approved"`)},
			},
		},
	}

	ops := operationsFromEvents(events)

	if len(ops) != 1 {
		t.Fatalf("len(ops) = %d, want 1", len(ops))
	}
	op := ops[0]
	if aws.ToString(op.Id) != "cb-1" {
		t.Errorf("Id = %q, want cb-1", aws.ToString(op.Id))
	}
	if aws.ToString(op.Name) != "approval" {
		t.Errorf("Name = %q, want approval", aws.ToString(op.Name))
	}
	if op.Type != types.OperationTypeCallback {
		t.Errorf("Type = %q, want CALLBACK", op.Type)
	}
	if op.Status != types.OperationStatusSucceeded {
		t.Errorf("Status = %q, want SUCCEEDED", op.Status)
	}
	if op.CallbackDetails == nil {
		t.Fatal("CallbackDetails = nil, want populated")
	}
	if aws.ToString(op.CallbackDetails.CallbackId) != "token-abc" {
		t.Errorf("CallbackId = %q, want token-abc (from the started event)", aws.ToString(op.CallbackDetails.CallbackId))
	}
	if aws.ToString(op.CallbackDetails.Result) != `"approved"` {
		t.Errorf("Result = %q, want %q", aws.ToString(op.CallbackDetails.Result), `"approved"`)
	}
}
