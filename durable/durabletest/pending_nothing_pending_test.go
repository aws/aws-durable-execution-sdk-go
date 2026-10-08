// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// pendingRunner returns a runner whose handler answers PENDING on every
// invocation and records no operation. No SDK operation produces that
// response, so the test supplies the raw response itself.
func pendingRunner() *LocalRunner[struct{}, int] {
	r := NewLocalRunner(func(durable.Context, struct{}) (int, error) { return 0, nil })
	r.exec.handler = func(context.Context, []byte) ([]byte, error) {
		return []byte(`{"Status":"PENDING"}`), nil
	}
	return r
}

// The service rejects a PENDING response that reports no pending
// operation, and fails the execution after four of them in a row. The
// local runner does the same.
func TestPendingWithNothingPendingFailsAfterFourInvocations(t *testing.T) {
	result, err := pendingRunner().RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Failed {
		t.Fatalf("status = %s, want FAILED", result.Status)
	}
	if result.Error == nil || result.Error.Type != "InvalidParameterValueException" ||
		result.Error.Message != msgPendingWithNothingPending {
		t.Fatalf("error = %+v, want InvalidParameterValueException: %s", result.Error, msgPendingWithNothingPending)
	}
	if n := len(result.Invocations); n != 4 {
		t.Errorf("invocations = %d, want 4", n)
	}
	if result.CapReached {
		t.Error("CapReached = true; the runner spun to the cap instead of failing the execution")
	}
}
