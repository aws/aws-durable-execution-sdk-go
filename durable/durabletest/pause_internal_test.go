// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"errors"
	"testing"

	smithy "github.com/aws/smithy-go"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func isConflict(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == errCodeResourceConflict
}

// A repeated pause with the same token succeeds and changes nothing; a
// pause with another token, or of an ended execution, conflicts. Once
// paused, the next checkpoint is recorded and answered without a token.
func TestMemoryClientPauseExecution(t *testing.T) {
	m := newMemoryClient()
	ctx := context.Background()
	in := durable.PauseExecutionInput{ExecutionArn: "arn", ClientToken: "t1", Reason: "r"}
	if _, err := m.PauseExecution(ctx, in); err != nil {
		t.Fatalf("PauseExecution() error = %v", err)
	}
	if _, err := m.PauseExecution(ctx, in); err != nil {
		t.Errorf("repeated PauseExecution() error = %v, want nil", err)
	}
	in.ClientToken = "t2"
	if _, err := m.PauseExecution(ctx, in); !isConflict(err) {
		t.Errorf("PauseExecution() with another token error = %v, want ResourceConflictException", err)
	}
	out, err := m.Checkpoint(ctx, durable.CheckpointInput{CheckpointToken: m.currentToken()})
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}
	if out.CheckpointToken != "" || !m.withheldToken() {
		t.Errorf("checkpoint token = %q, want none while paused", out.CheckpointToken)
	}
	if err := m.resume(); err != nil {
		t.Fatalf("resume() error = %v", err)
	}
	if err := m.resume(); !isConflict(err) {
		t.Errorf("second resume() error = %v, want ResourceConflictException", err)
	}

	m.mu.Lock()
	m.executionEnded = true
	m.mu.Unlock()
	if _, err := m.PauseExecution(ctx, in); !isConflict(err) {
		t.Errorf("PauseExecution() of an ended execution error = %v, want ResourceConflictException", err)
	}
}
