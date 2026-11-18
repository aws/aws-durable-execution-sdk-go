// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Compile-time check that memoryClient can pause an execution.
var _ durable.ExecutionPauser = (*memoryClient)(nil)

// errCodeResourceConflict is the error code the service returns for a
// pause of an execution that has closed or is already paused, and for a
// resume of an execution that is not paused.
const errCodeResourceConflict = "ResourceConflictException"

// pendingResponse is the invocation response the runner reports for a
// registered durable target that it does not invoke because the target's
// execution is paused.
var pendingResponse = []byte(`{"Status":"PENDING"}`)

// PauseExecution marks the local execution paused, as the
// PauseDurableExecution API does. It does not wait for the calling
// invocation to end. The next checkpoint of that invocation is recorded
// and answered without a token, so the invocation ends with PENDING. The
// runner then does not invoke the handler until
// [LocalRunner.ResumeExecution] is called.
//
// A repeated request with the same client token succeeds and changes
// nothing. A request for an execution that has ended or is already paused
// with another token returns a ResourceConflictException.
func (m *memoryClient) PauseExecution(_ context.Context, in durable.PauseExecutionInput) (durable.PauseExecutionOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.paused && in.ClientToken != "" && in.ClientToken == m.pauseToken {
		return durable.PauseExecutionOutput{}, nil
	}
	if m.paused {
		return durable.PauseExecutionOutput{}, serviceError(errCodeResourceConflict, "The execution is already paused.")
	}
	if m.executionEnded {
		return durable.PauseExecutionOutput{}, serviceError(errCodeResourceConflict, "The execution has closed.")
	}
	m.paused = true
	m.pauseToken = in.ClientToken
	m.pauseReason = in.Reason
	return durable.PauseExecutionOutput{}, nil
}

// resume clears the paused mark. It returns a ResourceConflictException
// when the execution is not paused.
func (m *memoryClient) resume() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.paused {
		return serviceError(errCodeResourceConflict, "The execution is not paused.")
	}
	m.paused = false
	m.pauseToken = ""
	m.pauseReason = ""
	return nil
}

// isPaused reports whether the execution is paused, and the reason the
// pause request gave.
func (m *memoryClient) isPaused() (bool, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.paused, m.pauseReason
}
