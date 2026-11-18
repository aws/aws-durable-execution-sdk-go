// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	smithy "github.com/aws/smithy-go"
)

// pauseReasonMaxLength is the longest Reason the PauseDurableExecution API
// accepts, in characters.
const pauseReasonMaxLength = 1024

// pauseConflictErrorCode is the error code the PauseDurableExecution API
// returns when the execution has closed or is already paused.
const pauseConflictErrorCode = "ResourceConflictException"

// pauseAfterRetry pauses the durable execution after the RETRY of attempt
// of the step id was recorded. It reports whether the pause request
// succeeded. Then the checkpointer is halted with errSuspendExecution, so
// the invocation ends with PENDING whatever the handler does next, as
// after a checkpoint response without a token.
//
// It reports false, and requests nothing, when the step's batch branch was
// abandoned, or when the client does not implement [ExecutionPauser]. It
// reports false after a failed request too, which it logs as a warning.
// The step then waits for its retry as usual.
func pauseAfterRetry(ec *execContext, id, name string, attempt int) bool {
	// A branch abandoned by its batch records nothing that belongs to the
	// batch result, so it must not pause the execution.
	if ec.abandon.abandoned() {
		return false
	}
	pauser, ok := ec.checkpointer.client.(ExecutionPauser)
	if !ok {
		return false
	}
	wireID := hashID(id)
	_, err := pauser.PauseExecution(ec, PauseExecutionInput{
		ExecutionArn: ec.executionArn,
		ClientToken:  pauseClientToken(ec.executionArn, wireID, attempt),
		Reason:       pauseReason(name, wireID, attempt),
	})
	if err != nil {
		ec.operationLogger(id, name, attempt).WarnContext(ec, pauseFailureMessage(name, wireID, attempt, err))
		return false
	}
	ec.checkpointer.halt(errSuspendExecution)
	return true
}

// pauseClientToken returns the ClientToken of the pause request after
// attempt of the step with wire ID wireID: the SHA-256 of
// "pause:<execution ARN>:<wire ID>:<attempt>" as 64 lowercase hex
// characters. It is the same when the attempt runs again and differs
// across executions and attempts.
func pauseClientToken(executionArn, wireID string, attempt int) string {
	sum := sha256.Sum256([]byte("pause:" + executionArn + ":" + wireID + ":" + strconv.Itoa(attempt)))
	return hex.EncodeToString(sum[:])
}

// pauseReason returns the Reason of the pause request: `Step "<name>"
// failed attempt <n>`, with the wire ID when the step has no name, cut to
// pauseReasonMaxLength characters.
func pauseReason(name, wireID string, attempt int) string {
	reason := fmt.Sprintf("Step %q failed attempt %d", stepLabel(name, wireID), attempt)
	if runes := []rune(reason); len(runes) > pauseReasonMaxLength {
		reason = string(runes[:pauseReasonMaxLength])
	}
	return reason
}

// stepLabel names a step in pause messages: its name, or its wire ID when
// it has no name.
func stepLabel(name, wireID string) string {
	if name != "" {
		return name
	}
	return wireID
}

// pauseFailureMessage returns the warning logged when the pause request
// failed. It names the error code and the error message. A ResourceConflictException means the
// execution has closed or is already paused; for every other error a
// missing permission is one possible cause.
func pauseFailureMessage(name, wireID string, attempt int, err error) string {
	code := pauseErrorName(err)
	hint := "A missing lambda:PauseDurableExecution permission on the execution role is one possible cause."
	if code == pauseConflictErrorCode {
		hint = "The execution has closed (for example, stopped) or is already paused."
	}
	return fmt.Sprintf("Could not pause the execution after attempt %d of step %q. Retrying as usual. Error: %s: %v. %s",
		attempt, stepLabel(name, wireID), code, err, hint)
}

// pauseErrorName returns the error code of a service error, or the Go type
// of any other error.
func pauseErrorName(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() != "" {
		return apiErr.ErrorCode()
	}
	return fmt.Sprintf("%T", err)
}
