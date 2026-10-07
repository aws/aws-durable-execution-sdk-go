// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"encoding/json"
	"fmt"

	smithy "github.com/aws/smithy-go"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// The service applies these limits to each checkpoint update. The local
// client applies the same limits, so a handler that the service would
// reject fails locally the same way.
const (
	// stepPayloadLimit bounds the payload of a STEP SUCCEED or RETRY
	// update: a step result or a WaitForCondition state.
	stepPayloadLimit = 262144

	// errorObjectLimit bounds the serialized error object of an update.
	errorObjectLimit = 262144

	// invokeInputLimit bounds the payload of a CHAINED_INVOKE START
	// update: the input of the invoked function.
	invokeInputLimit = 1048576

	// executionResultLimit bounds the payload of an EXECUTION SUCCEED
	// update: a handler result too large to return inline.
	executionResultLimit = 6291456

	// minWaitSeconds is the least WaitSeconds a WAIT START update may
	// carry.
	minWaitSeconds = 1
)

// Error codes and messages the service returns for a rejected checkpoint.
const (
	errCodeInvalidParameterValue = "InvalidParameterValueException"
	errCodeValidation            = "ValidationException"

	msgStepPayloadTooLarge = "STEP output payload size must be less than or equal to 262144 bytes."
	msgErrorObjectTooLarge = "Error object size must be less than or equal to 262144 bytes."
	msgInvokeInputTooLarge = "CHAINED_INVOKE input payload size must be less than or equal to 1048576 bytes."

	// msgPendingWithNothingPending is the error the service fails an
	// execution with after repeated PENDING responses that report no
	// pending operation.
	msgPendingWithNothingPending = "Cannot return PENDING status with no pending operations."
)

// validateCheckpoint returns the error the service returns for the first
// update of in that violates a service limit, or nil when every update is
// accepted. The service checks the request's shape before it checks the
// payloads, so a ValidationException takes precedence over an
// InvalidParameterValueException. The error is a client fault, so the SDK
// classifies it as it classifies the service's response.
func validateCheckpoint(in durable.CheckpointInput) error {
	for i, u := range in.Updates {
		// The service names an update by its 1-based position in the
		// request.
		pos := i + 1
		if u.Type == durable.OperationTypeWait && u.Action == durable.OperationActionStart {
			var secs int32
			if u.WaitOptions != nil && u.WaitOptions.WaitSeconds != nil {
				secs = *u.WaitOptions.WaitSeconds
			}
			if secs < minWaitSeconds {
				return serviceError(errCodeValidation, fmt.Sprintf(
					"1 validation error detected: Value '%d' at 'updates.%d.member.waitOptions.waitSeconds' failed to satisfy constraint: Member must have value greater than or equal to %d",
					secs, pos, minWaitSeconds))
			}
		}
		if u.Type == durable.OperationTypeExecution && u.Action == durable.OperationActionSucceed &&
			len(ptrStr(u.Payload)) > executionResultLimit {
			return serviceError(errCodeValidation, fmt.Sprintf(
				"1 validation error detected: Value at 'updates.%d.member.payload' failed to satisfy constraint: Member must have length less than or equal to %d",
				pos, executionResultLimit))
		}
	}
	for _, u := range in.Updates {
		size := len(ptrStr(u.Payload))
		switch {
		case u.Type == durable.OperationTypeStep &&
			(u.Action == durable.OperationActionSucceed || u.Action == durable.OperationActionRetry) &&
			size > stepPayloadLimit:
			return serviceError(errCodeInvalidParameterValue, msgStepPayloadTooLarge)
		case u.Type == durable.OperationTypeChainedInvoke && u.Action == durable.OperationActionStart &&
			size > invokeInputLimit:
			return serviceError(errCodeInvalidParameterValue, msgInvokeInputTooLarge)
		case errorObjectSize(u.Error) > errorObjectLimit:
			return serviceError(errCodeInvalidParameterValue, msgErrorObjectTooLarge)
		}
	}
	return nil
}

// errorObjectSize returns the length of the serialized error object, or
// zero when e is nil.
func errorObjectSize(e *durable.ErrorObject) int {
	if e == nil {
		return 0
	}
	data, err := json.Marshal(wireErrorObject(e))
	if err != nil {
		return 0
	}
	return len(data)
}

// serviceError builds a client-fault API error with the given code and
// message, the shape of the service's rejection of a checkpoint.
func serviceError(code, message string) error {
	return &smithy.GenericAPIError{Code: code, Message: message, Fault: smithy.FaultClient}
}
