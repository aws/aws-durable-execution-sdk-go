// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	smithy "github.com/aws/smithy-go"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// The service rejects a WAIT START update whose WaitSeconds is under 1.
// The SDK's Wait never sends one, so the local client's check is exercised
// with a request built directly.
func TestValidateCheckpointRejectsZeroWaitSeconds(t *testing.T) {
	const want = "1 validation error detected: Value '0' at 'updates.1.member.waitOptions.waitSeconds' failed to satisfy constraint: Member must have value greater than or equal to 1"
	in := durable.CheckpointInput{Updates: []durable.OperationUpdate{{
		Id:          aws.String("w"),
		Type:        durable.OperationTypeWait,
		Action:      durable.OperationActionStart,
		WaitOptions: &durable.WaitOptions{WaitSeconds: aws.Int32(0)},
	}}}
	err := validateCheckpoint(in)
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error = %v, want a service API error", err)
	}
	if apiErr.ErrorCode() != errCodeValidation || apiErr.ErrorMessage() != want {
		t.Fatalf("error = %s: %s, want %s: %s", apiErr.ErrorCode(), apiErr.ErrorMessage(), errCodeValidation, want)
	}

	in.Updates[0].WaitOptions.WaitSeconds = aws.Int32(1)
	if err := validateCheckpoint(in); err != nil {
		t.Fatalf("WaitSeconds 1: error = %v, want nil", err)
	}
}
