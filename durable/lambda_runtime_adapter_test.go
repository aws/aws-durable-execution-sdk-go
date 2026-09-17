// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/lambdacontext"
)

func TestInvocationInfoFromContextPopulated(t *testing.T) {
	ctx := lambdacontext.NewContext(context.Background(), &lambdacontext.LambdaContext{
		AwsRequestID:       "req-123",
		InvokedFunctionArn: "arn:aws:lambda:us-east-1:000:function:fn",
	})

	got := invocationInfoFromContext(ctx)
	if got.requestID != "req-123" {
		t.Errorf("requestID = %q, want %q", got.requestID, "req-123")
	}
	if want := "arn:aws:lambda:us-east-1:000:function:fn"; got.invokedFunctionARN != want {
		t.Errorf("invokedFunctionARN = %q, want %q", got.invokedFunctionARN, want)
	}
}

func TestInvocationInfoFromContextAbsent(t *testing.T) {
	got := invocationInfoFromContext(context.Background())
	if got != (invocationInfo{}) {
		t.Errorf("invocationInfoFromContext() = %+v, want zero value", got)
	}
}
