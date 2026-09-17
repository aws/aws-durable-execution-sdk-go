// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// This file is the only non-test file in the package that names
// aws-lambda-go types; test files also name them. It adapts the Lambda
// runtime entry point and extracts invocation metadata from the
// aws-lambda-go context values.

package durable

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
)

// Start registers handler as the Lambda function handler and begins
// processing invocations. It is the durable analogue of lambda.Start and
// does not return.
func Start[I, O any](handler Handler[I, O], opts ...HandlerOption) {
	lambda.Start(rawPayloadHandler(Wrap(handler, opts...)))
}

// rawPayloadHandler registers a raw payload function with the Lambda
// runtime such that the invocation payload arrives byte-exact.
//
// The aws-lambda-go reflective path JSON-decodes the payload into the
// function's parameter type. For a []byte parameter, encoding/json expects
// a base64 string, but the durable invocation payload is a JSON object.
// So Start registers through the runtime's raw byte interface instead,
// which passes the payload through unmodified.
type rawPayloadHandler func(context.Context, []byte) ([]byte, error)

func (f rawPayloadHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	return f(ctx, payload)
}

// invocationInfo is the per-invocation Lambda metadata the SDK exposes
// through [Context].
type invocationInfo struct {
	requestID          string
	invokedFunctionARN string
}

// invocationInfoFromContext extracts Lambda invocation metadata from the
// context values that the aws-lambda-go runtime sets. Outside a Lambda
// invocation (such as in local tests), the fields are empty.
func invocationInfoFromContext(ctx context.Context) invocationInfo {
	lc, ok := lambdacontext.FromContext(ctx)
	if !ok || lc == nil {
		return invocationInfo{}
	}
	return invocationInfo{
		requestID:          lc.AwsRequestID,
		invokedFunctionARN: lc.InvokedFunctionArn,
	}
}
