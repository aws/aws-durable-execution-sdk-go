// lambda_invoker.go provides the production LambdaInvoker implementation
// (see cloud_runner.go's doc for why LambdaInvoker is its own narrow
// interface, separate from checkpoint.GetExecutionStateClient), backed
// directly by the AWS SDK for Go v2's Lambda client - the same module
// pkg/durable/awssdk already depends on (see go.mod's pinned
// github.com/aws/aws-sdk-go-v2/service/lambda v1.99.0), imported
// independently here rather than by depending on the awssdk package
// itself, since awssdk.Client/LambdaAPI today only expose
// CheckpointDurableExecution and this task's scope explicitly excludes
// widening that package's interface (see cloud_runner.go's doc).
package testing

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// RawLambdaAPI is the subset of the AWS SDK for Go v2's *lambda.Client
// this package's LambdaInvoker implementation depends on, exposed as an
// interface (matching awssdk.LambdaAPI's own established pattern in this
// repo - see that package's client.go doc) so tests can substitute a
// fake without real AWS credentials or network access. *lambda.Client
// satisfies this directly.
type RawLambdaAPI interface {
	Invoke(ctx context.Context, params *lambda.InvokeInput, optFns ...func(*lambda.Options)) (*lambda.InvokeOutput, error)
}

// sdkLambdaInvoker adapts a RawLambdaAPI into this package's own
// LambdaInvoker interface (see cloud_runner.go), translating between the
// AWS SDK for Go v2's InvokeInput/InvokeOutput and the plain
// ([]byte, bool, error) shape LambdaInvoker exposes.
type sdkLambdaInvoker struct {
	api RawLambdaAPI
}

// NewLambdaInvoker constructs a LambdaInvoker backed by a real
// *lambda.Client, resolving credentials and region via the standard AWS
// SDK for Go v2 default chain (github.com/aws/aws-sdk-go-v2/config.LoadDefaultConfig)
// - the same resolution this repo's awssdk.New already uses for the
// production checkpoint.Client (see that function's doc for why this
// order is correct), reused here for consistency even though this is a
// separate capability (Invoke, not Checkpoint) and a separate package
// dependency edge.
//
// optFns are passed through to config.LoadDefaultConfig, letting callers
// override the region or supply explicit credentials, matching
// awssdk.New's own optFns parameter.
func NewLambdaInvoker(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (LambdaInvoker, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("testing.NewLambdaInvoker: loading AWS config: %w", err)
	}
	return &sdkLambdaInvoker{api: lambda.NewFromConfig(cfg)}, nil
}

// Invoke calls the real Lambda Invoke API synchronously
// (InvocationType: RequestResponse, the default - see InvokeInput's own
// doc), translating the response into LambdaInvoker's plain
// (responsePayload, functionError, executionArn, err) shape.
//
// FunctionError (a non-empty string on InvokeOutput, e.g. "Unhandled")
// indicates the invoked function's OWN container crashed/panicked before
// or during producing a response - confirmed from the AWS SDK for Go
// v2's own InvokeOutput.FunctionError doc, matching this method's
// functionError return value's own doc on the LambdaInvoker interface.
// This is distinct from - and, per that interface's doc, deliberately
// not conflated with - a durable execution that reached a FAILED
// terminal status through this SDK's own error handling (that is a
// perfectly normal, non-crashing Invoke response as far as Lambda's
// Invoke API itself is concerned; only GetDurableExecutionState's own
// root EXECUTION operation Status distinguishes SUCCEEDED from FAILED
// for that case - see cloud_runner.go's awaitTerminal).
//
// executionArn is read directly from out.DurableExecutionArn - CONFIRMED
// (via the official Invoke API Reference,
// docs.aws.amazon.com/lambda/latest/dg/API_Invoke.html, and this exact
// field's own doc comment in the pinned aws-sdk-go-v2/service/lambda
// v1.99.0 module) to always be populated by the real Invoke API when
// invoking a durable function - see cloud_runner.go's Run method for how
// this now lets a fresh invocation (no caller-supplied ARN) still be
// polled without requiring the caller to already know the execution's
// ARN ahead of time.
func (s *sdkLambdaInvoker) Invoke(ctx context.Context, functionNameOrARN string, payload []byte) ([]byte, bool, string, error) {
	out, err := s.api.Invoke(ctx, &lambda.InvokeInput{
		FunctionName: aws.String(functionNameOrARN),
		Payload:      payload,
	})
	if err != nil {
		return nil, false, "", err
	}
	return out.Payload, aws.ToString(out.FunctionError) != "", aws.ToString(out.DurableExecutionArn), nil
}

// InvokeAsync implements the OPTIONAL AsyncLambdaInvoker interface
// (cloud_runner.go) - see that interface's own doc for the full
// rationale on why CloudTestRunner.RunUntilCallback needs a genuinely
// asynchronous (InvocationType: Event) Invoke call, distinct from
// Invoke's own synchronous (RequestResponse) one above.
func (s *sdkLambdaInvoker) InvokeAsync(ctx context.Context, functionNameOrARN string, payload []byte) (string, error) {
	out, err := s.api.Invoke(ctx, &lambda.InvokeInput{
		FunctionName:   aws.String(functionNameOrARN),
		InvocationType: lambdatypes.InvocationTypeEvent,
		Payload:        payload,
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.DurableExecutionArn), nil
}
