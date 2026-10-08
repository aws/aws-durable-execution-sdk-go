// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// DurableExecutionAPI is the subset of the Lambda service client that
// CloudRunner requires. It covers execution lifecycle queries and
// callback operations. Callers supply a real *lambda.Client or a test
// fake implementing these methods.
type DurableExecutionAPI interface {
	// GetDurableExecution retrieves execution metadata including status
	// and result.
	GetDurableExecution(ctx context.Context, params *lambda.GetDurableExecutionInput, optFns ...func(*lambda.Options)) (*lambda.GetDurableExecutionOutput, error)

	// GetDurableExecutionHistory retrieves the execution's event
	// history.
	GetDurableExecutionHistory(ctx context.Context, params *lambda.GetDurableExecutionHistoryInput, optFns ...func(*lambda.Options)) (*lambda.GetDurableExecutionHistoryOutput, error)

	// Invoke starts or resumes a durable function invocation.
	Invoke(ctx context.Context, params *lambda.InvokeInput, optFns ...func(*lambda.Options)) (*lambda.InvokeOutput, error)

	// SendDurableExecutionCallbackSuccess completes a callback with
	// success.
	SendDurableExecutionCallbackSuccess(ctx context.Context, params *lambda.SendDurableExecutionCallbackSuccessInput, optFns ...func(*lambda.Options)) (*lambda.SendDurableExecutionCallbackSuccessOutput, error)

	// SendDurableExecutionCallbackFailure completes a callback with
	// failure.
	SendDurableExecutionCallbackFailure(ctx context.Context, params *lambda.SendDurableExecutionCallbackFailureInput, optFns ...func(*lambda.Options)) (*lambda.SendDurableExecutionCallbackFailureOutput, error)

	// SendDurableExecutionCallbackHeartbeat extends a callback timeout.
	SendDurableExecutionCallbackHeartbeat(ctx context.Context, params *lambda.SendDurableExecutionCallbackHeartbeatInput, optFns ...func(*lambda.Options)) (*lambda.SendDurableExecutionCallbackHeartbeatOutput, error)
}

// CloudRunnerOption configures a [CloudRunner].
type CloudRunnerOption func(*cloudRunnerConfig)

type cloudRunnerConfig struct {
	pollInterval time.Duration
	timeout      time.Duration
}

// WithPollInterval sets the interval between GetDurableExecution polls.
// Defaults to 2 seconds.
func WithPollInterval(d time.Duration) CloudRunnerOption {
	return func(c *cloudRunnerConfig) { c.pollInterval = d }
}

// WithTimeout sets the maximum time CloudRunner will spend polling before
// returning an error. Defaults to 5 minutes.
func WithTimeout(d time.Duration) CloudRunnerOption {
	return func(c *cloudRunnerConfig) { c.timeout = d }
}

const (
	defaultCloudPollInterval = 2 * time.Second
	defaultCloudTimeout      = 5 * time.Minute
)

// CloudRunner invokes a deployed durable Lambda function and polls the
// durable execution APIs until the execution reaches a terminal status.
// It then maps the result and operation log into the same [TestResult]
// and [TestOperation] types that [LocalRunner] returns, enabling the
// same assertions to be used regardless of execution environment.
//
// CloudRunner requires a caller-supplied [DurableExecutionAPI]
// implementation (typically a real *lambda.Client) and a function
// identifier (name or ARN).
//
// # Example
//
//	ctx := context.Background()
//	cfg, _ := config.LoadDefaultConfig(ctx)
//	client := lambda.NewFromConfig(cfg)
//	runner := durabletest.NewCloudRunner(client, "my-function:$LATEST")
//	result, err := runner.Run(ctx, `{"orderId": "123"}`)
type CloudRunner struct {
	api          DurableExecutionAPI
	functionName string
	cfg          cloudRunnerConfig
}

// NewCloudRunner creates a runner targeting the given deployed durable
// function. The api parameter accepts any implementation of
// [DurableExecutionAPI]; in production this is typically a
// *lambda.Client from aws-sdk-go-v2.
//
// functionName must be a qualified function identifier (name, ARN, or
// partial ARN with version/alias qualifier).
func NewCloudRunner(api DurableExecutionAPI, functionName string, opts ...CloudRunnerOption) *CloudRunner {
	cfg := cloudRunnerConfig{
		pollInterval: defaultCloudPollInterval,
		timeout:      defaultCloudTimeout,
	}
	for _, o := range opts {
		o(&cfg)
	}
	return &CloudRunner{
		api:          api,
		functionName: functionName,
		cfg:          cfg,
	}
}

// Run invokes the function with event (JSON-encoded), polls until the
// execution reaches a terminal status, and returns the [TestResult]. Run
// passes ctx to the Lambda Invoke call and to every poll, so a deadline or
// cancellation on ctx ends the run.
//
// The returned error is non-nil only when the runner itself fails: the
// event does not marshal to JSON, the Invoke call fails, the invoke
// response has no DurableExecutionArn, polling fails or exceeds the
// [WithTimeout] limit, or ctx ends. The result is then nil. An execution
// that fails produces a nil error and a [Failed] result with the recorded
// error in [TestResult.Error].
func (r *CloudRunner) Run(ctx context.Context, event any) (*TestResult, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("durabletest.CloudRunner: marshal event: %w", err)
	}

	invokeOut, err := r.api.Invoke(ctx, &lambda.InvokeInput{
		FunctionName: aws.String(r.functionName),
		Payload:      payload,
	})
	if err != nil {
		return nil, fmt.Errorf("durabletest.CloudRunner: invoke %q: %w", r.functionName, err)
	}

	executionArn := aws.ToString(invokeOut.DurableExecutionArn)
	if executionArn == "" {
		return nil, fmt.Errorf("durabletest.CloudRunner: invoke response has no DurableExecutionArn — is %q a qualified durable function?", r.functionName)
	}
	return r.RunWithArn(ctx, executionArn)
}

// RunWithArn polls an already-started execution by ARN until it reaches a
// terminal status. Use this when the execution was started externally
// (e.g. async invoke). The error contract is the one [CloudRunner.Run]
// states, minus the Invoke call: the error is non-nil, and the result
// nil, only when polling fails, exceeds the [WithTimeout] limit, or ctx
// ends.
func (r *CloudRunner) RunWithArn(ctx context.Context, executionArn string) (*TestResult, error) {
	result, err := r.pollUntilTerminal(ctx, executionArn)
	if err != nil {
		return nil, fmt.Errorf("durabletest.CloudRunner: %w", err)
	}
	return result, nil
}

// SendCallbackSuccess resolves a pending callback with a success payload,
// mirroring [LocalRunner.SendCallbackSuccess].
func (r *CloudRunner) SendCallbackSuccess(callbackID string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("durabletest.CloudRunner: marshal callback payload: %w", err)
	}
	_, err = r.api.SendDurableExecutionCallbackSuccess(context.Background(), &lambda.SendDurableExecutionCallbackSuccessInput{
		CallbackId: aws.String(callbackID),
		Result:     data,
	})
	if err != nil {
		return fmt.Errorf("durabletest.CloudRunner: SendCallbackSuccess: %w", err)
	}
	return nil
}

// SendCallbackFailure fails a pending callback with a typed error,
// mirroring [LocalRunner.SendCallbackFailure].
func (r *CloudRunner) SendCallbackFailure(callbackID, errorType, errorMessage string) error {
	_, err := r.api.SendDurableExecutionCallbackFailure(context.Background(), &lambda.SendDurableExecutionCallbackFailureInput{
		CallbackId: aws.String(callbackID),
		Error: &types.ErrorObject{
			ErrorType:    aws.String(errorType),
			ErrorMessage: aws.String(errorMessage),
		},
	})
	if err != nil {
		return fmt.Errorf("durabletest.CloudRunner: SendCallbackFailure: %w", err)
	}
	return nil
}

// SendCallbackHeartbeat extends the heartbeat timeout of a pending
// callback, mirroring [LocalRunner.SendCallbackHeartbeat].
func (r *CloudRunner) SendCallbackHeartbeat(callbackID string) error {
	_, err := r.api.SendDurableExecutionCallbackHeartbeat(context.Background(), &lambda.SendDurableExecutionCallbackHeartbeatInput{
		CallbackId: aws.String(callbackID),
	})
	if err != nil {
		return fmt.Errorf("durabletest.CloudRunner: SendCallbackHeartbeat: %w", err)
	}
	return nil
}

// pollUntilTerminal polls GetDurableExecution for status, then fetches
// the operation log and builds a TestResult.
func (r *CloudRunner) pollUntilTerminal(ctx context.Context, arn string) (*TestResult, error) {
	deadline := time.Now().Add(r.cfg.timeout)

	for {
		execOut, err := r.api.GetDurableExecution(ctx, &lambda.GetDurableExecutionInput{
			DurableExecutionArn:  aws.String(arn),
			IncludeExecutionData: aws.Bool(true),
		})
		if err != nil {
			return nil, fmt.Errorf("GetDurableExecution %q: %w", arn, err)
		}

		if isTerminalExecutionStatus(execOut.Status) {
			return r.buildResult(ctx, arn, execOut)
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out after %s waiting for execution %q (last status: %s)", r.cfg.timeout, arn, execOut.Status)
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled while polling %q: %w", arn, ctx.Err())
		case <-time.After(r.cfg.pollInterval):
		}
	}
}

// buildResult fetches the full event history and constructs a TestResult
// carrying the events, the invocation records, and the operations folded
// from the events. The execution's own lifecycle events fold into an
// EXECUTION operation; it is dropped so that [TestResult.Operations] holds
// only the handler's operations, as it does under [LocalRunner].
func (r *CloudRunner) buildResult(ctx context.Context, arn string, execOut *lambda.GetDurableExecutionOutput) (*TestResult, error) {
	events, err := r.fetchAllEvents(ctx, arn)
	if err != nil {
		return nil, fmt.Errorf("fetch history for %q: %w", arn, err)
	}

	tr := &TestResult{
		Operations: toTestOperations(withoutExecutionOperation(operationsFromEvents(events))),
	}
	tr.attachEvents(events)

	switch execOut.Status {
	case types.ExecutionStatusSucceeded:
		tr.Status = Succeeded
		if execOut.Result != nil {
			tr.RawResult = *execOut.Result
		}
	case types.ExecutionStatusFailed, types.ExecutionStatusTimedOut, types.ExecutionStatusStopped:
		tr.Status = Failed
		if execOut.Error != nil {
			tr.Error = &TestError{
				Type:       aws.ToString(execOut.Error.ErrorType),
				Message:    aws.ToString(execOut.Error.ErrorMessage),
				ErrorData:  aws.ToString(execOut.Error.ErrorData),
				StackTrace: copyStrings(execOut.Error.StackTrace),
			}
		}
	default:
		tr.Status = Failed
		tr.Error = &TestError{
			Type:    "UnknownStatus",
			Message: fmt.Sprintf("unexpected terminal status: %s", execOut.Status),
		}
	}

	return tr, nil
}

// fetchAllEvents retrieves the execution's full event history, following
// pagination, in the order the service returns it.
func (r *CloudRunner) fetchAllEvents(ctx context.Context, arn string) ([]types.Event, error) {
	// GetDurableExecutionHistory pages through the execution's events;
	// each event carries the ID of the operation it belongs to.
	var events []types.Event
	var marker *string

	for {
		out, err := r.api.GetDurableExecutionHistory(ctx, &lambda.GetDurableExecutionHistoryInput{
			DurableExecutionArn:  aws.String(arn),
			IncludeExecutionData: aws.Bool(true),
			MaxItems:             maxHistoryItemsPerPage,
			Marker:               marker,
		})
		if err != nil {
			return nil, err
		}
		events = append(events, out.Events...)
		if out.NextMarker == nil || *out.NextMarker == "" {
			return events, nil
		}
		marker = out.NextMarker
	}
}

// maxHistoryItemsPerPage is the largest page size the history API allows.
const maxHistoryItemsPerPage = 1000

// withoutExecutionOperation returns ops without any EXECUTION operation.
func withoutExecutionOperation(ops []durable.Operation) []durable.Operation {
	out := make([]durable.Operation, 0, len(ops))
	for _, op := range ops {
		if op.Type == durable.OperationTypeExecution {
			continue
		}
		out = append(out, op)
	}
	return out
}

func isTerminalExecutionStatus(s types.ExecutionStatus) bool {
	switch s {
	case types.ExecutionStatusSucceeded, types.ExecutionStatusFailed,
		types.ExecutionStatusTimedOut, types.ExecutionStatusStopped:
		return true
	}
	return false
}
