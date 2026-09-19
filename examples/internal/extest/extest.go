// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Package extest runs an example's handler test either locally, with
// [durabletest.LocalRunner], or against the deployed example function, with
// [durabletest.CloudRunner]. Both runners return the same
// [durabletest.TestResult], so one test body serves both environments.
//
// The environment variable named by [EnvRunner] selects the runner. It
// defaults to local, which needs no AWS credentials or network access.
// Cloud mode reads [EnvFunctionPrefix] for the FunctionNamePrefix the stack
// was deployed with and derives the function name from the example's
// directory, which is the working directory of a Go test binary.
//
// Some steps of a local test have no cloud counterpart: a locally run
// handler blocks on an invoke or a callback until the test resolves it,
// while the deployed example is resolved by its companion functions. A
// test guards those steps with [Runner.Local] and states why:
//
//	result := runner.RunUntilComplete(t, input)
//	if runner.Local() {
//		// Locally the invoke has no target; the deployed target answers
//		// in the cloud.
//		runner.CompleteChainedInvoke("invoke", reply)
//		result = runner.RunUntilComplete(t, input)
//	}
//
// Calling a local-only method in cloud mode fails the test with a message
// naming the method, so an unguarded step cannot pass by accident.
package extest

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

const (
	// EnvRunner selects the runner: "local" (the default when unset) or
	// "cloud".
	EnvRunner = "DURABLE_EXAMPLES_RUNNER"

	// EnvFunctionPrefix names the FunctionNamePrefix the example stack was
	// deployed with. The deployed functions read the same variable to
	// build the names of the companion functions they invoke.
	EnvFunctionPrefix = "FUNCTION_NAME_PREFIX"

	// defaultFunctionPrefix is the prefix the examples fall back to when
	// EnvFunctionPrefix is unset. TargetFunction mirrors it so a local
	// test registers a target under the name the handler will invoke.
	defaultFunctionPrefix = "v2-"

	// cloudPollInterval and cloudTimeout match the cloud smoke test.
	cloudPollInterval = 5 * time.Second
	cloudTimeout      = 15 * time.Minute
)

// Runner drives one example's handler in the selected environment.
type Runner[I, O any] struct {
	t     testing.TB
	local *durabletest.LocalRunner[I, O]
	cloud *durabletest.CloudRunner

	// cloudResult caches the terminal result of the cloud execution. A
	// terminal execution cannot advance, so repeated RunUntilComplete
	// calls in cloud mode return it instead of starting another
	// execution.
	cloudResult *durabletest.TestResult
}

// IsCloud reports whether tests in this process run against the deployed
// functions.
func IsCloud() bool {
	return os.Getenv(EnvRunner) == "cloud"
}

// New creates a runner for the handler under test. In local mode opts are
// passed to [durabletest.NewLocalRunner]; in cloud mode they are ignored,
// because the deployed function is already configured.
func New[I, O any](t *testing.T, handler durable.Handler[I, O], opts ...durable.HandlerOption) *Runner[I, O] {
	t.Helper()
	switch mode := os.Getenv(EnvRunner); mode {
	case "", "local":
		return &Runner[I, O]{t: t, local: durabletest.NewLocalRunner(handler, opts...)}
	case "cloud":
		return newCloud[I, O](t, cloudClient(t), TargetFunction(exampleName(t)))
	default:
		t.Fatalf("extest: %s=%q; want \"local\" or \"cloud\"", EnvRunner, mode)
		return nil
	}
}

// newCloud creates a cloud-mode runner over api. New calls it with the real
// Lambda client; tests pass a fake.
func newCloud[I, O any](t testing.TB, api durabletest.DurableExecutionAPI, functionName string) *Runner[I, O] {
	return &Runner[I, O]{
		t: t,
		cloud: durabletest.NewCloudRunner(api, functionName,
			durabletest.WithPollInterval(cloudPollInterval), durabletest.WithTimeout(cloudTimeout)),
	}
}

// Cloud reports whether this runner drives the deployed function.
func (r *Runner[I, O]) Cloud() bool { return r.cloud != nil }

// Local reports whether this runner drives the handler in-process.
func (r *Runner[I, O]) Local() bool { return r.local != nil }

// TargetFunction returns the qualified name of the deployed example named
// name (its directory under examples/), using the prefix in
// [EnvFunctionPrefix] or the examples' default. It is the name a handler
// builds for a companion function, so a local test can register the
// target under it and a cloud test can invoke it.
func TargetFunction(name string) string {
	prefix := os.Getenv(EnvFunctionPrefix)
	if prefix == "" {
		prefix = defaultFunctionPrefix
	}
	return prefix + "go-" + name + ":$LATEST"
}

// RunUntilComplete runs the execution until it reaches a terminal status
// or, locally, blocks on external resolution. See
// [durabletest.LocalRunner.RunUntilComplete] and
// [durabletest.CloudRunner.Run]. In cloud mode the first call starts the
// execution and waits for it to finish; later calls return that result.
func (r *Runner[I, O]) RunUntilComplete(t *testing.T, event I) *durabletest.TestResult {
	t.Helper()
	if r.local != nil {
		return r.local.RunUntilComplete(t, event)
	}
	if r.cloudResult == nil {
		r.cloudResult = r.cloud.Run(t, event)
	}
	return r.cloudResult
}

// Run performs a single invocation locally. The cloud has no
// single-invocation step, so in cloud mode Run behaves like
// [Runner.RunUntilComplete].
func (r *Runner[I, O]) Run(t *testing.T, event I) *durabletest.TestResult {
	t.Helper()
	if r.local != nil {
		return r.local.Run(t, event)
	}
	return r.RunUntilComplete(t, event)
}

// RegisterFunction registers fn as the local target of chained invokes of
// functionID; see [durabletest.LocalRunner.RegisterFunction]. In cloud
// mode it does nothing: the deployed companion is the target.
func (r *Runner[I, O]) RegisterFunction(functionID string, fn durabletest.Function) {
	if r.local != nil {
		r.local.RegisterFunction(functionID, fn)
	}
}

// Reset returns a local runner to its initial state; see
// [durabletest.LocalRunner.Reset]. In cloud mode it forgets the cached
// result, so the next RunUntilComplete starts a new execution.
func (r *Runner[I, O]) Reset() {
	if r.local != nil {
		r.local.Reset()
		return
	}
	r.cloudResult = nil
}

// CompleteChainedInvoke resolves a pending chained invoke locally; see
// [durabletest.LocalRunner.CompleteChainedInvoke].
func (r *Runner[I, O]) CompleteChainedInvoke(name string, payload any) error {
	r.localOnly("CompleteChainedInvoke")
	return r.local.CompleteChainedInvoke(name, payload)
}

// FailChainedInvoke fails a pending chained invoke locally; see
// [durabletest.LocalRunner.FailChainedInvoke].
func (r *Runner[I, O]) FailChainedInvoke(name, errorType, errorMessage string) error {
	r.localOnly("FailChainedInvoke")
	return r.local.FailChainedInvoke(name, errorType, errorMessage)
}

// OpenCallbacks lists the callbacks awaiting resolution locally; see
// [durabletest.LocalRunner.OpenCallbacks].
func (r *Runner[I, O]) OpenCallbacks() []durabletest.OpenCallback {
	r.localOnly("OpenCallbacks")
	return r.local.OpenCallbacks()
}

// SendCallbackSuccess completes a pending callback locally; see
// [durabletest.LocalRunner.SendCallbackSuccess].
func (r *Runner[I, O]) SendCallbackSuccess(callbackID string, payload any) error {
	r.localOnly("SendCallbackSuccess")
	return r.local.SendCallbackSuccess(callbackID, payload)
}

// SendCallbackFailure fails a pending callback locally; see
// [durabletest.LocalRunner.SendCallbackFailure].
func (r *Runner[I, O]) SendCallbackFailure(callbackID, errorType, errorMessage string) error {
	r.localOnly("SendCallbackFailure")
	return r.local.SendCallbackFailure(callbackID, errorType, errorMessage)
}

// SendCallbackHeartbeat extends a pending callback locally; see
// [durabletest.LocalRunner.SendCallbackHeartbeat].
func (r *Runner[I, O]) SendCallbackHeartbeat(callbackID string) error {
	r.localOnly("SendCallbackHeartbeat")
	return r.local.SendCallbackHeartbeat(callbackID)
}

// TimeoutCallback times out a pending callback locally; see
// [durabletest.LocalRunner.TimeoutCallback].
func (r *Runner[I, O]) TimeoutCallback(callbackID string) error {
	r.localOnly("TimeoutCallback")
	return r.local.TimeoutCallback(callbackID)
}

// CompletePendingTimers fires every pending timer locally; see
// [durabletest.LocalRunner.CompletePendingTimers].
func (r *Runner[I, O]) CompletePendingTimers() bool {
	r.localOnly("CompletePendingTimers")
	return r.local.CompletePendingTimers()
}

// OmitTokenOnCheckpoint withholds the checkpoint token locally; see
// [durabletest.LocalRunner.OmitTokenOnCheckpoint].
func (r *Runner[I, O]) OmitTokenOnCheckpoint(n int) {
	r.localOnly("OmitTokenOnCheckpoint")
	r.local.OmitTokenOnCheckpoint(n)
}

// localOnly fails the test when a step that only the local runner can
// perform is reached in cloud mode.
func (r *Runner[I, O]) localOnly(method string) {
	if r.local == nil {
		r.t.Helper()
		r.t.Fatalf("extest: %s is only available with the local runner; guard the call with runner.Local() and state why the cloud run does not need it", method)
	}
}

// exampleName is the example's directory name. A Go test binary runs with
// its package directory as the working directory.
func exampleName(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("extest: determine example directory: %v", err)
	}
	return filepath.Base(dir)
}

var (
	clientOnce sync.Once
	client     *lambda.Client
	clientErr  error
)

// cloudClient builds the Lambda client once per test binary from the
// default AWS configuration.
func cloudClient(t *testing.T) *lambda.Client {
	t.Helper()
	clientOnce.Do(func() {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			clientErr = err
			return
		}
		client = lambda.NewFromConfig(cfg)
	})
	if clientErr != nil {
		t.Fatalf("extest: load AWS config: %v", clientErr)
	}
	return client
}
