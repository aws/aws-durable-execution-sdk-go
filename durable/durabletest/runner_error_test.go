// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// The assertion helpers take testing.TB, so benchmarks and fuzz tests can
// call them.
var (
	_ func(testing.TB, *durabletest.TestResult, string)                           = durabletest.AssertGoldenSignature
	_ func(testing.TB, *durabletest.TestResult, string)                           = durabletest.AssertGoldenSignatureUnordered
	_ func(testing.TB, *durabletest.TestResult, []durabletest.OperationSignature) = durabletest.AssertSignatureContains
	_ func(testing.TB, *durabletest.TestResult, []durabletest.OperationSignature) = durabletest.AssertSignatureExcludes
)

// TestRunnerOutsideTest runs a handler from a plain main package with
// go run. The program uses no testing package API: it checks the
// runner's error, then the error ResultAs returns, and prints the value.
func TestRunnerOutsideTest(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs a separate program")
	}
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go command not found")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, goTool, "run", "./testdata/standalone")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go run: %v\n%s", err, out)
	}
	if got, want := strings.TrimSpace(string(out)), "result: hello, world"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestLocalRunnerErrorContract(t *testing.T) {
	t.Run("event does not marshal", func(t *testing.T) {
		handler := func(_ durable.Context, _ chan int) (string, error) { return "", nil }
		runner := durabletest.NewLocalRunner(handler)

		result, err := runner.Run(make(chan int))
		if err == nil || !strings.Contains(err.Error(), "marshal event") {
			t.Errorf("Run: err = %v, want a marshal error", err)
		}
		if result != nil {
			t.Errorf("Run: result = %+v, want nil", result)
		}

		result, err = runner.RunUntilComplete(make(chan int))
		if err == nil || !strings.Contains(err.Error(), "marshal event") {
			t.Errorf("RunUntilComplete: err = %v, want a marshal error", err)
		}
		if result != nil {
			t.Errorf("RunUntilComplete: result = %+v, want nil", result)
		}
	})

	t.Run("invocation error", func(t *testing.T) {
		handler := func(ctx durable.Context, _ string) (string, error) {
			return durable.Step(ctx, "s", func(durable.StepContext) (string, error) { return "v", nil })
		}
		runner := durabletest.NewLocalRunner(handler, durable.WithExecutionClient(failingClient{}))

		result, err := runner.Run("x")
		if err == nil {
			t.Fatalf("Run: err = nil, result = %+v; want the invocation error", result)
		}
		if result != nil {
			t.Errorf("Run: result = %+v, want nil", result)
		}

		result, err = runner.RunUntilComplete("x")
		if err == nil {
			t.Fatalf("RunUntilComplete: err = nil, result = %+v; want the invocation error", result)
		}
		if result != nil {
			t.Errorf("RunUntilComplete: result = %+v, want nil", result)
		}
	})

	t.Run("handler error is a result", func(t *testing.T) {
		handler := func(_ durable.Context, _ string) (string, error) {
			return "", errors.New("card declined")
		}
		result, err := durabletest.NewLocalRunner(handler).RunUntilComplete("x")
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if result.Status != durabletest.Failed {
			t.Fatalf("status = %s, want FAILED", result.Status)
		}
		if result.Error == nil || result.Error.Type != "Error" || result.Error.Message != "card declined" {
			t.Errorf("result.Error = %+v, want Error: card declined", result.Error)
		}
	})

	t.Run("blocked run is a result", func(t *testing.T) {
		handler := func(ctx durable.Context, _ string) (string, error) {
			cb, err := durable.CreateCallback[string](ctx, "approval")
			if err != nil {
				return "", err
			}
			return cb.Result(ctx)
		}
		result, err := durabletest.NewLocalRunner(handler).RunUntilComplete("x")
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if result.Status != durabletest.Pending {
			t.Errorf("status = %s, want PENDING", result.Status)
		}
	})

	t.Run("invocation cap is a result", func(t *testing.T) {
		handler := func(ctx durable.Context, _ string) (string, error) {
			for i := range 5 {
				if err := durable.Wait(ctx, "", time.Duration(i+1)*time.Second); err != nil {
					return "", err
				}
			}
			return "done", nil
		}
		result, err := durabletest.NewLocalRunner(handler).RunUntilComplete("x", durabletest.WithMaxInvocations(2))
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if !result.CapReached {
			t.Errorf("CapReached = false, want true (status %s)", result.Status)
		}
	})
}

// failingClient is an execution client whose every call fails.
type failingClient struct{}

func (failingClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, errors.New("execution client unavailable")
}

func (failingClient) Checkpoint(context.Context, durable.CheckpointInput) (durable.CheckpointOutput, error) {
	return durable.CheckpointOutput{}, errors.New("execution client unavailable")
}

func TestResultAsReportsHandlerError(t *testing.T) {
	result := &durabletest.TestResult{
		Status: durabletest.Failed,
		Error:  &durabletest.TestError{Type: "CardDeclinedError", Message: "card declined"},
	}
	_, err := durabletest.ResultAs[string](result)
	if err == nil {
		t.Fatal("ResultAs on FAILED returned nil error")
	}
	for _, want := range []string{"FAILED", "CardDeclinedError", "card declined"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %q, want it to contain %q", err, want)
		}
	}
}

func TestCloudRunnerErrorContract(t *testing.T) {
	const arn = "arn:aws:lambda:us-east-1:111:durable-execution:test"
	newRunner := func(api *fakeCloudAPI) *durabletest.CloudRunner {
		return durabletest.NewCloudRunner(api, "fn:$LATEST",
			durabletest.WithPollInterval(time.Millisecond),
			durabletest.WithTimeout(time.Minute))
	}
	runningForever := func(_ context.Context, _ *lambda.GetDurableExecutionInput) (*lambda.GetDurableExecutionOutput, error) {
		return &lambda.GetDurableExecutionOutput{Status: types.ExecutionStatusRunning}, nil
	}

	cases := []struct {
		name string
		api  *fakeCloudAPI
		ctx  func(t *testing.T) context.Context
		want string
	}{
		{
			name: "event does not marshal",
			api:  &fakeCloudAPI{},
			want: "marshal event",
		},
		{
			name: "invoke fails",
			api: &fakeCloudAPI{invokeFunc: func(context.Context, *lambda.InvokeInput) (*lambda.InvokeOutput, error) {
				return nil, errors.New("throttled")
			}},
			want: "throttled",
		},
		{
			name: "no execution ARN",
			api: &fakeCloudAPI{invokeFunc: func(context.Context, *lambda.InvokeInput) (*lambda.InvokeOutput, error) {
				return &lambda.InvokeOutput{}, nil
			}},
			want: "no DurableExecutionArn",
		},
		{
			name: "polling fails",
			api: &fakeCloudAPI{getExecutionFunc: func(context.Context, *lambda.GetDurableExecutionInput) (*lambda.GetDurableExecutionOutput, error) {
				return nil, errors.New("access denied")
			}},
			want: "access denied",
		},
		{
			name: "context ends",
			api:  &fakeCloudAPI{getExecutionFunc: runningForever},
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
				t.Cleanup(cancel)
				return ctx
			},
			want: "context deadline exceeded",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			if tc.ctx != nil {
				ctx = tc.ctx(t)
			}
			var event any = "input"
			if tc.name == "event does not marshal" {
				event = make(chan int)
			}
			result, err := newRunner(tc.api).Run(ctx, event)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want it to contain %q", err, tc.want)
			}
			if result != nil {
				t.Errorf("result = %+v, want nil", result)
			}
		})
	}

	t.Run("RunWithArn context ends", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		result, err := newRunner(&fakeCloudAPI{getExecutionFunc: runningForever}).RunWithArn(ctx, arn)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
		if result != nil {
			t.Errorf("result = %+v, want nil", result)
		}
	})

	t.Run("failed execution is a result", func(t *testing.T) {
		api := &fakeCloudAPI{getExecutionFunc: func(context.Context, *lambda.GetDurableExecutionInput) (*lambda.GetDurableExecutionOutput, error) {
			return &lambda.GetDurableExecutionOutput{
				Status: types.ExecutionStatusFailed,
				Error:  &types.ErrorObject{ErrorType: aws.String("Error"), ErrorMessage: aws.String("boom")},
			}, nil
		}}
		result, err := newRunner(api).RunWithArn(t.Context(), arn)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if result.Status != durabletest.Failed || result.Error == nil || result.Error.Message != "boom" {
			t.Errorf("result = %+v, want FAILED with message boom", result)
		}
	})

	t.Run("ctx reaches every call", func(t *testing.T) {
		type key struct{}
		ctx := context.WithValue(t.Context(), key{}, "marker")
		var calls []string
		check := func(name string, ctx context.Context) {
			if ctx.Value(key{}) != "marker" {
				t.Errorf("%s did not receive the caller's context", name)
			}
			calls = append(calls, name)
		}
		polls := 0
		api := &fakeCloudAPI{
			invokeFunc: func(ctx context.Context, _ *lambda.InvokeInput) (*lambda.InvokeOutput, error) {
				check("Invoke", ctx)
				return &lambda.InvokeOutput{DurableExecutionArn: aws.String(arn)}, nil
			},
			getExecutionFunc: func(ctx context.Context, _ *lambda.GetDurableExecutionInput) (*lambda.GetDurableExecutionOutput, error) {
				check("GetDurableExecution", ctx)
				polls++
				if polls < 2 {
					return &lambda.GetDurableExecutionOutput{Status: types.ExecutionStatusRunning}, nil
				}
				return &lambda.GetDurableExecutionOutput{Status: types.ExecutionStatusSucceeded, Result: aws.String(`"ok"`)}, nil
			},
			getHistoryFunc: func(ctx context.Context, _ *lambda.GetDurableExecutionHistoryInput) (*lambda.GetDurableExecutionHistoryOutput, error) {
				check("GetDurableExecutionHistory", ctx)
				return &lambda.GetDurableExecutionHistoryOutput{}, nil
			},
		}
		if _, err := newRunner(api).Run(ctx, "input"); err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(calls, ","); got != "Invoke,GetDurableExecution,GetDurableExecution,GetDurableExecutionHistory" {
			t.Errorf("calls = %s", got)
		}
	})
}
