// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build cloud

// Cloud test matrix for the deployed examples.
//
// Each example function is invoked with the shared event payload and its
// durable execution is polled until it reaches a terminal state. The
// terminal state, the result of a succeeding example, and the error type
// of a failing example are then checked against the expectation the
// example declares in expectations.go. The example list is parsed from
// build.sh so there is a single source of truth.
//
// The execution's operation signature is then compared with the golden
// file the example's local handler test asserts, using the comparison
// mode that test declares (see extest.ParseHandlerTest). An example whose
// deployed run differs by design records the deployed sequence in
// testdata/signature.cloud.golden; see extest.AssertCloudSignature for
// how that file is created and kept honest.
//
// This test runs one invocation per example with the shared event. The
// per-example tests under examples/<name>/handler_test.go carry each
// example's full assertions and run against the same deployed functions
// when the runner is switched to cloud mode (see examples/internal/extest).
//
// Prerequisites:
//   - Examples deployed: ./build.sh && sam build && sam deploy
//     (see .github/workflows/cloud-tests.yml)
//   - AWS credentials and region configured for the target test account
//   - FUNCTION_NAME_PREFIX set to the FunctionNamePrefix used at deploy
//     time (empty for none)
//
// Run:
//
//	go test -tags cloud ./cloud -run TestExamples -v -timeout 80m -parallel 8
package cloud

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// pollInterval is how often a pending durable execution is re-checked.
const pollInterval = 5 * time.Second

// executionTimeout bounds how long a single example may take to reach a
// terminal state after being invoked.
const executionTimeout = 15 * time.Minute

// nonDurable lists plain Lambda examples: the synchronous invoke response
// is both the terminal signal and the result, and there is no durable
// execution to poll.
var nonDurable = map[string]bool{
	"non-durable": true,
}

func TestExamples(t *testing.T) {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatalf("load AWS config: %v", err)
	}
	client := lambda.NewFromConfig(cfg)
	prefix := os.Getenv("FUNCTION_NAME_PREFIX")

	payload, err := os.ReadFile("../event.json")
	if err != nil {
		t.Fatalf("read event payload: %v", err)
	}

	examples, err := loadExamples("../build.sh")
	if err != nil {
		t.Fatalf("load example list: %v", err)
	}

	for _, name := range examples {
		if companions[name] {
			continue
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// expectations_test.go already fails the unit run for a
			// missing entry; failing here as well keeps a cloud-only run
			// from passing an unverified example.
			exp, ok := expectations[name]
			if !ok {
				t.Fatalf("no expectation declared for %s in expectations.go", name)
			}

			functionName := prefix + "go-" + name
			out, err := client.Invoke(ctx, &lambda.InvokeInput{
				FunctionName: aws.String(functionName),
				Qualifier:    aws.String("$LATEST"),
				Payload:      payload,
			})
			if err != nil {
				t.Fatalf("invoke %s: %v", functionName, err)
			}

			if nonDurable[name] {
				if out.FunctionError != nil {
					t.Fatalf("expected clean invoke, got FunctionError %s: %s",
						aws.ToString(out.FunctionError), string(out.Payload))
				}
				exp.assert(t, outcome{result: string(out.Payload)})
				return
			}

			if out.DurableExecutionArn == nil {
				t.Fatalf("no durable execution ARN returned (FunctionError: %s, payload: %s)",
					aws.ToString(out.FunctionError), string(out.Payload))
			}

			arn := aws.ToString(out.DurableExecutionArn)
			final := waitForTerminal(ctx, t, client, arn)
			got := outcome{
				failed: final.Status != types.ExecutionStatusSucceeded,
				result: aws.ToString(final.Result),
			}
			if final.Error != nil {
				got.errorType = aws.ToString(final.Error.ErrorType)
			}
			exp.assert(t, got)

			assertSignature(t, client, name, arn)
		})
	}
}

// assertSignature compares the operation signature of the finished
// execution with the golden the example's handler test asserts, in the
// mode that test declares for the default golden.
func assertSignature(t *testing.T, client *lambda.Client, name, arn string) {
	t.Helper()
	dir := "../" + name
	ht, err := extest.ParseHandlerTest(dir + "/handler_test.go")
	if err != nil {
		t.Fatalf("read signature declaration: %v", err)
	}
	mode, err := ht.ModeFor(extest.GoldenPath)
	if err != nil {
		t.Fatalf("read signature declaration: %v", err)
	}
	result := durabletest.NewCloudRunner(client, name).RunWithArn(t, arn)
	extest.AssertCloudSignature(t, result, mode, dir)
}

// waitForTerminal polls the durable execution until it leaves RUNNING or
// the execution timeout elapses, and returns the final description with
// its result and error included.
func waitForTerminal(ctx context.Context, t *testing.T, client *lambda.Client, arn string) *lambda.GetDurableExecutionOutput {
	t.Helper()
	deadline := time.Now().Add(executionTimeout)
	for {
		out, err := client.GetDurableExecution(ctx, &lambda.GetDurableExecutionInput{
			DurableExecutionArn:  aws.String(arn),
			IncludeExecutionData: aws.Bool(true),
		})
		if err != nil {
			t.Fatalf("get durable execution %s: %v", arn, err)
		}
		if out.Status != types.ExecutionStatusRunning {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for terminal state of %s", executionTimeout, arn)
		}
		time.Sleep(pollInterval)
	}
}
