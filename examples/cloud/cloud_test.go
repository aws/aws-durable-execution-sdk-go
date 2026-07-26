// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build cloud

// Cloud integration tests for the deployed examples.
//
// Each example function is invoked with the shared event payload and its
// durable execution is polled until it reaches a terminal state, which is
// asserted against the expected terminal state documented in
// examples/README.md. The example list is parsed from build.sh so there is
// a single source of truth.
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
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// pollInterval is how often a pending durable execution is re-checked.
const pollInterval = 5 * time.Second

// executionTimeout bounds how long a single example may take to reach a
// terminal state after being invoked.
const executionTimeout = 15 * time.Minute

// expectFailed lists examples whose durable execution is documented to end
// FAILED (see examples/README.md). Every other example is expected to end
// SUCCEEDED.
var expectFailed = map[string]bool{
	"retry-exhaustion":                  true,
	"retry-callback":                    true,
	"child-ops-invalid-depth":           true,
	"context-validation-child":          true,
	"context-validation-step":           true,
	"context-validation-wait-condition": true,
	"handler-error":                     true,
}

// companions are deployed functions that only serve as invoke targets or
// callback submitters for other examples; they are never invoked directly.
var companions = map[string]bool{
	"retry-invoke-target":  true,
	"invoke-simple-target": true,
	"invoke-tenant-target": true,
	"callback-sender":      true,
}

// nonDurable lists plain Lambda examples: the synchronous invoke response
// is the terminal signal and there is no durable execution to poll.
var nonDurable = map[string]bool{
	"non-durable": true,
}

// loadExamples parses the EXAMPLES list out of build.sh so the harness
// stays in sync with what actually gets built and deployed.
func loadExamples(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var examples []string
	inList := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !inList {
			if strings.HasPrefix(line, `EXAMPLES="`) {
				inList = true
			}
			continue
		}
		if strings.HasPrefix(line, `"`) {
			break
		}
		if name := strings.TrimSpace(line); name != "" {
			examples = append(examples, name)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(examples) == 0 {
		return nil, fmt.Errorf("no examples found in %s", path)
	}
	return examples, nil
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
				return
			}

			want := types.ExecutionStatusSucceeded
			if expectFailed[name] {
				want = types.ExecutionStatusFailed
			}

			if out.DurableExecutionArn == nil {
				// A handler-level error can fail before a durable
				// execution is reported; that satisfies FAILED.
				if want == types.ExecutionStatusFailed && out.FunctionError != nil {
					return
				}
				t.Fatalf("no durable execution ARN returned (FunctionError: %s, payload: %s)",
					aws.ToString(out.FunctionError), string(out.Payload))
			}

			got := waitForTerminal(ctx, t, client, aws.ToString(out.DurableExecutionArn))
			if got != want {
				t.Fatalf("expected terminal status %s, got %s", want, got)
			}
		})
	}
}

// waitForTerminal polls the durable execution until it leaves RUNNING or
// the execution timeout elapses.
func waitForTerminal(ctx context.Context, t *testing.T, client *lambda.Client, arn string) types.ExecutionStatus {
	t.Helper()
	deadline := time.Now().Add(executionTimeout)
	for {
		out, err := client.GetDurableExecution(ctx, &lambda.GetDurableExecutionInput{
			DurableExecutionArn: aws.String(arn),
		})
		if err != nil {
			t.Fatalf("get durable execution %s: %v", arn, err)
		}
		if out.Status != types.ExecutionStatusRunning {
			return out.Status
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for terminal state of %s", executionTimeout, arn)
		}
		time.Sleep(pollInterval)
	}
}
