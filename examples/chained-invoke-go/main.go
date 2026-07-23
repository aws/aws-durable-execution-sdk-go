// Command chained-invoke-go is a minimal AWS Lambda custom runtime
// (provided.al2023-style container image) that runs a durable function
// demonstrating operations.Invoke, using the AWS Durable Execution SDK
// for Go. Implements the Lambda Runtime API directly against the
// standard library, mirroring this repo's other examples' main.go
// exactly (see any of their doc comments for why: no network access to
// fetch aws-lambda-go or the AWS SDK for Go v2 in this development
// environment).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/awssdk"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// OrderEvent, OrderResult, and handler are defined in handler.go, kept
// separate from this file's Lambda Runtime API plumbing so handler_test.go
// can exercise the handler via testing.LocalTestRunner without needing a
// real Lambda execution environment (see handler.go's doc comment).
//
// NOTE: unlike this repo's other examples, InventoryCheckFunctionARN
// (the target of operations.Invoke) is not deployed alongside this
// function as a mock - it targets a SECOND, separately deployed real
// Lambda function for real-cloud verification (see
// docs/remaining-work.md's writeup for this session's task closing
// docs/ts-sdk-examples-comparison.md's Invoke gap 4/8, and the exact
// ARN used). Locally, both this example's plain handler and its
// RetryingInventoryCheckHandler variant mock that target entirely via
// LocalTestRunner.RegisterFunction/RegisterDurableFunction (see
// handler_test.go).
//
// durableEntry is built lazily inside main() (not as a package-level
// var) because awssdk.New needs a context and can fail (e.g. if
// credential resolution fails) - this example previously used
// awscli.Client{} (a CLI-shim stopgap client, the CLI installation step
// it required has now been dropped from this example's Dockerfile too),
// matching examples/simple-step-go/main.go's and
// examples/run-in-child-context-go/main.go's identical fix - see
// docs/remaining-work.md §10 task 20's writeup on why awssdk (the real
// AWS SDK for Go v2-backed production client) replaced awscli/sigv4lambda
// as this repo's standard client for every deployable example.
var durableEntry func(ctx context.Context, input types.DurableExecutionInvocationInput) (types.DurableExecutionOutput, error)

func main() {
	runtimeAPI := os.Getenv("AWS_LAMBDA_RUNTIME_API")
	if runtimeAPI == "" {
		log.Fatal("AWS_LAMBDA_RUNTIME_API is not set - this binary must run inside the Lambda execution environment")
	}

	// awssdk.New resolves credentials and region via the standard AWS SDK
	// for Go v2 default chain, which correctly picks up this Lambda
	// function's execution-role credentials from the AWS_* environment
	// variables the Lambda runtime sets - see
	// examples/simple-step-go/main.go's identical pattern and awssdk's
	// package doc for the full rationale.
	awsClient, err := awssdk.New(context.Background())
	if err != nil {
		log.Fatalf("failed to construct awssdk.Client: %v", err)
	}
	durableEntry = durable.WithDurableExecution(RetryingInventoryCheckHandler, &durable.Config{Client: awsClient})

	client := &http.Client{Timeout: 0}

	for {
		requestID, rawEvent, err := getNextInvocation(client, runtimeAPI)
		if err != nil {
			log.Printf("ERROR fetching next invocation: %v", err)
			time.Sleep(time.Second)
			continue
		}

		fmt.Printf("RAW_EVENT requestId=%s bytes=%d payload=%s\n", requestID, len(rawEvent), string(rawEvent))

		var input types.DurableExecutionInvocationInput
		if err := json.Unmarshal(rawEvent, &input); err != nil {
			log.Printf("ERROR unmarshaling invocation input: %v", err)
			postInvocationError(client, runtimeAPI, requestID, err)
			continue
		}

		output, err := durableEntry(context.Background(), input)
		if err != nil {
			log.Printf("ERROR from durable entry point: %v", err)
			postInvocationError(client, runtimeAPI, requestID, err)
			continue
		}

		responseBytes, err := json.Marshal(output)
		if err != nil {
			log.Printf("ERROR marshaling durable output: %v", err)
			postInvocationError(client, runtimeAPI, requestID, err)
			continue
		}

		fmt.Printf("DURABLE_OUTPUT requestId=%s payload=%s\n", requestID, string(responseBytes))

		if err := postInvocationResponse(client, runtimeAPI, requestID, responseBytes); err != nil {
			log.Printf("ERROR posting invocation response: %v", err)
		}
	}
}

func getNextInvocation(client *http.Client, runtimeAPI string) (requestID string, event []byte, err error) {
	resp, err := client.Get(fmt.Sprintf("http://%s/2018-06-01/runtime/invocation/next", runtimeAPI))
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, err
	}

	requestID = resp.Header.Get("Lambda-Runtime-Aws-Request-Id")
	if requestID == "" {
		return "", nil, fmt.Errorf("missing Lambda-Runtime-Aws-Request-Id header")
	}
	return requestID, body, nil
}

func postInvocationResponse(client *http.Client, runtimeAPI, requestID string, result []byte) error {
	url := fmt.Sprintf("http://%s/2018-06-01/runtime/invocation/%s/response", runtimeAPI, requestID)
	resp, err := client.Post(url, "application/json", bytes.NewReader(result))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("runtime API response POST returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func postInvocationError(client *http.Client, runtimeAPI, requestID string, handlerErr error) error {
	url := fmt.Sprintf("http://%s/2018-06-01/runtime/invocation/%s/error", runtimeAPI, requestID)
	payload, _ := json.Marshal(map[string]string{
		"errorMessage": handlerErr.Error(),
		"errorType":    "HandlerError",
	})
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return nil
}
