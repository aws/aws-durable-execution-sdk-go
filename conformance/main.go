// Command conformance is the single Lambda container image deployed as
// every one of the Go SDK conformance test suite's Lambda functions -
// see registry.go's doc comment for why one image serves all 148
// requirements rather than 148 separate images, and CONFORMANCE_TEST_ID
// below for how a single deployed function picks which requirement's
// handler to run.
//
// This file's Lambda Runtime API loop (getNextInvocation/
// postInvocationResponse/postInvocationError) is copied verbatim in
// structure from examples/simple-step-go/main.go (see that file's own
// doc comment for why every example in this repo hand-rolls the Runtime
// API against net/http rather than using aws-lambda-go) - duplicated
// here rather than factored into a shared internal package because it
// is genuinely tiny (~40 lines) and this repo's established convention
// (see docs/subagent-briefing.md) is that every examples/* entry is a
// fully self-contained, independently buildable Go module; this
// package follows that same convention for consistency, even though
// conformance/ is not itself one of the examples/ catalog entries.
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

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/awssdk"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func main() {
	runtimeAPI := os.Getenv("AWS_LAMBDA_RUNTIME_API")
	if runtimeAPI == "" {
		log.Fatal("AWS_LAMBDA_RUNTIME_API is not set - this binary must run inside the Lambda execution environment")
	}

	// CONFORMANCE_TEST_ID selects which registered requirement handler
	// THIS deployed function runs - set per-function in template.yaml's
	// Environment.Variables (one Lambda function per requirement id,
	// all sharing this same container image). Read once at cold start,
	// not per-invocation, matching how a real SDK user's function name/
	// configuration is also fixed at deploy time, not per-invocation.
	testID := os.Getenv("CONFORMANCE_TEST_ID")
	if testID == "" {
		log.Fatal("CONFORMANCE_TEST_ID is not set - every conformance Lambda function must set this to the requirement id it implements (see template.yaml)")
	}
	factory, ok := requirementHandlerFactories[testID]
	if !ok {
		log.Fatalf("no registered handler for CONFORMANCE_TEST_ID=%q - is handlers/*.go's init() actually registering it? (see registry.go's Register)", testID)
	}

	awsClient, err := awssdk.New(context.Background())
	if err != nil {
		log.Fatalf("failed to construct awssdk.Client: %v", err)
	}
	handler := factory(awsClient)

	client := &http.Client{Timeout: 0}

	for {
		requestID, rawEvent, err := getNextInvocation(client, runtimeAPI)
		if err != nil {
			log.Printf("ERROR fetching next invocation: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var input types.DurableExecutionInvocationInput
		if err := json.Unmarshal(rawEvent, &input); err != nil {
			log.Printf("ERROR unmarshaling invocation input: %v", err)
			postInvocationError(client, runtimeAPI, requestID, err)
			continue
		}

		output, err := handler(context.Background(), input)
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
