// Command inventory-check-target is a MINIMAL, PLAIN (non-durable)
// Lambda function used as the real invoke target for
// examples/chained-invoke-go's RetryingInventoryCheckHandler cloud
// verification (docs/remaining-work.md, closing
// docs/ts-sdk-examples-comparison.md's Invoke gap 4/8, with-retry/invoke).
//
// This is deliberately NOT a durable-execution function (no
// pkg/durable/durable.WithDurableExecution wrapping, no checkpoint.Client
// at all) - operations.Invoke's target function can be ANY Lambda
// function, durable or standard (see pkg/durable/operations/invoke.go's
// own doc: "calls another Lambda function - durable or standard"), and a
// plain function is both sufficient and simpler for this specific
// purpose: giving RetryingInventoryCheckHandler's manual retry loop
// something REAL to call that can be made to genuinely fail a
// caller-controlled number of times, so the retry loop's multi-attempt
// behavior is exercised against actual cross-function Lambda invocation,
// not just a local mock.
//
// Implements the Lambda Runtime API directly against the standard
// library, mirroring every other example's main.go in this repo (see any
// of their doc comments for why: no network access to fetch
// aws-lambda-go in this development environment) - simplified here since
// this target has no durable-execution plumbing to bootstrap at all,
// just a plain synchronous request/response handler.
//
// # Why stateless (no counter, no environment variable) - a deliberate,
// simpler design than "an env-var-configured failure count"
//
// Rather than persisting a call-counter as external state (e.g. in an
// env var, which a Lambda function cannot even mutate for itself across
// invocations, or DynamoDB, which would need its own IAM role/table
// provisioning for a one-off verification target), this target derives
// whether to fail PURELY from its own request payload's FailUntilAttempt
// field, compared against the SAME Attempt field
// RetryingInventoryCheckHandler's caller-side loop already sends (see
// chained-invoke-go/handler.go's InventoryCheckRequest.Attempt, added
// specifically to support this real-cloud verification target). This
// keeps the target fully stateless and idempotent - calling it twice
// with the same input always produces the same output, which is both
// simpler to reason about and, not coincidentally, exactly the
// replay-safety property every operation in this SDK already depends on
// for its OWN correctness - while still genuinely exercising a
// multi-attempt retry loop end-to-end against a real second Lambda
// function: attempt 1 and 2 fail because Attempt <= FailUntilAttempt,
// attempt 3 succeeds because it's the first attempt number greater than
// FailUntilAttempt.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

// InventoryCheckRequest/Response mirror
// examples/chained-invoke-go/handler.go's identically-named types
// exactly (this is a separate Go module with no dependency on that
// package, so the shapes are duplicated rather than imported - the wire
// JSON shape is what has to match, not the Go type identity).
type InventoryCheckRequest struct {
	SKU              string `json:"sku"`
	Qty              int    `json:"qty"`
	Attempt          int    `json:"attempt"`
	FailUntilAttempt int    `json:"failUntilAttempt"`
}

type InventoryCheckResponse struct {
	Available bool `json:"available"`
}

func main() {
	runtimeAPI := os.Getenv("AWS_LAMBDA_RUNTIME_API")
	if runtimeAPI == "" {
		log.Fatal("AWS_LAMBDA_RUNTIME_API is not set - this binary must run inside the Lambda execution environment")
	}

	client := &http.Client{Timeout: 0}

	for {
		requestID, rawEvent, err := getNextInvocation(client, runtimeAPI)
		if err != nil {
			log.Printf("ERROR fetching next invocation: %v", err)
			time.Sleep(time.Second)
			continue
		}

		fmt.Printf("RAW_EVENT requestId=%s bytes=%d payload=%s\n", requestID, len(rawEvent), string(rawEvent))

		respBytes, handlerErr := handle(rawEvent)
		if handlerErr != nil {
			log.Printf("ERROR from handler: %v", handlerErr)
			postInvocationError(client, runtimeAPI, requestID, handlerErr)
			continue
		}

		fmt.Printf("RESPONSE requestId=%s payload=%s\n", requestID, string(respBytes))

		if err := postInvocationResponse(client, runtimeAPI, requestID, respBytes); err != nil {
			log.Printf("ERROR posting invocation response: %v", err)
		}
	}
}

// handle implements this target's entire business logic: fail
// (returning a Go error, which postInvocationError below reports to
// Lambda as a genuine FunctionError - exactly what a real chained-invoke
// target's real failure looks like on the wire, per
// operations.Invoke's own confirmed flowchart) if the request's Attempt
// has not yet exceeded FailUntilAttempt, otherwise succeed.
func handle(rawEvent []byte) ([]byte, error) {
	var req InventoryCheckRequest
	if err := json.Unmarshal(rawEvent, &req); err != nil {
		return nil, fmt.Errorf("inventory-check-target: unmarshaling request: %w", err)
	}

	if req.Attempt <= req.FailUntilAttempt {
		return nil, fmt.Errorf("inventory-check-target: simulated transient failure (attempt %d, failUntilAttempt %d)", req.Attempt, req.FailUntilAttempt)
	}

	resp := InventoryCheckResponse{Available: true}
	return json.Marshal(resp)
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
