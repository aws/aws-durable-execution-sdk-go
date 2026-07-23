// Command echo-target is a MINIMAL, DURABLE echo target Lambda function
// used as the real invoke target for the Go SDK conformance test
// harness's Invoke suite (conformance/handlers/invoke_5_*.go,
// test-requirements/invoke/5-1.yaml through 5-16.yaml).
//
// # Update: now a genuine durable function - closing the
// # ChainedInvokeStartedDetails.DurableExecutionArn gap
//
// This target was ORIGINALLY a deliberately plain (non-durable) Lambda
// function - see this file's own git history for the original design
// doc, retained below in spirit. That design caused every one of this
// suite's 16 requirements (bar 5-8, whose own ExpectedExecutionHistory
// does not assert this field) to genuinely, permanently fail on
// ChainedInvokeStartedDetails.DurableExecutionArn: the official API
// reference's own documented Pattern for that field requires the
// INVOKED TARGET to itself be a durable execution (confirmed, at the
// time, by reading that doc directly) - and this SDK's own
// operations.Invoke places no such restriction on its own target
// (ChainedInvokeOptions only ever carries FunctionName/TenantID),
// meaning a plain target was never a real requirement of this SDK's own
// design, only an accidental, avoidable choice made when this suite was
// first built.
//
// A LATER session settled this conclusively via a real, live experiment
// (prompted by explicit user skepticism: "all other SDKs are working
// fine, so try to find the issue" - the same discipline that already
// twice turned an apparent "confirmed limitation" into a real, fixable
// SDK/deployment bug): a genuinely durable, temporary echo function was
// deployed and invoked via operations.Invoke, and its real
// ChainedInvokeStartedDetails.DurableExecutionArn came back fully
// populated - conclusively proving the field's absence was solely a
// consequence of THIS target's own deliberate plainness, not any
// limitation of operations.Invoke, the backend, or this SDK's wire
// protocol. See docs/remaining-work.md's own Invoke-suite section for
// the full experiment writeup and evidence.
//
// This target is now wrapped in durable.WithDurableExecution, using a
// single Step to echo its input - the smallest possible durable
// function shape, chosen deliberately to keep behavior (and this
// suite's own expected checkpoint counts, beyond the target's own
// internal history, which no Invoke5N* requirement inspects) as close
// to the original plain design as a durable wrapper allows. The
// reserved __echoTargetControl.fail branch (see below) is preserved
// unchanged - it now causes the Step itself to fail, which
// durable.WithDurableExecution surfaces as a genuine ExecutionFailed
// outcome, which the real backend still reports back to the CALLER as
// an ordinary ChainedInvokeFailed event, exactly as it did when this
// target was plain (a target's own internal durability is invisible to
// its caller's operations.Invoke beyond this one field).
//
// # Design: one smart echo target, not several single-purpose ones
//
// Every one of the 16 Invoke suite requirements' own "invocations" prose
// (test-requirements/invoke/5-*.yaml) describes the target as an ECHO
// function: it receives some JSON payload and returns it back unchanged
// (5-16's uppercasing happens on the CALLER's own side via a custom
// resultSerdes option - see handlers/invoke_5_16.go - never inside this
// target). Two requirements (5-5, 5-6) additionally need the target to
// genuinely FAIL instead of echoing, to exercise operations.Invoke's
// ChainedInvokeFailed/InvokeFailedError path against a real backend-
// reported failure.
//
// Rather than deploying a second Lambda function just for those two
// requirements, this target branches on one optional, reserved field in
// its own input payload: EchoTargetControl.Fail. Any payload that is not
// a JSON object containing this exact reserved key (i.e. every ordinary
// echo-only requirement's input - a bare string, number, null, array, or
// object without this key) is completely unaffected and echoed back
// byte-for-byte unchanged; only a request that deliberately opts in via
// {"__echoTargetControl": {"fail": true}} (or a string/object payload
// that happens to itself be shaped that way, which is an acceptable,
// documented, and vanishingly unlikely collision for a test harness) hits
// the failure branch. This mirrors inventory-check-target's own
// "stateless, derive behavior purely from this request's own payload"
// design principle (see that target's own main.go doc for the identical
// rationale) rather than external counter/environment-variable state,
// while covering both the "always echo" and "sometimes fail" needs of
// this ONE suite from a single deployed function - simpler to build,
// deploy, and reason about than two nearly-identical targets, and this
// suite (unlike chained-invoke-go's retry example) never needs a
// multi-attempt failure count, only an unconditional one, so the
// reserved-field branch is intentionally simpler than
// InventoryCheckRequest's own Attempt/FailUntilAttempt pair.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// echoTargetControlKey is the single reserved top-level JSON object key
// this target inspects before deciding to echo vs. fail. Deliberately an
// unlikely, namespaced-looking string (leading double underscore + this
// target's own name) rather than a short, generic word like "fail" -
// minimizing the already-small chance a genuine echo-payload requirement
// (e.g. 5-3's arbitrary nested JSON, 5-7's large payload) accidentally
// collides with it.
const echoTargetControlKey = "__echoTargetControl"

// echoTargetControl is the optional, reserved control payload shape a
// caller can nest under echoTargetControlKey to make this target do
// something other than plain echo. Fail is the only control this suite's
// 16 requirements need (see 5-5/5-6's own "invoked function throws an
// error" scenario) - extend this struct, not add a new target, if a
// future requirement needs a different simulated behavior (e.g. a sleep
// for a timeout scenario), following this same single-smart-target
// design (see this file's own top-level doc for the rationale).
type echoTargetControl struct {
	Fail bool `json:"fail"`
}

// handler is this target's entire business logic, now wrapped in
// durable.WithDurableExecution (see this file's own top-level doc for
// why) - either echoes rawEvent back unchanged, or returns a genuine
// error for the reserved control-key failure branch, exactly mirroring
// handle()'s own pre-durable behavior.
//
// # Deliberately NOT wrapped in an operations.Step
//
// An earlier version of this handler wrapped the echo/fail logic in a
// single operations.Step, on the assumption that would be the most
// natural way to add "real work" to a durable function. That turned out
// to be a genuine, real bug for requirement 5-7 ("Invoke large
// payload"): a STEP's own checkpointed output payload is subject to a
// SEPARATE, SMALLER real backend limit (262144 bytes / 256 KiB,
// confirmed via a real deployed InvalidParameterValueException: "STEP
// output payload size must be less than or equal to 262144 bytes") than
// the EXECUTION-level result this handler's own return value ultimately
// becomes (which 5-7's own 512 KiB payload, comfortably under THIS
// SDK's separate, larger 750 KiB client-side Invoke threshold, does not
// exceed). Since this target's entire purpose is to be as thin as
// possible - it has no actual "work" to checkpoint independently of its
// own overall result - there is no reason to introduce a STEP at all;
// echoing directly in the handler body means the ONLY checkpoint this
// target's execution ever produces is its own terminal
// ExecutionSucceeded/ExecutionFailed, whose payload size limit (per this
// requirement's own real, successful deployment at up to 512 KiB) is
// evidently the larger of the two.
//
// Echoing the RAW bytes (json.RawMessage) rather than round-tripping
// through unmarshal-into-any/marshal is deliberate: it guarantees
// byte-for-byte fidelity for every input shape this suite exercises
// (5-3's nested arrays/objects, 5-4's literal null, 5-7's large string) -
// there is no risk of e.g. numeric precision drift or map key reordering
// from an intermediate Go representation, since the bytes themselves are
// never actually parsed into a Go value at all except for the narrow,
// best-effort control-key probe below.
func handler(rawEvent json.RawMessage, dc types.DurableContext) (json.RawMessage, error) {
	var probe map[string]json.RawMessage
	// A non-object payload (string/number/null/array) simply fails this
	// unmarshal into a map - deliberately ignored rather than
	// propagated, exactly like the pre-durable handle()'s own identical
	// check.
	if err := json.Unmarshal(rawEvent, &probe); err == nil {
		if rawControl, ok := probe[echoTargetControlKey]; ok {
			var control echoTargetControl
			if err := json.Unmarshal(rawControl, &control); err == nil && control.Fail {
				return nil, errors.New("echo-target: simulated failure (requested via __echoTargetControl.fail)")
			}
		}
	}
	return rawEvent, nil
}

func main() {
	runtimeAPI := os.Getenv("AWS_LAMBDA_RUNTIME_API")
	if runtimeAPI == "" {
		log.Fatal("AWS_LAMBDA_RUNTIME_API is not set - this binary must run inside the Lambda execution environment")
	}

	// awssdk.New resolves credentials/region via the standard AWS SDK
	// for Go v2 default chain - the identical pattern every other
	// durable function's main.go in this repo already uses (see e.g.
	// examples/simple-step-go/main.go's own doc comment).
	awsClient, err := awssdk.New(context.Background())
	if err != nil {
		log.Fatalf("failed to construct awssdk.Client: %v", err)
	}
	durableEntry := durable.WithDurableExecution(handler, &durable.Config{Client: awsClient})

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
