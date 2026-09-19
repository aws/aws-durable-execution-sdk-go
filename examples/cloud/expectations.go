// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Package cloud contains the cloud smoke test for the deployed examples.
// The test itself lives in cloud_test.go behind the `cloud` build tag.
// This file declares what each example is expected to produce; it has no
// build tag so that expectations_test.go can check the table against the
// example list in every ordinary test run.
package cloud

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

// expectation declares how the smoke test verifies one example once its
// execution has reached a terminal state. Every example in build.sh that is
// not a companion has exactly one entry in expectations; the unit test in
// expectations_test.go fails when an example is missing or an entry is
// malformed, so a new example cannot pass by default.
type expectation struct {
	// failed is true when the execution is documented to end FAILED. The
	// error type is then asserted with errorType.
	failed bool

	// result is the expected handler result as JSON. Both sides are
	// decoded before comparison, so key order and whitespace do not
	// matter. A succeeding example sets exactly one of result and check.
	result string

	// errorType is the expected error type name of a failing example.
	errorType string

	// check verifies a result that is legitimately nondeterministic:
	// timestamps, measured durations, or counts that depend on
	// scheduling. It receives the raw result JSON. nondeterministic states
	// which part of the result varies and why; an entry with a check but
	// no reason is rejected by validate.
	check            func(t testing.TB, result string)
	nondeterministic string
}

// outcome is what the smoke test observed for one example.
type outcome struct {
	failed    bool
	result    string
	errorType string
}

// validate reports the first way the entry is malformed, or nil.
func (e expectation) validate() error {
	if e.failed {
		if e.errorType == "" {
			return fmt.Errorf("failing example must declare errorType")
		}
		if e.result != "" || e.check != nil {
			return fmt.Errorf("failing example must not declare a result or check")
		}
		return nil
	}
	if e.errorType != "" {
		return fmt.Errorf("succeeding example must not declare errorType")
	}
	if (e.result == "") == (e.check == nil) {
		return fmt.Errorf("succeeding example must declare exactly one of result and check")
	}
	if e.check != nil && e.nondeterministic == "" {
		return fmt.Errorf("check requires a nondeterministic reason")
	}
	if e.result != "" && !json.Valid([]byte(e.result)) {
		return fmt.Errorf("result is not valid JSON")
	}
	return nil
}

// assert compares the observed outcome with the expectation.
func (e expectation) assert(t testing.TB, got outcome) {
	t.Helper()
	if got.failed != e.failed {
		want, have := "SUCCEEDED", "FAILED"
		if e.failed {
			want, have = have, want
		}
		t.Fatalf("expected terminal status %s, got %s (error type %q, result %s)", want, have, got.errorType, got.result)
	}
	if e.failed {
		if got.errorType != e.errorType {
			t.Fatalf("expected error type %q, got %q", e.errorType, got.errorType)
		}
		return
	}
	if e.check != nil {
		e.check(t, got.result)
		return
	}
	assertJSONEqual(t, e.result, got.result)
}

// assertJSONEqual fails the test unless want and got decode to the same
// value.
func assertJSONEqual(t testing.TB, want, got string) {
	t.Helper()
	var wantV, gotV any
	if err := json.Unmarshal([]byte(want), &wantV); err != nil {
		t.Fatalf("expected result is not valid JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(got), &gotV); err != nil {
		t.Fatalf("result is not valid JSON: %v\n%s", err, got)
	}
	if !reflect.DeepEqual(wantV, gotV) {
		t.Fatalf("result mismatch\nwant: %s\ngot:  %s", want, got)
	}
}

// resultObject decodes a JSON object result. It fails the test when the
// result is not an object.
func resultObject(t testing.TB, result string) map[string]any {
	t.Helper()
	var obj map[string]any
	if err := json.Unmarshal([]byte(result), &obj); err != nil {
		t.Fatalf("result is not a JSON object: %v\n%s", err, result)
	}
	return obj
}

// assertFields fails the test unless every key in want is present in obj
// with an equal value. Values are compared as decoded JSON, so numbers in
// want must be float64.
func assertFields(t testing.TB, obj map[string]any, want map[string]any) {
	t.Helper()
	for k, w := range want {
		g, ok := obj[k]
		if !ok {
			t.Fatalf("result lacks field %q: %v", k, obj)
		}
		if !reflect.DeepEqual(w, g) {
			t.Fatalf("field %q: want %v, got %v", k, w, g)
		}
	}
}

// assertPositiveDuration checks that obj[key] is a positive number. It is
// used for measured elapsed times, which vary from run to run.
func assertPositiveDuration(t testing.TB, obj map[string]any, key string) {
	t.Helper()
	v, ok := obj[key].(float64)
	if !ok || v <= 0 {
		t.Fatalf("expected positive %s, got %v", key, obj[key])
	}
}

// companions are deployed functions that only serve as invoke targets or
// callback submitters for other examples; they are never invoked directly
// and have no expectation.
var companions = map[string]bool{
	"retry-invoke-target":  true,
	"invoke-simple-target": true,
	"invoke-tenant-target": true,
	"callback-sender":      true,
}

// loadExamples parses the EXAMPLES list out of build.sh so the smoke test
// and the expectation table stay in sync with what gets built and
// deployed.
func loadExamples(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

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

// checkForceCheckpointStepRetry verifies the force-checkpoint-step-retry
// example. The force-checkpoint examples return the Parallel result
// serialized as a JSON string, so the result is decoded twice. Item 0 is
// the long-running branch, which succeeds; item 1 is the retrying branch,
// which fails (status 2) with a StepError.
func checkForceCheckpointStepRetry(t testing.TB, result string) {
	t.Helper()
	var inner string
	if err := json.Unmarshal([]byte(result), &inner); err != nil {
		t.Fatalf("result is not a JSON string: %v\n%s", err, result)
	}
	obj := resultObject(t, inner)
	items, ok := obj["Items"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("expected 2 items, got %v", obj["Items"])
	}
	first, _ := items[0].(map[string]any)
	assertFields(t, first, map[string]any{"Name": "long-running", "Result": "long-complete"})
	second, _ := items[1].(map[string]any)
	assertFields(t, second, map[string]any{"Name": "retrying", "Status": float64(2)})
	errObj, _ := second["Err"].(map[string]any)
	assertFields(t, errObj, map[string]any{"ErrorType": "StepError"})
}

// expectations is the single table of expected outcomes, keyed by example
// name. It is the source of truth for the "Expected Terminal State" column
// in examples/README.md.
var expectations = map[string]expectation{
	"attempt-fallback": {result: `{"supplier":"supplier-c","attempt":3,"status":"backordered"}`},
	"chained-invoke":   {result: `{"orderId":"ORD-12345","stages":[{"orderId":"ORD-12345","stage":"validate","status":"completed"},{"orderId":"ORD-12345","stage":"process","status":"completed"},{"orderId":"ORD-12345","stage":"confirm","status":"completed"}],"complete":true}`},

	"child-context-basic":                 {result: `"child step completed"`},
	"child-context-checkpoint-size-limit": {result: `{"success":true,"totalIterations":5}`},
	"child-context-error-data-propagation": {
		result: `{"found":true,"errorData":"{\"reason\":\"operator-cancelled\"}","errorType":"ChildContextError"}`,
	},
	"child-context-error-propagation": {
		result: `{"found":true,"errMsg":"durable: child context \"outer-child\" failed: ChildContextError: durable: child context \"inner-child\" failed: StepError: durable: step \"throw-error\" failed after 1 attempts: Error: intentional nested failure","depth":2,"isOpErr":true,"isChild":true}`,
	},
	"child-context-failing-step":         {result: `{"success":true}`},
	"child-context-large-data":           {result: `{"success":true,"summary":{"totalDataSize":281600,"stepsExecuted":5,"childContextUsed":true},"dataIntegrityHash":281600}`},
	"child-context-nested-blocks":        {result: `{"grandchildValue":"deep-value","childValue":"child-wraps(deep-value)","parentValue":"parent-wraps(child-wraps(deep-value))"}`},
	"child-context-serdes":               {result: `"HELLO"`},
	"child-context-serdes-large-payload": {result: `{"payloadLength":307200,"payloadHash":"c10a028e3fa345bba74b4603acfef02aacd68568aa8396fa95287df1a27c5bdc"}`},
	"child-context-serdes-virtual":       {result: `"HELLO FROM VIRTUAL"`},
	"child-context-virtual":              {result: `"virtual child step completed"`},
	"child-ops-invalid-depth":            {failed: true, errorType: "ChildContextError"},
	"child-ops-preservation":             {result: `{"branchFailed":true}`},

	"comprehensive-operations":      {result: `{"stepResult":"Step 1 completed successfully","waitCompleted":true,"mapResults":[2,4,6,8,10],"parallelResults":["apple","banana","orange"],"totalOperations":4}`},
	"concurrent-callback-submitter": {result: `{"results":["callback-1-done","callback-2-done"],"allCompleted":true}`},
	"concurrent-callback-wait": {
		nondeterministic: "elapsedMs is the measured wall-clock time between two concurrent waits",
		check: func(t testing.TB, result string) {
			assertPositiveDuration(t, resultObject(t, result), "elapsedMs")
		},
	},
	"concurrent-operations": {result: `["task 1 result","task 2 result"]`},
	"concurrent-wait":       {result: `"Completed waits"`},

	"context-validation-child":          {failed: true, errorType: "ChildContextError"},
	"context-validation-step":           {failed: true, errorType: "ChildContextError"},
	"context-validation-wait-condition": {failed: true, errorType: "ChildContextError"},

	"create-callback-concurrent":     {result: `{"results":["result-1","result-2","result-3"],"allCompleted":true}`},
	"create-callback-error-instance": {result: `{"timeoutError":{"isCallbackError":true,"isTimedOut":true,"errorMessage":"durable: callback \"timeout-test\" timed out: Callback.Timeout: Callback timed out"},"failureError":{"isCallbackError":true,"errorMessage":"durable: callback \"failure-test\" failed externally: CallbackError: test failure"}}`},
	"create-callback-failures":       {result: `{"success":false,"error":"durable: callback \"failing-operation\" failed: CallbackError: external system failed"}`},
	"create-callback-heartbeat":      {result: `{"longTaskResult":"long-task-done"}`},
	"create-callback-mixed-ops":      {result: `{"stepResult":{"userId":123,"name":"John Doe"},"callbackResult":"processed","completed":true}`},
	"create-callback-serdes":         {result: `{"receivedData":{"id":99,"message":"custom serialized data","timestamp":"2026-07-01T12:00:00Z"}}`},
	"create-callback-simple":         {result: `{"value":"hello from external"}`},
	"create-callback-timeout":        {result: `{"timedOut":true,"error":"durable: callback \"timeout-callback\" failed: Callback.Timeout: Callback timed out"}`},

	"error-determinism":       {result: `{"isDeterministic":true,"errorPropsBeforeReplay":{"isStepError":true,"causeName":"Error"},"errorPropsAfterReplay":{"isStepError":true,"causeName":"Error"}}`},
	"error-handling-taxonomy": {result: `{"stepErrorInfo":{"matched":true,"typeName":"StepError","operationName":"failing-step","attempts":1,"isOpError":true,"opErrorName":"failing-step","errorType":"Error"},"invokeErrorInfo":{"matched":true,"typeName":"InvokeError","operationName":"failing-invoke","isOpError":true,"opErrorName":"failing-invoke","errorType":"Error"},"callbackErrorInfo":{"matched":true,"typeName":"CallbackExternalError","operationName":"failing-callback","isOpError":true,"opErrorName":"failing-callback","errorType":"CallbackError"}}`},

	"force-checkpoint-callback": {result: `"{\"Items\":[{\"Index\":0,\"Name\":\"long-running\",\"Status\":1,\"Result\":\"long-complete\",\"Err\":null},{\"Index\":1,\"Name\":\"callbacks\",\"Status\":1,\"Result\":\"callbacks-complete\",\"Err\":null}],\"Reason\":1}"`},
	"force-checkpoint-invoke":   {result: `"{\"Items\":[{\"Index\":0,\"Name\":\"long-running\",\"Status\":1,\"Result\":\"long-complete\",\"Err\":null},{\"Index\":1,\"Name\":\"invokes\",\"Status\":1,\"Result\":\"invokes-complete\",\"Err\":null}],\"Reason\":1}"`},
	"force-checkpoint-step-retry": {
		nondeterministic: "the failed branch's error carries stack-trace frames whose file paths depend on the build machine",
		check:            checkForceCheckpointStepRetry,
	},
	"force-checkpoint-wait": {result: `"{\"Items\":[{\"Index\":0,\"Name\":\"long-running\",\"Status\":1,\"Result\":\"long-complete\",\"Err\":null},{\"Index\":1,\"Name\":\"waits\",\"Status\":1,\"Result\":\"waits-complete\",\"Err\":null}],\"Reason\":1}"`},

	"future-all":         {result: `["result 1","result 2","result 3"]`},
	"future-all-settled": {result: `{"outcomes":["fulfilled: success","rejected: durable: step \"failure\" failed after 0 attempts: Error: failure","fulfilled: another success"]}`},
	"future-all-wait":    {result: `"all waits completed"`},
	"future-any": {
		nondeterministic: "value is whichever of the two concurrent succeeding steps settles first",
		check: func(t testing.TB, result string) {
			obj := resultObject(t, result)
			assertFields(t, obj, map[string]any{"status": "succeeded"})
			if v := obj["value"]; v != "first success" && v != "second success" {
				t.Fatalf("expected value from a succeeding step, got %v", v)
			}
		},
	},
	"future-combinators-mixed": {result: `{"allResults":["Result from step 1","Result from step 2","Result from step 3"],"raceResult":"Fast result","settledCount":2,"anyResult":"First success!"}`},
	"future-join":              {result: `{"receipt":{"chargeId":"ch_123","amount":42.5},"reserved":3,"notified":true,"completed":true}`},
	"future-race":              {result: `"fast result"`},
	"future-race-wait": {
		nondeterministic: "elapsedMs is the measured wall-clock time until the first wait settles",
		check: func(t testing.TB, result string) {
			assertPositiveDuration(t, resultObject(t, result), "elapsedMs")
		},
	},
	"future-replay":          {result: `{"successStep":"Success"}`},
	"future-select":          {result: `{"winner":"fallback","quote":106.59,"note":"fallback quote with 2% surcharge"}`},
	"future-unhandled-error": {result: `{"successStep":"Success","scenariosTested":["basic-all-catch","immediate-combinator-usage","combinator-after-wait-replay","combinator-after-extended-wait"]}`},

	"handler-error":        {failed: true, errorType: "Error"},
	"hello-world":          {result: `"Hello World!"`},
	"insight-plugin":       {result: `{"message":"processed order ORD-12345","orderId":"ORD-12345"}`},
	"interrupted-no-retry": {result: `{"status":"failed","errorName":"StepError","message":"durable: step \"long-running-step\" failed after 1 attempts: StepInterruptedError: durable: step \"long-running-step\" interrupted before completing an attempt","causeName":"StepInterruptedError"}`},
	"invoke-simple":        {result: `{"status":"completed","input":{}}`},
	"invoke-tenant-id":     {result: `"wait finished"`},
	"large-payload":        {result: `{"success":true,"totalSize":307200,"chunkCount":6}`},

	"logger-after-callback": {result: `{"message":"done","callbackId":"self-resolved","result":"callback-resolved"}`},
	"logger-after-wait":     {result: `"done"`},
	"logger-log-levels":     {result: `"done"`},
	"logger-slog-handler":   {result: `"done"`},

	"map-basic": {result: `[2,4,6,8,10]`},
	"map-completion-config-issue": {
		nondeterministic: "startedCount and successfulItems depend on which of the three concurrent items settle before MinSuccessful is reached",
		check: func(t testing.TB, result string) {
			assertFields(t, resultObject(t, result), map[string]any{
				"totalItems": float64(4), "successfulCount": float64(2), "failedCount": float64(0),
				"hasFailures": false, "batchStatus": "SUCCEEDED", "completionReason": "MIN_SUCCESSFUL_REACHED",
			})
		},
	},
	"map-custom-summary-generator-replay": {
		nondeterministic: "startedCount depends on whether the slow item has started when MinSuccessful is reached",
		check: func(t testing.TB, result string) {
			assertFields(t, resultObject(t, result), map[string]any{
				"totalCount": float64(3), "successCount": float64(2), "completionReason": "MIN_SUCCESSFUL_REACHED",
				"itemIndexes": []any{float64(0), float64(1), float64(2)},
			})
		},
	},
	"map-empty":              {result: `{"results":[],"errors":[],"successCount":0,"failureCount":0,"totalCount":0,"status":"SUCCEEDED","completionReason":"ALL_COMPLETED"}`},
	"map-error-preservation": {result: `{"success":["Processed item 1","Processed item 3"],"errors":[{"message":"durable: child context \"\" failed: StepError: durable: step \"process-item-1\" failed after 1 attempts: Error: custom error for item 2","isStepError":true}],"totalErrors":1,"totalSuccess":2}`},
	"map-error-type-preservation": {
		result: `{"preserved":true,"beforeReplay":{"isChildCtxErr":true,"childCtxName":"","isStepErr":true,"stepName":"charge","stepAttempts":1,"leafTypeName":"PaymentError","leafMessage":"payment declined: INSUFFICIENT_FUNDS","isOperationError":true},"afterReplay":{"isChildCtxErr":true,"childCtxName":"","isStepErr":true,"stepName":"charge","stepAttempts":1,"leafTypeName":"PaymentError","leafMessage":"payment declined: INSUFFICIENT_FUNDS","isOperationError":true},"successCount":2,"failureCount":1}`,
	},
	"map-failure-threshold": {
		nondeterministic: "successCount depends on how many non-failing items settle before the third failure stops the batch",
		check: func(t testing.TB, result string) {
			assertFields(t, resultObject(t, result), map[string]any{
				"completionReason": "FAILURE_TOLERANCE_EXCEEDED", "failureCount": float64(3), "totalCount": float64(5),
			})
		},
	},
	"map-failure-threshold-percentage": {failed: true, errorType: "BatchError"},
	"map-flat-summarized-replay":       {result: `{"liveResultCount":8,"replayedResultCount":8,"replayedItems":[0,1,2,3,4,5,6,7]}`},
	"map-high-concurrency-invoke": {
		result: `{"results":["{\"status\":\"completed\",\"input\":{\"message\":\"payload-0\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-1\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-2\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-3\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-4\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-5\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-6\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-7\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-8\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-9\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-10\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-11\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-12\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-13\"}}","{\"status\":\"completed\",\"input\":{\"message\":\"payload-14\"}}"]}`,
	},
	"map-large-scale":                  {result: `{"success":true,"message":"Successfully processed 50 items with substantial data using map","summary":{"itemsProcessed":50,"totalDataSizeMB":4,"totalDataSizeBytes":5120000,"maxConcurrency":10,"averageItemSize":102400,"allItemsProcessed":true}}`},
	"map-min-successful":               {result: `{"successCount":2,"totalCount":5,"completionReason":"MIN_SUCCESSFUL_REACHED","results":["Item 1 processed","Item 2 processed"]}`},
	"map-tolerated-failure-count":      {result: `{"successCount":3,"failureCount":2,"totalCount":5,"completionReason":"ALL_COMPLETED","hasFailure":true}`},
	"map-tolerated-failure-percentage": {result: `{"successCount":6,"failureCount":3,"totalCount":9,"completionReason":"FAILURE_TOLERANCE_EXCEEDED","hasFailure":true,"results":["Item 0 processed","Item 1 processed","Item 3 processed","Item 4 processed","Item 6 processed","Item 7 processed"]}`},
	"map-virtual-context":              {result: `{"processedItems":[2,4,6,8,10],"totalCount":5,"successCount":5}`},

	"multiple-waits":      {result: `{"completedWaits":2,"finalStep":"done"}`},
	"named-step":          {result: `"processed: default"`},
	"no-replay-execution": {result: `{"completed":true}`},
	"non-durable":         {result: `{"statusCode":200,"body":"{\"message\":\"Hello from Lambda!\"}"}`},
	"order-fulfillment":   {result: `{"orderId":"ORD-12345","status":"FULFILLED","total":109.97,"paymentId":"pay-ORD-12345-10997","trackingNumber":"TRK-ORD-12345-001","labelUrl":"https://labels.example.com/ORD-12345"}`},

	"parallel-basic":                        {result: `["task 1 completed","task 2 completed","task 3 completed after wait"]`},
	"parallel-custom-summary-generator":     {result: `{"totalCount":3,"successCount":3,"resultLengths":[120000,120000,120000]}`},
	"parallel-empty":                        {result: `{"results":[],"errors":[],"successCount":0,"failureCount":0,"totalCount":0,"status":"SUCCEEDED","completionReason":"ALL_COMPLETED"}`},
	"parallel-error-preservation":           {result: `{"success":["task completed successfully"],"errors":[{"message":"durable: child context \"failing-task\" failed: StepError: durable: step \"failing-task\" failed after 1 attempts: Error: custom error message","isStepError":true}],"totalErrors":1}`},
	"parallel-failure-threshold-count":      {failed: true, errorType: "BatchError"},
	"parallel-failure-threshold-percentage": {failed: true, errorType: "BatchError"},
	"parallel-heterogeneous":                {result: `{"results":["computed: 7*6=42","waited: 1s elapsed"],"completionReason":"ALL_COMPLETED"}`},
	"parallel-invoke":                       {result: `{"successCount":3}`},
	"parallel-min-successful":               {result: `{"successCount":2,"totalCount":4,"completionReason":"MIN_SUCCESSFUL_REACHED","results":["Branch 1 result","Branch 2 result"]}`},
	"parallel-min-successful-callback":      {result: `{"successCount":3,"totalCount":5,"completionReason":"MIN_SUCCESSFUL_REACHED"}`},
	"parallel-should-complete": {
		nondeterministic: "startedCount depends on whether the slowest branch has started when the quorum is reached",
		check: func(t testing.TB, result string) {
			assertFields(t, resultObject(t, result), map[string]any{
				"successCount": float64(2), "totalCount": float64(3), "completionReason": "CUSTOM_COMPLETION_SUCCEEDED",
				"results": []any{"Branch B done", "Branch C done"},
			})
		},
	},
	"parallel-tolerated-failure":            {result: `{"successCount":3,"failureCount":2,"totalCount":5,"completionReason":"ALL_COMPLETED","hasFailure":true}`},
	"parallel-tolerated-failure-percentage": {result: `{"successCount":2,"failureCount":2,"totalCount":4,"completionReason":"FAILURE_TOLERANCE_EXCEEDED","hasFailure":true,"successResults":["result-1","result-3"]}`},
	"parallel-virtual-context":              {result: `{"results":["fetched","processed","validated"],"totalCount":3,"successCount":3}`},
	"parallel-wait":                         {result: `"Completed waits"`},
	"plugin-lifecycle":                      {result: `{"message":"plugin lifecycle complete","hooks":[{"hook":"OnInvocationStart"},{"hook":"OnOperationStart","operationName":"compute"},{"hook":"OnOperationAttemptStart","operationName":"compute","attempt":1},{"hook":"OnOperationAttemptEnd","operationName":"compute","attempt":1},{"hook":"OnOperationEnd","operationName":"compute"}]}`},

	"retry-callback":   {failed: true, errorType: "CallbackTimeoutError"},
	"retry-exhaustion": {failed: true, errorType: "StepError"},
	"retry-invoke":     {result: `{"response":{"message":"success on attempt 3","attempt":3},"attempts":3}`},

	"serde-basic":                   {result: `{"user":{"firstName":"","lastName":"","email":""},"greeting":"Hello, I'm  . My email is "}`},
	"serde-callback-deserializer":   {result: `{"first":"HELLO FIRST","second":"HELLO SECOND"}`},
	"serde-custom-config":           {result: `{"summary":"Order ORD-12345: $0.00 (processed)","id":"ORD-12345","amount":0,"status":"processed"}`},
	"serde-preview-field-selection": {result: `{"id":"cust-9","email":"decoy@example.com","customerEmail":"person@example.com","auditLength":2000}`},
	"serde-preview-truncation":      {result: `{"id":"acct-123","tier":"gold","notesLength":500}`},

	"simple-execution": {
		nondeterministic: "timestamp is the wall-clock time of the invocation",
		check: func(t testing.TB, result string) {
			obj := resultObject(t, result)
			assertFields(t, obj, map[string]any{"message": "Handler completed successfully"})
			if received, _ := obj["received"].(string); !strings.Contains(received, `"orderId": "ORD-12345"`) {
				t.Fatalf("expected received to echo the event, got %q", received)
			}
			assertPositiveDuration(t, obj, "timestamp")
		},
	},
	"simple-step":            {result: `"step completed"`},
	"step-error-determinism": {result: `{"deterministic":true,"before":{"hasError":true,"isStepErr":true,"attempts":1},"after":{"hasError":true,"isStepErr":true,"attempts":1}}`},
	"step-with-retry":        {result: `"step succeeded"`},
	"steps-with-retry":       {result: `{"id":"rec-001","name":""}`},
	"undefined-results":      {result: `"result"`},

	"wait-basic":                             {result: `"Function Completed"`},
	"wait-callback-anonymous":                {result: `{"value":"anonymous-result","completed":true}`},
	"wait-callback-basic":                    {result: `{"value":"hello from callback"}`},
	"wait-callback-child-context":            {result: `{"parentResult":{"parentData":"parent-value"},"childContextResult":{"childResult":{"childData":42},"childProcessed":true}}`},
	"wait-callback-error-instance-failure":   {result: `{"isCallbackError":true,"errorMessage":"durable: callback \"failure-test\" failed externally: CallbackExternalError: external failure"}`},
	"wait-callback-error-instance-submitter": {result: `{"isCallbackError":true,"isSubmitterError":true,"errorType":"Error","errorMessage":"submitter failed"}`},
	"wait-callback-error-instance-timeout":   {result: `{"isCallbackError":true,"isTimeoutError":true,"containsTimedOut":true,"heartbeat":false,"errorMessage":"durable: callback \"timeout-test\" timed out: CallbackTimeoutError: Callback timed out"}`},
	"wait-callback-failures":                 {result: `{"success":false,"error":"durable: callback \"failure-callback\" failed: CallbackError: external system failed","errorType":"CallbackError","mode":"external"}`},
	"wait-callback-heartbeat":                {result: `{"value":"heartbeat-success","completed":true}`},
	"wait-callback-mixed-ops":                {result: `{"stepResult":{"userId":123,"name":"John Doe"},"callbackResult":"callback-data","finalStep":{"status":"completed","timestamp":1234567890},"workflowCompleted":true}`},
	"wait-callback-multiple-invocations":     {result: `{"firstCallback":{"step":1},"secondCallback":{"step":2},"stepResult":{"processed":true,"step":1},"invocationCount":"multiple"}`},
	"wait-callback-nested":                   {result: `{"outerCallback":"outer-value","nestedResults":{"innerCallback":"inner-value","deepNested":{"innerCallback":"deep-value","deepLevel":"inner-child"},"level":"outer-child"}}`},
	"wait-callback-quick-completion":         {result: `{"value":"instant","success":true}`},
	"wait-callback-serdes":                   {result: `{"receivedData":{"id":42,"message":"serialized callback data","timestamp":"2026-01-01T00:00:00Z","metadata":{"version":"1.0","processed":true}},"isProcessed":true}`},
	"wait-callback-submitter-failure":        {result: `{"success":false,"error":"durable: callback \"failing-submitter-callback\" failed: Error: submitter failed: service unavailable"}`},
	"wait-callback-submitter-retry":          {result: `{"value":"retry-success","success":true}`},
	"wait-callback-timeout":                  {result: `{"timedOut":true,"error":"durable: callback \"timeout-callback\" failed: Callback.Timeout: Callback timed out"}`},
	"wait-configurable":                      {result: `"wait finished"`},
	"wait-for-condition":                     {result: `3`},
	"wait-named":                             {result: `"wait finished"`},
	"wait-unawaited":                         {result: `"result"`},
}
