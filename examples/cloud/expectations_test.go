// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package cloud

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// TestExpectationsCoverEveryExample fails when an example built by build.sh
// that is neither a companion nor local only has no expectation, when a
// companion or local-only example has one, when an expectation names an
// example that is not built, or when an entry is malformed. It runs without the cloud build tag
// so that adding an example without expected-result data fails the
// ordinary unit test run.
func TestExpectationsCoverEveryExample(t *testing.T) {
	examples, err := loadExamples("../build.sh")
	if err != nil {
		t.Fatalf("load example list: %v", err)
	}

	built := map[string]bool{}
	for _, name := range examples {
		built[name] = true
		if companions[name] {
			if _, ok := expectations[name]; ok {
				t.Errorf("%s: companion functions are never invoked and must not have an expectation", name)
			}
			continue
		}
		if localOnly[name] {
			if _, ok := expectations[name]; ok {
				t.Errorf("%s: local-only examples are never invoked and must not have an expectation", name)
			}
			continue
		}
		exp, ok := expectations[name]
		if !ok {
			t.Errorf("%s: no expectation declared in expectations.go", name)
			continue
		}
		if err := exp.validate(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}

	var stale []string
	for name := range expectations {
		if !built[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("expectations for examples that build.sh does not build: %s", strings.Join(stale, ", "))
	}
}

func TestExpectationValidate(t *testing.T) {
	check := func(testing.TB, string) {}
	cases := []struct {
		name    string
		exp     expectation
		wantErr string
	}{
		{"result", expectation{result: `1`}, ""},
		{"check with reason", expectation{check: check, nondeterministic: "varies"}, ""},
		{"failed", expectation{failed: true, errorType: "Error"}, ""},
		{"empty", expectation{}, "exactly one of result and check"},
		{"result and check", expectation{result: `1`, check: check, nondeterministic: "x"}, "exactly one of result and check"},
		{"check without reason", expectation{check: check}, "requires a nondeterministic reason"},
		{"invalid json", expectation{result: `{`}, "not valid JSON"},
		{"succeeding with errorType", expectation{result: `1`, errorType: "Error"}, "must not declare errorType"},
		{"failed without errorType", expectation{failed: true}, "must declare errorType"},
		{"failed with result", expectation{failed: true, errorType: "Error", result: `1`}, "must not declare a result"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.exp.validate()
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

// probeTB records the first fatal failure reported through it instead of
// failing the enclosing test. Fatalf panics with probeStop so that the
// assertion under test stops the way it would under a real *testing.T.
type probeTB struct {
	testing.TB
	failed bool
	msg    string
}

type probeStop struct{}

func (p *probeTB) Helper() {}

func (p *probeTB) Fatalf(format string, args ...any) {
	p.failed = true
	p.msg = fmt.Sprintf(format, args...)
	panic(probeStop{})
}

func (p *probeTB) Errorf(format string, args ...any) {
	p.failed = true
	p.msg = fmt.Sprintf(format, args...)
}

// probe runs fn against a probeTB and reports whether fn failed.
func probe(fn func(testing.TB)) (failed bool, msg string) {
	p := &probeTB{}
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(probeStop); !ok {
				panic(r)
			}
		}
		failed, msg = p.failed, p.msg
	}()
	fn(p)
	return p.failed, p.msg
}

func TestExpectationAssert(t *testing.T) {
	run := func(exp expectation, got outcome) bool {
		failed, _ := probe(func(tb testing.TB) { exp.assert(tb, got) })
		return !failed
	}

	if !run(expectation{result: `{"a":1,"b":[true]}`}, outcome{result: `{"b":[true],"a":1}`}) {
		t.Error("equal results in a different key order must pass")
	}
	if run(expectation{result: `{"a":1}`}, outcome{result: `{"a":2}`}) {
		t.Error("a differing result must fail")
	}
	if run(expectation{result: `1`}, outcome{failed: true, errorType: "Error"}) {
		t.Error("a failed execution must not satisfy a succeeding expectation")
	}
	if !run(expectation{failed: true, errorType: "StepError"}, outcome{failed: true, errorType: "StepError"}) {
		t.Error("matching error type must pass")
	}
	if run(expectation{failed: true, errorType: "StepError"}, outcome{failed: true, errorType: "BatchError"}) {
		t.Error("a differing error type must fail")
	}
	if run(expectation{failed: true, errorType: "StepError"}, outcome{result: `1`}) {
		t.Error("a succeeded execution must not satisfy a failing expectation")
	}

	called := false
	exp := expectation{nondeterministic: "probe", check: func(tb testing.TB, result string) {
		called = true
		if result != `{"elapsedMs":5}` {
			tb.Fatalf("unexpected result %s", result)
		}
	}}
	if !run(exp, outcome{result: `{"elapsedMs":5}`}) || !called {
		t.Error("check must run against the raw result")
	}
}

// TestExpectationChecksAcceptObservedResults runs every predicate-based
// expectation against a result of the shape the example produces, so a
// predicate that rejects the real result is caught before a cloud run.
func TestExpectationChecksAcceptObservedResults(t *testing.T) {
	samples := map[string]string{
		"concurrent-callback-wait":            `{"elapsedMs":1263}`,
		"future-any":                          `{"status":"succeeded","value":"first success"}`,
		"future-race-wait":                    `{"elapsedMs":1182}`,
		"map-completion-config-issue":         `{"totalItems":4,"successfulCount":2,"failedCount":0,"startedCount":2,"hasFailures":false,"batchStatus":"SUCCEEDED","completionReason":"MIN_SUCCESSFUL_REACHED","successfulItems":[{"index":0,"itemId":1},{"index":2,"itemId":3}],"failedItems":null}`,
		"map-custom-summary-generator-replay": `{"totalCount":3,"successCount":2,"startedCount":1,"completionReason":"MIN_SUCCESSFUL_REACHED","itemIndexes":[0,1,2]}`,
		"map-failure-threshold":               `{"completionReason":"FAILURE_TOLERANCE_EXCEEDED","successCount":0,"failureCount":3,"totalCount":5}`,
		"parallel-heterogeneous":              `{"compute":{"expression":"7*6","value":42},"waitedSeconds":1,"invokeError":"durable: child context \"invoke\" failed: InvokeError: invoke failed: durable: invoke \"child-function\" of \"target-handler\" failed: ResourceNotFoundException: Function not found","completionReason":"ALL_COMPLETED"}`,
		"map-min-successful":                  `{"successCount":2,"totalCount":5,"completionReason":"MIN_SUCCESSFUL_REACHED","results":["Item 4 processed","Item 5 processed"]}`,
		"parallel-min-successful":             `{"successCount":2,"totalCount":4,"completionReason":"MIN_SUCCESSFUL_REACHED","results":["Branch 2 result","Branch 4 result"]}`,
		"parallel-should-complete":            `{"successCount":1,"startedCount":2,"totalCount":3,"completionReason":"CUSTOM_COMPLETION_SUCCEEDED","results":["Branch A done"]}`,
		"simple-execution":                    `{"received":"{\n  \"orderId\": \"ORD-12345\"\n}","timestamp":1789772875799,"message":"Handler completed successfully"}`,
	}
	for name, exp := range expectations {
		if exp.check == nil {
			continue
		}
		sample, ok := samples[name]
		if !ok {
			t.Errorf("%s: predicate-based expectation has no sample result in this test", name)
			continue
		}
		if failed, msg := probe(func(tb testing.TB) { exp.assert(tb, outcome{result: sample}) }); failed {
			t.Errorf("%s: predicate rejects the observed result shape: %s", name, msg)
		}
	}
}
