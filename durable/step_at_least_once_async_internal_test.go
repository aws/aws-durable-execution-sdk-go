// SPDX-License-Identifier: Apache-2.0

package durable

import "testing"

// A step under AtLeastOnceAsync whose previous attempt was interrupted
// after its START runs again on replay, as under AtLeastOnce.
func TestAtLeastOnceAsyncStartedStepRunsAgain(t *testing.T) {
	executed := false
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s", func(StepContext) (string, error) {
			executed = true
			return "ok", nil
		}, WithSemantics(AtLeastOnceAsync))
	}, withLambdaAPI(&fakePluginClient{}))

	ops := []wireOperation{
		lifecycleExecOp(),
		{
			Id: hashID("1"), Status: "STARTED", Type: "STEP", SubType: "Step", Name: "s",
			StepDetails: &wireStepDetails{Attempt: 0},
		},
	}
	resp, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:async", "tok1", ops))
	if err != nil {
		t.Fatal(err)
	}
	if !executed {
		t.Fatal("interrupted step did not run again")
	}
	assertPluginResponseStatus(t, resp, invocationSucceeded)
}
