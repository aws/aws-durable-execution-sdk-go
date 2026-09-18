// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestOrphanGoBranchDoesNotOutliveSuccessfulInvocation asserts that a
// durable.Go branch still running when the handler returns cannot
// checkpoint after the invocation has responded SUCCEEDED. The branch's next
// step must be refused and the branch must exit within a bounded time.
func TestOrphanGoBranchDoesNotOutliveSuccessfulInvocation(t *testing.T) {
	release := make(chan struct{})
	// outcome receives the orphan step's error once the step returns.
	outcome := make(chan error, 1)
	h := func(ctx durable.Context, _ any) (string, error) {
		durable.Go(ctx, "orphan", func(c durable.Context) (string, error) {
			<-release // still running when the handler returns
			_, err := durable.Step(c, "late", func(durable.StepContext) (string, error) {
				return "late", nil
			})
			outcome <- err
			return "orphan", err
		})
		return "handler-done", nil
	}
	r := durabletest.NewLocalRunner(h)
	res := r.Run(t, nil)
	if res.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", res.Status)
	}
	if res.RawResult != `"handler-done"` {
		t.Fatalf("result = %s, want %q", res.RawResult, `"handler-done"`)
	}

	close(release) // let the orphan proceed after the response was produced
	select {
	case err := <-outcome:
		if err == nil {
			t.Fatal("orphan durable.Go branch checkpointed a step AFTER the invocation responded SUCCEEDED")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("orphan branch did not exit after the invocation responded")
	}
	if op := res.Operation("late"); op != nil {
		t.Errorf("late step was recorded in the invocation's operations: %+v", op)
	}
}

// TestOrphanGoBranchDoesNotOutliveFailedInvocation asserts the same
// refusal when the handler returns an error or panics.
func TestOrphanGoBranchDoesNotOutliveFailedInvocation(t *testing.T) {
	finishes := map[string]func() (string, error){
		"error": func() (string, error) { return "", errors.New("handler failed") },
		"panic": func() (string, error) { panic("handler panicked") },
	}
	for name, finish := range finishes {
		t.Run(name, func(t *testing.T) {
			release := make(chan struct{})
			outcome := make(chan error, 1)
			h := func(ctx durable.Context, _ any) (string, error) {
				durable.Go(ctx, "orphan", func(c durable.Context) (string, error) {
					<-release
					_, err := durable.Step(c, "late", func(durable.StepContext) (string, error) {
						return "late", nil
					})
					outcome <- err
					return "orphan", err
				})
				return finish()
			}
			r := durabletest.NewLocalRunner(h)
			res := r.Run(t, nil)
			if res.Status != durabletest.Failed {
				t.Fatalf("status = %s, want FAILED", res.Status)
			}

			close(release)
			select {
			case err := <-outcome:
				if err == nil {
					t.Fatal("orphan durable.Go branch checkpointed a step AFTER the invocation responded FAILED")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("orphan branch did not exit after the invocation responded")
			}
		})
	}
}
