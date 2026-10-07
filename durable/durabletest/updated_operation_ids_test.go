// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// updatedNames records, per invocation, the names of the operations the
// invocation payload lists as updated.
type updatedNames struct {
	mu   sync.Mutex
	invs [][]string
}

func (u *updatedNames) plugin() durable.Plugin {
	return durable.Plugin{
		OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
			names := []string{}
			for _, op := range info.UpdatedOperations {
				names = append(names, op.Name)
			}
			sort.Strings(names)
			u.mu.Lock()
			u.invs = append(u.invs, names)
			u.mu.Unlock()
		},
	}
}

func (u *updatedNames) get() [][]string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([][]string(nil), u.invs...)
}

func equalNames(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

// TestLocalRunnerSetsUpdatedOperationIds checks that each invocation
// payload lists the operations whose state changed since the previous
// successful invocation, and only those.
func TestLocalRunnerSetsUpdatedOperationIds(t *testing.T) {
	t.Run("timer and callback completed while suspended", func(t *testing.T) {
		var u updatedNames
		handler := func(ctx durable.Context, _ string) (string, error) {
			if _, err := durable.Step(ctx, "fetch", func(durable.StepContext) (int, error) { return 1, nil }); err != nil {
				return "", err
			}
			if err := durable.Wait(ctx, "pause", 5*time.Second); err != nil {
				return "", err
			}
			cb, err := durable.CreateCallback[string](ctx, "approval")
			if err != nil {
				return "", err
			}
			return cb.Result(ctx)
		}
		runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(u.plugin()))
		result := driveToCompletion(t, runner, nil)
		if result.Status != durabletest.Succeeded {
			t.Fatalf("status = %s, want SUCCEEDED", result.Status)
		}
		// The first invocation has nothing updated. The second follows
		// the wait's completion. The third follows the callback's.
		want := [][]string{{}, {"pause"}, {"approval"}}
		if got := u.get(); !equalNames(got, want) {
			t.Fatalf("updated operations per invocation = %v, want %v", got, want)
		}
	})

	t.Run("completion reported during an invocation is not listed again", func(t *testing.T) {
		var u updatedNames
		handler := func(ctx durable.Context, _ string) (string, error) {
			// The short wait completes while the step runs, and a
			// checkpoint response reports it. The invocation then
			// suspends on the long wait.
			short := durable.WaitAsync(ctx, "short", time.Second)
			slow := durable.StepAsync(ctx, "slow", func(durable.StepContext) (int, error) {
				time.Sleep(50 * time.Millisecond)
				return 1, nil
			})
			if err := durable.Join(ctx, "join", []durable.Awaitable{short, slow}); err != nil {
				return "", err
			}
			if err := durable.Wait(ctx, "long", time.Hour); err != nil {
				return "", err
			}
			return "ok", nil
		}
		runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(u.plugin()))
		result := driveToCompletion(t, runner, nil)
		if result.Status != durabletest.Succeeded {
			t.Fatalf("status = %s, want SUCCEEDED", result.Status)
		}
		want := [][]string{{}, {"long"}}
		if got := u.get(); !equalNames(got, want) {
			t.Fatalf("updated operations per invocation = %v, want %v", got, want)
		}
	})
}
