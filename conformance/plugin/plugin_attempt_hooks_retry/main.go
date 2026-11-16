// Command plugin_attempt_hooks_retry implements conformance requirement
// 10-3: a plugin reports each attempt of a retried step with its attempt
// number and outcome.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports the attempt hooks of step operations. Attempts are
// numbered from 1 and the numbering continues across invocations. The
// attempt hooks run on the goroutine that runs the step body, so their
// order is deterministic.
// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone.
func newPlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnOperationAttemptStart: func(_ context.Context, info durable.AttemptHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			out.EmitWithArn(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "attempt-start",
				"n":      info.Attempt,
				"op":     info.ID,
			}, inv.ExecutionArn)
		},
		OnOperationAttemptEnd: func(_ context.Context, info durable.AttemptEndHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			out.EmitWithArn(map[string]any{
				"plugin":  "CONFPLUGIN",
				"hook":    "attempt-end",
				"n":       info.Attempt,
				"outcome": string(info.Outcome),
				"op":      info.ID,
			}, inv.ExecutionArn)
		},
	}, nil
}

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		// The step context's attempt counter is 1 on the first attempt.
		// The step fails once and succeeds on the second attempt.
		if sc.Attempt() < 2 {
			return "", fmt.Errorf("Attempt %d failed", sc.Attempt())
		}
		return "Operation succeeded", nil
	}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: time.Second,
		Jitter:       durable.JitterNone,
	})))
}

func main() {
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newPlugin)))
}
