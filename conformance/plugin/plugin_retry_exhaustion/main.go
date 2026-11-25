// Command plugin_retry_exhaustion implements conformance requirement 10-15:
// the attempt hooks fire for every attempt until the retries are
// exhausted, then the operation-end hook reports FAILED.
package main

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports the attempt hooks and the terminal end of step
// operations. The end fires only once the step has an outcome: the first
// attempt's retryable failure suspends the invocation without an end.
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
		OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			out.EmitWithArn(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "operation-end",
				"op":     info.ID,
				"status": string(info.Status),
			}, inv.ExecutionArn)
		},
	}, nil
}

func handler(ctx durable.Context, _ any) (string, error) {
	// Two attempts in total, one second apart; both fail.
	if _, err := durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "", errors.New("always fails")
	}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  2,
		InitialDelay: time.Second,
		Jitter:       durable.JitterNone,
	}))); err != nil {
		return "", err
	}
	return "unreachable", nil
}

func main() {
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newPlugin)))
}
