// Command plugin_attempt_info_shape implements conformance requirement
// 10-21: the attempt-start and attempt-end hooks of a step carry the full
// attempt field set.
//
// The plugin dumps the info parameter of each attempt hook through
// [pluginlog.InfoRecord]: every field of OperationHookInfo under its
// canonical camelCase name, with an unset field omitted. The requirement
// asserts the presence of each probed key, so an omitted field is the
// signal that the Go info type lacks it or left it unset.
//
// AttemptEndHookInfo declares its own Error field. That field hides the
// Error of the embedded OperationHookInfo, which the SDK leaves nil on
// attempt hooks. So the attempt-end record takes error and outcome from
// the AttemptEndHookInfo fields.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone. The
// plugin dumps the attempt infos of step operations.
func newPlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnOperationAttemptStart: func(_ context.Context, info durable.AttemptHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			out.EmitWithArn(pluginlog.InfoRecord("attempt-start", info.OperationHookInfo), inv.ExecutionArn)
		},
		OnOperationAttemptEnd: func(_ context.Context, info durable.AttemptEndHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			rec := pluginlog.InfoRecord("attempt-end", info.OperationHookInfo)
			rec["outcome"] = string(info.Outcome)
			if info.Error != nil {
				rec["error"] = pluginlog.ErrorMessage(info.Error)
			}
			out.EmitWithArn(rec, inv.ExecutionArn)
		},
	}, nil
}

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "flaky", func(sc durable.StepContext) (string, error) {
		// The step context's attempt counter is 1 on the first attempt.
		// The step fails once and succeeds on the second attempt.
		if sc.Attempt() < 2 {
			return "", fmt.Errorf("Attempt %d failed", sc.Attempt())
		}
		return "ok", nil
	}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: time.Second,
		Jitter:       durable.JitterNone,
	})))
}

func main() {
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newPlugin)))
}
