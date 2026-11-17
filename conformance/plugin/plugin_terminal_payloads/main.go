// Command plugin_terminal_payloads implements conformance requirement
// 10-14: the operation-end hook carries the checkpointed result of a
// succeeded step and the error message of a failed one.
package main

import (
	"context"
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports the terminal payload of step operations: the raw
// serialized result as checkpointed, and the recorded error message.
// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone.
func newPlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			if info.SubType != durable.OperationSubTypeStep {
				return
			}
			out.EmitWithArn(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "operation-end",
				"op":     info.ID,
				"status": string(info.Status),
				"result": pluginlog.OrNone(info.Result),
				"error":  pluginlog.ErrorMessage(info.Error),
			}, inv.ExecutionArn)
		},
	}, nil
}

func handler(ctx durable.Context, _ any) (string, error) {
	// Step A succeeds with the constant "task-a".
	if _, err := durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "task-a", nil
	}); err != nil {
		return "", err
	}
	// Step B always fails with "boom" and is not retried, so the execution
	// fails.
	if _, err := durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "", errors.New("boom")
	}, durable.WithRetry(durable.NoRetry())); err != nil {
		return "", err
	}
	return "unreachable", nil
}

func main() {
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newPlugin)))
}
