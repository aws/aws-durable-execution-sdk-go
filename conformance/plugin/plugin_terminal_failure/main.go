// Command plugin_terminal_failure implements conformance requirement 10-7:
// the invocation-end hook carries the FAILED status when the execution
// fails.
package main

import (
	"context"
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports the invocation lifecycle.
// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone.
func newPlugin(context.Context, durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
			out.Capture(info.ExecutionArn)
			out.EmitWithArn(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "invocation-start",
				"first":  info.IsFirstInvocation,
			}, info.ExecutionArn)
		},
		OnInvocationEnd: func(_ context.Context, info durable.InvocationEndHookInfo) {
			out.EmitWithArn(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "invocation-end",
				"status": string(info.Status),
			}, info.ExecutionArn)
		},
	}, nil
}

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "", errors.New("Something went wrong")
	}, durable.WithRetry(durable.NoRetry()))
}

func main() {
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newPlugin)))
}
