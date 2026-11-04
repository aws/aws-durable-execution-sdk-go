// Command plugin_invocation_lifecycle implements conformance requirement
// 10-1: a plugin reports the invocation-start and invocation-end hooks of
// a single-invocation execution.
package main

import (
	"context"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports the invocation lifecycle. The start hook fires before the
// handler runs and captures the execution ARN; the end hook fires after
// the outcome is recorded and carries the terminal status.
// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone.
func newPlugin(durable.InvocationHookInfo) (*durable.Plugin, error) {
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

func handler(ctx durable.Context, name string) (string, error) {
	return durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		sc.Logger().Info("Greeting step running for: " + name)
		return "Hello, " + name + "!", nil
	})
}

func main() {
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newPlugin)))
}
