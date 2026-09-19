// Command plugin_multiple_plugins implements conformance requirement 10-5:
// two registered plugins both receive the invocation lifecycle hooks.
package main

import (
	"context"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// lifecyclePlugin reports the invocation lifecycle under the given plugin
// label. With several plugins registered, the hooks of different plugins
// for one notification may run in parallel, so the records of A and B may
// interleave in any order.
func lifecyclePlugin(label string) durable.Plugin {
	return durable.Plugin{
		OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
			out.Capture(info.ExecutionArn)
			out.EmitWithArn(map[string]any{"plugin": label, "hook": "invocation-start"}, info.ExecutionArn)
		},
		OnInvocationEnd: func(_ context.Context, info durable.InvocationEndHookInfo) {
			out.EmitWithArn(map[string]any{
				"plugin": label,
				"hook":   "invocation-end",
				"status": string(info.Status),
			}, info.ExecutionArn)
		},
	}
}

func handler(ctx durable.Context, name string) (string, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "Hello, " + name + "!", nil
	})
}

func main() {
	durable.Start(handler, durable.WithPlugins(lifecyclePlugin("CONFPLUGIN-A"), lifecyclePlugin("CONFPLUGIN-B")))
}
