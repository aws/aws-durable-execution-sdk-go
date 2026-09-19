// Command plugin_error_isolation implements conformance requirement 10-4:
// a plugin whose every hook panics after logging never affects the
// execution outcome.
package main

import (
	"context"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// faulty logs one record from each hook and then panics. The SDK recovers
// a panicking notification hook and discards the panic, so the execution
// result and history match a run without the plugin. The operation and
// attempt hooks report step operations only.
var faulty = durable.Plugin{
	OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
		out.Capture(info.ExecutionArn)
		out.EmitWithArn(map[string]any{"plugin": "CONFPLUGIN-FAULTY", "hook": "invocation-start"}, info.ExecutionArn)
		panic("faulty invocation-start")
	},
	OnInvocationEnd: func(_ context.Context, info durable.InvocationEndHookInfo) {
		out.EmitWithArn(map[string]any{"plugin": "CONFPLUGIN-FAULTY", "hook": "invocation-end"}, info.ExecutionArn)
		panic("faulty invocation-end")
	},
	OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
		if info.SubType != durable.OperationSubTypeStep {
			return
		}
		out.Emit(map[string]any{"plugin": "CONFPLUGIN-FAULTY", "hook": "operation-start"})
		panic("faulty operation-start")
	},
	OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
		if info.SubType != durable.OperationSubTypeStep {
			return
		}
		out.Emit(map[string]any{"plugin": "CONFPLUGIN-FAULTY", "hook": "operation-end"})
		panic("faulty operation-end")
	},
	OnOperationAttemptStart: func(_ context.Context, info durable.AttemptHookInfo) {
		if info.SubType != durable.OperationSubTypeStep {
			return
		}
		out.Emit(map[string]any{"plugin": "CONFPLUGIN-FAULTY", "hook": "attempt-start"})
		panic("faulty attempt-start")
	},
	OnOperationAttemptEnd: func(_ context.Context, info durable.AttemptEndHookInfo) {
		if info.SubType != durable.OperationSubTypeStep {
			return
		}
		out.Emit(map[string]any{"plugin": "CONFPLUGIN-FAULTY", "hook": "attempt-end"})
		panic("faulty attempt-end")
	},
}

func handler(ctx durable.Context, name string) (string, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "Hello, " + name + "!", nil
	})
}

func main() {
	durable.Start(handler, durable.WithPlugins(faulty))
}
