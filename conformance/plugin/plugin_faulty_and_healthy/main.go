// Command plugin_faulty_and_healthy implements conformance requirement
// 10-17: a plugin that panics in every hook never blocks another plugin's
// hooks or the execution.
package main

import (
	"context"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

const (
	faultyLabel  = "CONFPLUGIN-FAULTY"
	healthyLabel = "CONFPLUGIN-HEALTHY"
)

func isStep(info durable.OperationHookInfo) bool {
	return pluginlog.Upper(info.Type) == string(durable.OperationTypeStep)
}

// faulty is registered first. It logs from every hook this handler
// exercises and then panics. The SDK recovers the panic and still
// dispatches the hook to the other plugin.
// newFaulty is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone.
func newFaulty(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
			out.EmitWithArn(map[string]any{"plugin": faultyLabel, "hook": "invocation-start"}, info.ExecutionArn)
			panic("faulty OnInvocationStart")
		},
		OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
			if !isStep(info) {
				return
			}
			out.EmitWithArn(map[string]any{"plugin": faultyLabel, "hook": "operation-start"}, inv.ExecutionArn)
			panic("faulty OnOperationStart")
		},
		OnOperationAttemptStart: func(_ context.Context, info durable.AttemptHookInfo) {
			if !isStep(info.OperationHookInfo) {
				return
			}
			out.EmitWithArn(map[string]any{"plugin": faultyLabel, "hook": "attempt-start"}, inv.ExecutionArn)
			panic("faulty OnOperationAttemptStart")
		},
		OnOperationAttemptEnd: func(_ context.Context, info durable.AttemptEndHookInfo) {
			if !isStep(info.OperationHookInfo) {
				return
			}
			out.EmitWithArn(map[string]any{"plugin": faultyLabel, "hook": "attempt-end"}, inv.ExecutionArn)
			panic("faulty OnOperationAttemptEnd")
		},
		OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			if !isStep(info) {
				return
			}
			out.EmitWithArn(map[string]any{"plugin": faultyLabel, "hook": "operation-end"}, inv.ExecutionArn)
			panic("faulty OnOperationEnd")
		},
		OnInvocationEnd: func(_ context.Context, info durable.InvocationEndHookInfo) {
			out.EmitWithArn(map[string]any{"plugin": faultyLabel, "hook": "invocation-end"}, info.ExecutionArn)
			panic("faulty OnInvocationEnd")
		},
	}, nil
}

// healthy is registered second and reports every hook normally.
// newHealthy is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone.
func newHealthy(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
			out.EmitWithArn(map[string]any{
				"plugin": healthyLabel,
				"hook":   "invocation-start",
				"first":  info.IsFirstInvocation,
			}, info.ExecutionArn)
		},
		OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
			if !isStep(info) {
				return
			}
			out.EmitWithArn(map[string]any{"plugin": healthyLabel, "hook": "operation-start", "op": info.ID}, inv.ExecutionArn)
		},
		OnOperationAttemptStart: func(_ context.Context, info durable.AttemptHookInfo) {
			if !isStep(info.OperationHookInfo) {
				return
			}
			out.EmitWithArn(map[string]any{"plugin": healthyLabel, "hook": "attempt-start", "op": info.ID}, inv.ExecutionArn)
		},
		OnOperationAttemptEnd: func(_ context.Context, info durable.AttemptEndHookInfo) {
			if !isStep(info.OperationHookInfo) {
				return
			}
			out.EmitWithArn(map[string]any{
				"plugin":  healthyLabel,
				"hook":    "attempt-end",
				"op":      info.ID,
				"outcome": string(info.Outcome),
			}, inv.ExecutionArn)
		},
		OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			if !isStep(info) {
				return
			}
			out.EmitWithArn(map[string]any{
				"plugin": healthyLabel,
				"hook":   "operation-end",
				"op":     info.ID,
				"status": string(info.Status),
			}, inv.ExecutionArn)
		},
		OnInvocationEnd: func(_ context.Context, info durable.InvocationEndHookInfo) {
			out.EmitWithArn(map[string]any{
				"plugin": healthyLabel,
				"hook":   "invocation-end",
				"status": string(info.Status),
			}, info.ExecutionArn)
		},
	}, nil
}

func handler(ctx durable.Context, name string) (string, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "Hello, " + name + "!", nil
	})
}

func main() {
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newFaulty), durable.PluginFactoryFunc(newHealthy)))
}
