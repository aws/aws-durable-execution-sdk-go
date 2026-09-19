// Command plugin_faulty_and_healthy implements conformance requirement
// 10-17: a plugin that panics in every hook never blocks another plugin's
// hooks or the execution.
//
// The op field of each record is OperationHookInfo.ID. The Go SDK fills it
// with the operation's positional id (for example 1 or 1-1), while the
// execution history and the ParentID field carry the checkpointed
// operation id. The runner compares op against the history id, so the
// requirement is registered as not implemented in template_plugin.yaml
// until the two ids agree.
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
var faulty = durable.Plugin{
	OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
		out.Capture(info.ExecutionArn)
		out.EmitWithArn(map[string]any{"plugin": faultyLabel, "hook": "invocation-start"}, info.ExecutionArn)
		panic("faulty OnInvocationStart")
	},
	OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
		if !isStep(info) {
			return
		}
		out.Emit(map[string]any{"plugin": faultyLabel, "hook": "operation-start"})
		panic("faulty OnOperationStart")
	},
	OnOperationAttemptStart: func(_ context.Context, info durable.AttemptHookInfo) {
		if !isStep(info.OperationHookInfo) {
			return
		}
		out.Emit(map[string]any{"plugin": faultyLabel, "hook": "attempt-start"})
		panic("faulty OnOperationAttemptStart")
	},
	OnOperationAttemptEnd: func(_ context.Context, info durable.AttemptEndHookInfo) {
		if !isStep(info.OperationHookInfo) {
			return
		}
		out.Emit(map[string]any{"plugin": faultyLabel, "hook": "attempt-end"})
		panic("faulty OnOperationAttemptEnd")
	},
	OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
		if !isStep(info) {
			return
		}
		out.Emit(map[string]any{"plugin": faultyLabel, "hook": "operation-end"})
		panic("faulty OnOperationEnd")
	},
	OnInvocationEnd: func(_ context.Context, info durable.InvocationEndHookInfo) {
		out.EmitWithArn(map[string]any{"plugin": faultyLabel, "hook": "invocation-end"}, info.ExecutionArn)
		panic("faulty OnInvocationEnd")
	},
}

// healthy is registered second and reports every hook normally.
var healthy = durable.Plugin{
	OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
		out.Capture(info.ExecutionArn)
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
		out.Emit(map[string]any{"plugin": healthyLabel, "hook": "operation-start", "op": info.ID})
	},
	OnOperationAttemptStart: func(_ context.Context, info durable.AttemptHookInfo) {
		if !isStep(info.OperationHookInfo) {
			return
		}
		out.Emit(map[string]any{"plugin": healthyLabel, "hook": "attempt-start", "op": info.ID})
	},
	OnOperationAttemptEnd: func(_ context.Context, info durable.AttemptEndHookInfo) {
		if !isStep(info.OperationHookInfo) {
			return
		}
		out.Emit(map[string]any{
			"plugin":  healthyLabel,
			"hook":    "attempt-end",
			"op":      info.ID,
			"outcome": string(info.Outcome),
		})
	},
	OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
		if !isStep(info) {
			return
		}
		out.Emit(map[string]any{
			"plugin": healthyLabel,
			"hook":   "operation-end",
			"op":     info.ID,
			"status": string(info.Status),
		})
	},
	OnInvocationEnd: func(_ context.Context, info durable.InvocationEndHookInfo) {
		out.EmitWithArn(map[string]any{
			"plugin": healthyLabel,
			"hook":   "invocation-end",
			"status": string(info.Status),
		}, info.ExecutionArn)
	},
}

func handler(ctx durable.Context, name string) (string, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "Hello, " + name + "!", nil
	})
}

func main() {
	durable.Start(handler, durable.WithPlugins(faulty, healthy))
}
