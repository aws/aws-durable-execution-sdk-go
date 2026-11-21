// Command plugin_parallel_branch_hooks implements conformance requirement
// 10-12: a plugin reports the start and end of each parallel branch's
// function with the branch's parent linkage.
//
// The Go SDK dispatches the lifecycle of a parallel branch through the
// operation hooks: OnOperationStart fires before the branch function runs
// and OnOperationEnd fires once the branch's terminal checkpoint is
// recorded, both from the goroutine that runs the branch. WrapChildContextFn
// does not fire for batch items. So the fn-start and fn-end records come
// from those two hooks, filtered to the ParallelBranch subtype.
package main

import (
	"context"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports the branch function lifecycle of parallel branches.
// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone.
func newPlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
			if info.SubType != durable.OperationSubTypeParallelBranch {
				return
			}
			out.EmitWithArn(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "fn-start",
				"op":     info.ID,
				"parent": pluginlog.OrNone(info.ParentID),
			}, inv.ExecutionArn)
		},
		OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			if info.SubType != durable.OperationSubTypeParallelBranch {
				return
			}
			out.EmitWithArn(map[string]any{
				"plugin":  "CONFPLUGIN",
				"hook":    "fn-end",
				"op":      info.ID,
				"parent":  pluginlog.OrNone(info.ParentID),
				"outcome": pluginlog.Outcome(info.Status),
			}, inv.ExecutionArn)
		},
	}, nil
}

func handler(ctx durable.Context, _ any) ([]string, error) {
	result, err := durable.Parallel(ctx, "parallel", []durable.Branch[string]{
		{Func: func(_ durable.Context) (string, error) { return "task-1", nil }},
		{Func: func(_ durable.Context) (string, error) { return "task-2", nil }},
	}, durable.WithMaxConcurrency(1))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newPlugin)))
}
