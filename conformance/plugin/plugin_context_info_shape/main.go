// Command plugin_context_info_shape implements conformance requirement
// 10-23: context operations expose their subType token in operation-start,
// and the hook that fires when a branch function starts exposes the
// children-replay indicator.
//
// The plugin dumps the info parameter of each hook through
// [pluginlog.InfoRecord], filtered to CONTEXT-type operations. The parent
// parallel reports SubType Parallel; each named branch reports SubType
// ParallelBranch.
//
// The requirement probes a per-branch "fn-start" hook carrying an
// isReplayingChildren field. The Go SDK has no separate hook for the start
// of a branch function: WrapChildContextFn excludes Map and Parallel items,
// and OnOperationStart is the hook that fires immediately before a branch
// body runs. So the plugin emits the fn-start record from OnOperationStart
// of every ParallelBranch item. The indicator maps to OperationHookInfo
// IsReplay. A branch item that is live dispatches its start with IsReplay
// false. A branch item that is re-entered from a STARTED checkpoint, because
// a child suspended in an earlier invocation, dispatches its start with
// IsReplay true and runs its body again so its checkpointed children
// replay. So IsReplay on a branch start is the children-replay indicator
// the requirement describes.
package main

import (
	"context"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone. The
// plugin dumps the start info of every context operation and, for a
// branch item, also emits the fn-start record described in the package
// comment.
func newPlugin(_ context.Context, inv durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
			if pluginlog.Upper(info.Type) != string(durable.OperationTypeContext) {
				return
			}
			out.EmitWithArn(pluginlog.InfoRecord("operation-start", info), inv.ExecutionArn)
			if info.SubType != durable.OperationSubTypeParallelBranch {
				return
			}
			fnStart := pluginlog.InfoRecord("fn-start", info)
			fnStart["isReplayingChildren"] = info.IsReplay
			out.EmitWithArn(fnStart, inv.ExecutionArn)
		},
	}, nil
}

func handler(ctx durable.Context, _ any) ([]string, error) {
	result, err := durable.Parallel(ctx, "ctx", []durable.Branch[string]{
		{Name: "branch-a", Func: func(bctx durable.Context) (string, error) {
			if _, err := durable.Step(bctx, "inner", func(_ durable.StepContext) (string, error) {
				return "x", nil
			}); err != nil {
				return "", err
			}
			if err := durable.Wait(bctx, "", 2*time.Second); err != nil {
				return "", err
			}
			return "a-done", nil
		}},
		{Name: "branch-b", Func: func(_ durable.Context) (string, error) {
			return "b-done", nil
		}},
	}, durable.WithMaxConcurrency(1))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler, durable.WithPlugins(durable.PluginFactoryFunc(newPlugin)))
}
