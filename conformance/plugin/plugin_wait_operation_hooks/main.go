// Command plugin_wait_operation_hooks implements conformance requirement
// 10-10: the operation-start and operation-end hooks fire for a wait
// operation.
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
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// plugin reports the lifecycle of wait operations. The wait starts live in
// the first invocation. It completes while the execution is suspended, so
// the invocation that resumes replays it from its terminal checkpoint and
// dispatches its end.
var plugin = durable.Plugin{
	OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
		out.Capture(info.ExecutionArn)
	},
	OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
		if pluginlog.Upper(info.Type) != string(durable.OperationTypeWait) {
			return
		}
		out.Emit(map[string]any{
			"plugin": "CONFPLUGIN",
			"hook":   "operation-start",
			"op":     info.ID,
			"type":   pluginlog.Upper(info.Type),
		})
	},
	OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
		if pluginlog.Upper(info.Type) != string(durable.OperationTypeWait) {
			return
		}
		out.Emit(map[string]any{
			"plugin": "CONFPLUGIN",
			"hook":   "operation-end",
			"op":     info.ID,
			"type":   pluginlog.Upper(info.Type),
			"status": string(info.Status),
		})
	},
}

func handler(ctx durable.Context, _ any) (string, error) {
	if err := durable.Wait(ctx, "", 2*time.Second); err != nil {
		return "", err
	}
	return "Wait completed", nil
}

func main() {
	durable.Start(handler, durable.WithPlugins(plugin))
}
