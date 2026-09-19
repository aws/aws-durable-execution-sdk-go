// Command plugin_wait_replay_flag implements conformance requirement 10-18:
// a wait still pending during a replay re-fires operation-start with
// replay=true while it is non-terminal, and a wait that is already
// terminal never re-fires operation-start.
package main

import (
	"context"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

func isWait(info durable.OperationHookInfo) bool {
	return pluginlog.Upper(info.Type) == string(durable.OperationTypeWait)
}

// plugin reports the lifecycle of wait operations, correlated by the
// stable wait name: branch operation ids are not deterministic under
// concurrency. A replayed start reports the wait's checkpointed status, so
// pending is read from that status alone. A wait replayed from a terminal
// checkpoint dispatches only its end.
var plugin = durable.Plugin{
	OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
		out.Capture(info.ExecutionArn)
	},
	OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
		if !isWait(info) {
			return
		}
		out.Emit(map[string]any{
			"plugin":  "CONFPLUGIN",
			"hook":    "operation-start",
			"type":    pluginlog.Upper(info.Type),
			"name":    info.Name,
			"replay":  info.IsReplay,
			"pending": !pluginlog.OperationTerminal(info.Status),
		})
	},
	OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
		if !isWait(info) {
			return
		}
		out.Emit(map[string]any{
			"plugin": "CONFPLUGIN",
			"hook":   "operation-end",
			"type":   pluginlog.Upper(info.Type),
			"name":   info.Name,
			"status": string(info.Status),
		})
	},
}

func handler(ctx durable.Context, _ any) ([]string, error) {
	// Both waits run concurrently, so both are pending at once. The short
	// wait completes first; the long wait stays pending across the first
	// replay.
	result, err := durable.Parallel(ctx, "waits", []durable.Branch[string]{
		{Func: func(c durable.Context) (string, error) {
			if err := durable.Wait(c, "short", 2*time.Second); err != nil {
				return "", err
			}
			return "short-done", nil
		}},
		{Func: func(c durable.Context) (string, error) {
			if err := durable.Wait(c, "long", 8*time.Second); err != nil {
				return "", err
			}
			return "long-done", nil
		}},
	}, durable.WithMaxConcurrency(2))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler, durable.WithPlugins(plugin))
}
