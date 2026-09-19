// Command plugin_suspension_invocation_end implements conformance
// requirement 10-16: the invocation-end hook fires for every invocation,
// with a non-terminal status when the invocation suspends and a terminal
// status when the execution completes.
package main

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var out = pluginlog.New(nil)

// suspensionPlugin reports the invocation lifecycle. The end record
// carries the first-invocation flag of the same invocation, captured by
// the start hook: the end info does not expose it, and the start and end
// of one invocation run in the same process.
type suspensionPlugin struct {
	mu    sync.Mutex
	first bool
}

func (p *suspensionPlugin) plugin() durable.Plugin {
	return durable.Plugin{
		OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) {
			out.Capture(info.ExecutionArn)
			p.mu.Lock()
			p.first = info.IsFirstInvocation
			p.mu.Unlock()
			out.EmitWithArn(map[string]any{
				"plugin": "CONFPLUGIN",
				"hook":   "invocation-start",
				"first":  info.IsFirstInvocation,
			}, info.ExecutionArn)
		},
		OnInvocationEnd: func(_ context.Context, info durable.InvocationEndHookInfo) {
			p.mu.Lock()
			first := p.first
			p.mu.Unlock()
			out.EmitWithArn(map[string]any{
				"plugin":   "CONFPLUGIN",
				"hook":     "invocation-end",
				"first":    first,
				"terminal": pluginlog.Terminal(info.Status),
				"status":   string(info.Status),
			}, info.ExecutionArn)
		},
	}
}

func handler(ctx durable.Context, _ any) (string, error) {
	if err := durable.Wait(ctx, "", 2*time.Second); err != nil {
		return "", err
	}
	return "Wait completed", nil
}

func main() {
	durable.Start(handler, durable.WithPlugins((&suspensionPlugin{}).plugin()))
}
