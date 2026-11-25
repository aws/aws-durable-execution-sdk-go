// Command plugin-child-depth demonstrates
// [durable.WithPluginChildOperationsDepth], which bounds how deep in the
// operation tree the operations reported to plugins lie. Both
// WithPluginChildOperationsDepth and the plugin API are experimental and
// may change in a future release.
//
// The handler runs a step, a child context holding two steps, and a final
// step. The plugin records every operation it is told has ended. With a
// depth of 0 it is told only about the operations claimed on the
// handler's Context: the two outer steps and the child context. Every
// operation at that depth is reported with ChildrenOmitted set, meaning
// that anything nested inside it is withheld. For the child context that
// is its two steps, so a plugin that finds no children under it knows the
// subtree was not reported rather than empty. The withheld steps run and
// checkpoint exactly as reported ones do; only the notifications are
// omitted.
//
// The factory creates one recorder per invocation. The WrapInvocation
// hook attaches that recorder to the context it passes to the handler,
// and the handler reads it back from its [durable.Context]. So two
// invocations running at the same time in one process never share a
// recorder.
package main

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// reportedOp is one operation the plugin was told has ended.
type reportedOp struct {
	Name            string `json:"name"`
	ChildrenOmitted bool   `json:"childrenOmitted,omitempty"`
}

// recorder collects the reported operations of one invocation. It is
// safe for concurrent use because the plugin dispatch contract permits
// hooks to fire from several goroutines.
type recorder struct {
	mu  sync.Mutex
	ops []reportedOp
}

func (r *recorder) record(op reportedOp) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []reportedOp {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]reportedOp(nil), r.ops...)
}

// recorderKey is the context key under which WrapInvocation passes the
// invocation's recorder to the handler.
type recorderKey struct{}

// newPlugin is a durable.PluginFactory: the handler calls it once per
// invocation and the returned Plugin serves that invocation alone. Each
// call creates a new recorder, so the operations of one invocation never
// land in the events of another.
func newPlugin(context.Context, durable.InvocationHookInfo) (*durable.Plugin, error) {
	return pluginFor(&recorder{}), nil
}

// pluginFor returns the Plugin whose hooks record into rec.
// WrapInvocation runs the handler with rec attached to the context, so
// the handler can read the same recorder its plugin writes to.
func pluginFor(rec *recorder) *durable.Plugin {
	return &durable.Plugin{
		WrapInvocation: func(ctx context.Context, _ durable.InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
			return fn(context.WithValue(ctx, recorderKey{}, rec))
		},
		OnOperationEnd: func(_ context.Context, info durable.OperationHookInfo) {
			rec.record(reportedOp{Name: info.Name, ChildrenOmitted: info.ChildrenOmitted})
		},
	}
}

// options registers the plugin and reports operations down to depth 0:
// those claimed on the handler's Context. The steps inside the child
// context have depth 1 and are omitted.
var options = []durable.HandlerOption{
	durable.WithPlugins(durable.PluginFactoryFunc(newPlugin)),
	durable.WithPluginChildOperationsDepth(0),
}

// Output lists the operations the plugin was told have ended before the
// handler returned.
type Output struct {
	Reported []reportedOp `json:"reported"`
}

func step(ctx durable.Context, name string) error {
	_, err := durable.Step(ctx, name, func(_ durable.StepContext) (bool, error) {
		return true, nil
	})
	return err
}

func handler(ctx durable.Context, _ any) (Output, error) {
	rec, ok := ctx.Value(recorderKey{}).(*recorder)
	if !ok {
		return Output{}, errors.New("no recorder in context: register the plugin with durable.WithPlugins")
	}

	if err := step(ctx, "load-order"); err != nil {
		return Output{}, err
	}

	// The child context has depth 0; the two steps inside it have depth 1.
	if _, err := durable.RunInChildContext(ctx, "fulfil", func(cctx durable.Context) (bool, error) {
		if err := step(cctx, "reserve-stock"); err != nil {
			return false, err
		}
		return true, step(cctx, "charge-card")
	}); err != nil {
		return Output{}, err
	}

	if err := step(ctx, "notify"); err != nil {
		return Output{}, err
	}

	return Output{Reported: rec.snapshot()}, nil
}

func main() { durable.Start(handler, options...) }
