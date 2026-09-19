package durable

import (
	"fmt"
	"math"
)

// validateHandlerOptions performs early rejection of invalid handler
// options at construction time. Called from [Wrap]; panics on invalid
// configuration to fail fast rather than deferring failures to the first
// invocation.
//
// nil log handler, serdes, and execution client are valid (they select built-in
// defaults). Invalid configurations are a Plugin with no hook functions set,
// which is always a programming mistake, and a negative plugin depth bound.
func validateHandlerOptions(opts *handlerOptions) error {
	for i, p := range opts.plugins {
		if !pluginHasHooks(p) {
			return fmt.Errorf("durable: WithPlugins: plugin at index %d has no hook functions set", i)
		}
	}
	if opts.pluginChildDepthSet && opts.pluginChildDepth < 0 {
		return fmt.Errorf("durable: WithPluginChildOperationsDepth: depth must not be negative, got %d", opts.pluginChildDepth)
	}
	return nil
}

// pluginDepthBound returns the hookDepthBound the root context takes from
// the options: the configured depth plus one, or 0 when no depth is set.
// A depth of math.MaxInt also gives 0. No operation tree is that deep, so
// the depth reports every operation, exactly as the default does; and
// adding one to it would overflow to a negative bound.
func (o *handlerOptions) pluginDepthBound() int {
	if !o.pluginChildDepthSet || o.pluginChildDepth == math.MaxInt {
		return 0
	}
	return o.pluginChildDepth + 1
}

// pluginHasHooks reports whether at least one hook function is set on p.
func pluginHasHooks(p Plugin) bool {
	return p.OnInvocationStart != nil ||
		p.OnInvocationEnd != nil ||
		p.OnOperationStart != nil ||
		p.OnOperationEnd != nil ||
		p.OnOperationAttemptStart != nil ||
		p.OnOperationAttemptEnd != nil ||
		p.OnOperationChange != nil ||
		p.WrapInvocation != nil ||
		p.WrapOperationAttemptFn != nil ||
		p.WrapChildContextFn != nil ||
		p.EnrichLogContext != nil
}
