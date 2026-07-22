package durable

import "fmt"

// validateHandlerOptions performs early rejection of invalid handler
// options at construction time. Called from [Wrap]; panics on invalid
// configuration to fail fast rather than deferring failures to the first
// invocation.
//
// nil logger, serdes, and execution client are valid (they select built-in
// defaults). The only invalid configuration currently is a Plugin with no
// hook functions set, which is always a programming mistake.
func validateHandlerOptions(opts *handlerOptions) error {
	for i, p := range opts.plugins {
		if !pluginHasHooks(p) {
			return fmt.Errorf("durable: WithPlugins: plugin at index %d has no hook functions set", i)
		}
	}
	return nil
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
