package durable

import (
	"fmt"
	"math"
	"reflect"
)

// validateHandlerOptions performs early rejection of invalid handler
// options at construction time. Called from [Wrap]; panics on invalid
// configuration to fail fast rather than deferring failures to the first
// invocation.
//
// nil log handler, serdes, and execution client are valid (they select built-in
// defaults). Invalid configurations are a nil plugin factory, which is
// always a programming mistake, and a negative plugin depth bound. The
// plugins themselves exist only per invocation, so a Plugin with no hook
// functions set is detected there and logged; see newInvocationPlugins.
func validateHandlerOptions(opts *handlerOptions) error {
	for i, f := range opts.pluginFactories {
		if isNilPluginFactory(f) {
			return fmt.Errorf("durable: WithPlugins: factory at index %d is nil", i)
		}
	}
	if opts.pluginChildDepthSet && opts.pluginChildDepth < 0 {
		return fmt.Errorf("durable: WithPluginChildOperationsDepth: depth must not be negative, got %d", opts.pluginChildDepth)
	}
	return nil
}

// isNilPluginFactory reports whether f is nil or holds a nil value. The
// comparison f == nil is true only for a nil interface. PluginFactoryFunc(nil)
// and a typed nil pointer are non-nil interfaces, so they pass it. Both are
// the same programming mistake and neither can produce a working plugin.
// IsNil panics on kinds that cannot hold nil, so the kind switch guards it.
func isNilPluginFactory(f PluginFactory) bool {
	if f == nil {
		return true
	}
	v := reflect.ValueOf(f)
	switch v.Kind() {
	case reflect.Func, reflect.Pointer, reflect.Map, reflect.Chan, reflect.Slice, reflect.Interface, reflect.UnsafePointer:
		return v.IsNil()
	default:
		return false
	}
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
// A factory that returns a Plugin without any is logged and skipped for
// the invocation.
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
