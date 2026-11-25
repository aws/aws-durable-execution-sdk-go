package durable

import (
	"fmt"
	"math"
	"reflect"
	"strings"
)

// validateHandlerOptions rejects invalid options at construction and caches
// factory diagnostic names. Wrap panics on the returned error, before any
// invocation. Nil log handler, serdes, and execution client select defaults.
// Plugins exist only per invocation, so hookless results are detected and
// logged there; see newInvocationPlugins.
func validateHandlerOptions(opts *handlerOptions) error {
	for i, f := range opts.pluginFactories {
		if isNilPluginFactory(f) {
			return fmt.Errorf("durable: WithPlugins: factory at index %d is nil", i)
		}
	}
	if opts.pluginChildDepthSet && opts.pluginChildDepth < 0 {
		return fmt.Errorf("durable: WithPluginChildOperationsDepth: depth must not be negative, got %d", opts.pluginChildDepth)
	}
	return validatePluginRegistrations(opts)
}

// validatePluginRegistrations reads metadata only after every factory has
// passed the nil check. Each group records its first registration index,
// so repetition within one entry is valid but two entries cannot share it.
func validatePluginRegistrations(opts *handlerOptions) error {
	opts.pluginFactoryNames = make([]string, len(opts.pluginFactories))
	groups := make(map[string]int)
	for i, f := range opts.pluginFactories {
		registration, err := pluginRegistrationSafely(f, i)
		if err != nil {
			return err
		}
		name := registration.Name
		if name == "" {
			name = pluginFactoryName(f)
		}
		opts.pluginFactoryNames[i] = name
		for _, group := range registration.ExclusiveGroups {
			if strings.TrimSpace(group) == "" {
				return fmt.Errorf("durable: WithPlugins: factory at index %d (%q) declares a blank exclusive group %q", i, name, group)
			}
			if previous, ok := groups[group]; ok && previous != i {
				return fmt.Errorf("durable: WithPlugins: factory at index %d (%q) and factory at index %d (%q) share exclusive group %q",
					previous, opts.pluginFactoryNames[previous], i, name, group)
			}
			groups[group] = i
		}
	}
	return nil
}

// pluginRegistrationSafely contains a metadata panic at the configuration
// boundary. The error identifies the factory without calling its String
// method or formatting the panic value, both of which may run author code.
func pluginRegistrationSafely(f PluginFactory, index int) (registration PluginRegistration, err error) {
	if registered, ok := f.(RegisteredPluginFactory); ok {
		defer func() {
			if recover() != nil {
				err = fmt.Errorf("durable: WithPlugins: factory at index %d (%T): Registration() panicked", index, f)
			}
		}()
		return registered.Registration(), nil
	}
	return PluginRegistration{}, nil
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
