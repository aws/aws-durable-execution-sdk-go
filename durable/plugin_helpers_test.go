package durable

import "context"

// withTestPlugins registers fixed Plugin values for every invocation. The
// tests in this package own the state their hooks close over, so they do
// not need a fresh Plugin per invocation; the helper wraps each value in
// a PluginFactoryFunc that returns a copy of it. Tests of the factory
// contract itself use PluginFactoryFunc directly.
func withTestPlugins(plugins ...Plugin) HandlerOption {
	factories := make([]PluginFactory, len(plugins))
	for i := range plugins {
		p := plugins[i]
		factories[i] = PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) { return &p, nil })
	}
	return WithPlugins(factories...)
}
