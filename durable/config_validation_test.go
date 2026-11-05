package durable

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestWrapPanicsOnNilHandler(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Wrap(nil) should panic")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if !strings.Contains(msg, "nil") {
			t.Errorf("panic message should mention nil: %q", msg)
		}
	}()
	Wrap[any, any](nil)
}

func TestWrapPanicsOnNilPluginFactory(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Wrap with nil PluginFactory should panic")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if !strings.Contains(msg, "factory at index 0") {
			t.Errorf("panic message should identify the offending factory: %q", msg)
		}
	}()
	handler := func(_ Context, _ string) (string, error) { return "", nil }
	Wrap(handler, WithPlugins(nil))
}

func TestWrapPanicsOnSecondNilPluginFactory(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Wrap with second nil PluginFactory should panic")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if !strings.Contains(msg, "factory at index 1") {
			t.Errorf("panic message should identify index 1: %q", msg)
		}
	}()
	handler := func(_ Context, _ string) (string, error) { return "", nil }
	valid := PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) {
		return &Plugin{OnInvocationStart: func(context.Context, InvocationHookInfo) {}}, nil
	})
	Wrap(handler, WithPlugins(valid, nil))
}

// ptrFactory is a pointer-receiver PluginFactory whose CreatePlugin reads
// no fields, like telemetry.TelemetryPlugin. A typed nil *ptrFactory is a
// non-nil PluginFactory interface, so f == nil does not catch it.
type ptrFactory struct{}

func (*ptrFactory) CreatePlugin(InvocationHookInfo) (*Plugin, error) {
	return &Plugin{OnInvocationStart: func(context.Context, InvocationHookInfo) {}}, nil
}

// valueFactory is a value-type PluginFactory. A struct kind cannot hold
// nil, so it exercises the kind guard around reflect.Value.IsNil.
type valueFactory struct{}

func (valueFactory) CreatePlugin(InvocationHookInfo) (*Plugin, error) {
	return &Plugin{OnInvocationStart: func(context.Context, InvocationHookInfo) {}}, nil
}

func TestWrapPanicsOnTypedNilPluginFactory(t *testing.T) {
	cases := []struct {
		name    string
		factory PluginFactory
	}{
		{name: "nil PluginFactoryFunc", factory: PluginFactoryFunc(nil)},
		{name: "typed nil pointer", factory: (*ptrFactory)(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("Wrap with typed nil PluginFactory should panic")
				}
				msg, ok := r.(string)
				if !ok {
					t.Fatalf("expected string panic, got %T: %v", r, r)
				}
				if !strings.Contains(msg, "factory at index 0") {
					t.Errorf("panic message should identify the offending factory: %q", msg)
				}
			}()
			handler := func(_ Context, _ string) (string, error) { return "", nil }
			Wrap(handler, WithPlugins(tc.factory))
		})
	}
}

func TestWrapAcceptsNonNilFactoryKinds(t *testing.T) {
	cases := []struct {
		name    string
		factory PluginFactory
	}{
		{name: "PluginFactoryFunc", factory: PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) {
			return &Plugin{OnInvocationStart: func(context.Context, InvocationHookInfo) {}}, nil
		})},
		{name: "non-nil pointer", factory: &ptrFactory{}},
		{name: "value type", factory: valueFactory{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := func(_ Context, _ string) (string, error) { return "", nil }
			h := Wrap(handler, WithPlugins(tc.factory))
			if h == nil {
				t.Fatal("Wrap returned nil")
			}
		})
	}
}

func TestWrapAcceptsValidConfig(t *testing.T) {
	// Should not panic with valid configuration.
	handler := func(_ Context, _ string) (string, error) { return "", nil }
	h := Wrap(handler)
	if h == nil {
		t.Fatal("Wrap returned nil")
	}
}

func TestWrapAcceptsValidConfigWithAllOptions(t *testing.T) {
	handler := func(_ Context, _ string) (string, error) { return "", nil }
	plugin := Plugin{OnInvocationStart: func(context.Context, InvocationHookInfo) {}}
	h := Wrap(handler,
		WithLogHandler(slog.DiscardHandler),
		withTestPlugins(plugin),
	)
	if h == nil {
		t.Fatal("Wrap returned nil")
	}
}

func TestWrapAcceptsNilSerdes(t *testing.T) {
	// WithSerdes(nil) is valid — means "use default encoding/json".
	handler := func(_ Context, _ string) (string, error) { return "", nil }
	h := Wrap(handler, WithSerdes(nil))
	if h == nil {
		t.Fatal("Wrap returned nil")
	}
}

func TestWrapAcceptsNilLogHandler(t *testing.T) {
	// WithLogHandler(nil) is valid — means "use the default handler".
	handler := func(_ Context, _ string) (string, error) { return "", nil }
	h := Wrap(handler, WithLogHandler(nil))
	if h == nil {
		t.Fatal("Wrap returned nil")
	}
}

func TestWrapAcceptsNilExecutionClient(t *testing.T) {
	// WithExecutionClient(nil) is valid — means "use default AWS config".
	handler := func(_ Context, _ string) (string, error) { return "", nil }
	h := Wrap(handler, WithExecutionClient(nil))
	if h == nil {
		t.Fatal("Wrap returned nil")
	}
}

func TestValidateHandlerOptions(t *testing.T) {
	tests := []struct {
		name    string
		opts    *handlerOptions
		wantErr bool
		errMsg  string
	}{
		{
			name:    "empty options (all defaults)",
			opts:    &handlerOptions{},
			wantErr: false,
		},
		{
			name: "valid factory",
			opts: &handlerOptions{
				pluginFactories: []PluginFactory{
					PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) { return nil, nil }),
				},
			},
			wantErr: false,
		},
		{
			name: "nil factory",
			opts: &handlerOptions{
				pluginFactories: []PluginFactory{nil},
			},
			wantErr: true,
			errMsg:  "factory at index 0",
		},
		{
			name: "multiple factories, second nil",
			opts: &handlerOptions{
				pluginFactories: []PluginFactory{
					PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) { return nil, nil }),
					nil,
				},
			},
			wantErr: true,
			errMsg:  "factory at index 1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateHandlerOptions(tc.opts)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.errMsg != "" && !strings.Contains(err.Error(), tc.errMsg) {
					t.Errorf("error %q should contain %q", err.Error(), tc.errMsg)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}
