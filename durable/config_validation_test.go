package durable

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
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
	valid := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		return &Plugin{OnInvocationStart: func(context.Context, InvocationHookInfo) {}}, nil
	})
	Wrap(handler, WithPlugins(valid, nil))
}

// ptrFactory is a pointer-receiver PluginFactory whose CreatePlugin reads
// no fields, like telemetry.TelemetryPlugin. A typed nil *ptrFactory is a
// non-nil PluginFactory interface, so f == nil does not catch it.
type ptrFactory struct{}

func (*ptrFactory) CreatePlugin(context.Context, InvocationHookInfo) (*Plugin, error) {
	return &Plugin{OnInvocationStart: func(context.Context, InvocationHookInfo) {}}, nil
}

// valueFactory is a value-type PluginFactory. A struct kind cannot hold
// nil, so it exercises the kind guard around reflect.Value.IsNil.
type valueFactory struct{}

func (valueFactory) CreatePlugin(context.Context, InvocationHookInfo) (*Plugin, error) {
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
		{name: "PluginFactoryFunc", factory: PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
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
					PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) { return nil, nil }),
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
					PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) { return nil, nil }),
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

// registrationFactory lets tests observe metadata evaluation separately from
// plugin creation. Embedding PluginFactory keeps plain factories usable too.
type registrationFactory struct {
	PluginFactory
	registration func() PluginRegistration
}

func (f *registrationFactory) Registration() PluginRegistration { return f.registration() }

func requirePluginConfigurationPanic(t *testing.T, construct func(), want ...string) {
	t.Helper()
	defer func() {
		r := recover()
		message, ok := r.(string)
		if !ok {
			t.Fatalf("configuration panic = %T: %v, want a string from Wrap validation", r, r)
		}
		for _, text := range append([]string{"durable: WithPlugins:"}, want...) {
			if !strings.Contains(message, text) {
				t.Errorf("configuration panic = %q, want %q", message, text)
			}
		}
	}()
	construct()
	t.Fatal("handler construction succeeded, want a configuration panic")
}

func TestPluginRegistrationExclusiveGroups(t *testing.T) {
	cases := []struct {
		name   string
		groups [][]string
		want   []string
	}{
		{name: "same group", groups: [][]string{{"views"}, {"views"}}, want: []string{"index 0", "factory-0", "index 1", "factory-1", `"views"`}},
		{name: "two groups conflict with first", groups: [][]string{{"views", "exporters"}, {"views"}, {"exporters"}}, want: []string{"index 0", "index 1", `"views"`}},
		{name: "two groups conflict with second", groups: [][]string{{"views", "exporters"}, {"exporters"}, {"views"}}, want: []string{"index 0", "index 1", `"exporters"`}},
		{name: "two groups declared last", groups: [][]string{{"views"}, {"exporters"}, {"views", "exporters"}}, want: []string{"index 0", "index 2", `"views"`}},
		{name: "duplicate within entry", groups: [][]string{{"views", "views"}}},
		{name: "duplicate still conflicts across entries", groups: [][]string{{"views", "views"}, {"views"}}, want: []string{"index 0", "index 1", `"views"`}},
		{name: "empty element", groups: [][]string{{""}}, want: []string{"index 0", "factory-0", "blank exclusive group"}},
		{name: "whitespace element", groups: [][]string{{" \t\n"}}, want: []string{"index 0", "factory-0", "blank exclusive group"}},
		{name: "unicode whitespace element", groups: [][]string{{"\u2003"}}, want: []string{"index 0", "blank exclusive group"}},
		{name: "blank after valid element", groups: [][]string{{"views", " "}}, want: []string{"index 0", "blank exclusive group"}},
		{name: "nil groups", groups: [][]string{nil, nil}},
		{name: "empty groups", groups: [][]string{{}, {}}},
		{name: "exact case and whitespace", groups: [][]string{{"views"}, {"Views"}, {" views "}}},
		{name: "same spaced identifier", groups: [][]string{{" views "}, {" views "}}, want: []string{"index 0", "index 1", `" views "`}},
		{name: "disjoint multiple groups", groups: [][]string{{"views", "exporters"}, {"audit", "logs"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var created, hooked atomic.Int32
			var factories []PluginFactory
			for i, groups := range tc.groups {
				factories = append(factories, &registrationFactory{
					PluginFactory: PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
						created.Add(1)
						return &Plugin{OnInvocationStart: func(context.Context, InvocationHookInfo) { hooked.Add(1) }}, nil
					}),
					registration: func() PluginRegistration {
						return PluginRegistration{Name: fmt.Sprintf("factory-%d", i), ExclusiveGroups: groups}
					},
				})
			}
			if len(tc.want) > 0 {
				handler := func(Context, string) (string, error) { return "ok", nil }
				for _, constructor := range []struct {
					name string
					call func()
				}{
					{"Wrap", func() { Wrap(handler, WithPlugins(factories...)) }},
					{"Start", func() { Start(handler, WithPlugins(factories...)) }},
				} {
					t.Run(constructor.name, func(t *testing.T) {
						requirePluginConfigurationPanic(t, constructor.call, tc.want...)
					})
				}
				if created.Load() != 0 || hooked.Load() != 0 {
					t.Errorf("invalid registration ran %d factories and %d hooks", created.Load(), hooked.Load())
				}
				return
			}
			runFactoryHandler(t, context.Background(), "arn:test:registration", WithPlugins(factories...))
			if got := int(created.Load()); got != len(factories) {
				t.Errorf("created plugins = %d, want %d", got, len(factories))
			}
			if got := int(hooked.Load()); got != len(factories) {
				t.Errorf("invocation hooks = %d, want %d", got, len(factories))
			}
		})
	}
}

func TestPluginRegistrationWithPlainFactory(t *testing.T) {
	var hooked atomic.Int32
	plain := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		return &Plugin{OnInvocationStart: func(context.Context, InvocationHookInfo) { hooked.Add(1) }}, nil
	})
	registered := &registrationFactory{
		PluginFactory: plain,
		registration: func() PluginRegistration {
			return PluginRegistration{Name: "registered", ExclusiveGroups: []string{"views"}}
		},
	}
	runFactoryHandler(t, context.Background(), "arn:test:mixed", WithPlugins(registered), WithPlugins(plain))
	if got := hooked.Load(); got != 2 {
		t.Errorf("invocation hooks = %d, want both registered and plain factories (2)", got)
	}
}

func TestPluginRegistrationSameNameConflictAcrossOptions(t *testing.T) {
	factory := &registrationFactory{registration: func() PluginRegistration {
		return PluginRegistration{Name: "same-name", ExclusiveGroups: []string{"views"}}
	}}
	requirePluginConfigurationPanic(t, func() {
		Wrap(func(Context, string) (string, error) { return "", nil }, WithPlugins(factory), WithPlugins(factory))
	}, `index 0 ("same-name")`, `index 1 ("same-name")`, `"views"`)
}

// nilSafeRegistrationFactory would allow Registration on a nil receiver.
// The counter detects a call even if its metadata or panic were ignored.
type nilSafeRegistrationFactory struct{}

var nilRegistrationCalls atomic.Int32

func (*nilSafeRegistrationFactory) Registration() PluginRegistration {
	nilRegistrationCalls.Add(1)
	return PluginRegistration{ExclusiveGroups: []string{""}}
}

func (*nilSafeRegistrationFactory) CreatePlugin(context.Context, InvocationHookInfo) (*Plugin, error) {
	panic("nil factory must not be invoked")
}

func TestPluginRegistrationNilFactoryTakesPrecedence(t *testing.T) {
	before := nilRegistrationCalls.Load()
	calls := 0
	first := &registrationFactory{registration: func() PluginRegistration {
		calls++
		panic("metadata must not run before all nil checks")
	}}
	opts := &handlerOptions{pluginFactories: []PluginFactory{first, (*nilSafeRegistrationFactory)(nil)}}
	want := "durable: WithPlugins: factory at index 1 is nil"
	if err := validateHandlerOptions(opts); err == nil || err.Error() != want {
		t.Fatalf("validation error = %v, want %q", err, want)
	}
	requirePluginConfigurationPanic(t, func() {
		Wrap(func(Context, string) (string, error) { return "", nil }, WithPlugins(opts.pluginFactories...))
	}, want)
	if calls != 0 || nilRegistrationCalls.Load() != before {
		t.Errorf("Registration calls: first=%d, typed nil=%d; want 0 each", calls, nilRegistrationCalls.Load()-before)
	}
}

func TestPluginRegistrationPanicFailsConstruction(t *testing.T) {
	for _, value := range []any{"registration defect", nil, panickingErr{}} {
		t.Run(fmt.Sprintf("%T", value), func(t *testing.T) {
			var registrationCalls, laterCalls, created, hooked int
			plain := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
				created++
				return &Plugin{OnInvocationStart: func(context.Context, InvocationHookInfo) { hooked++ }}, nil
			})
			bad := &registrationFactory{PluginFactory: plain, registration: func() PluginRegistration {
				registrationCalls++
				panic(value)
			}}
			later := &registrationFactory{PluginFactory: plain, registration: func() PluginRegistration {
				laterCalls++
				return PluginRegistration{}
			}}
			factories := []PluginFactory{plain, bad, later}
			want := "durable: WithPlugins: factory at index 1 (*durable.registrationFactory): Registration() panicked"
			if err := validateHandlerOptions(&handlerOptions{pluginFactories: factories}); err == nil || err.Error() != want {
				t.Fatalf("validation error = %v, want %q", err, want)
			}
			handler := func(Context, string) (string, error) { return "", nil }
			requirePluginConfigurationPanic(t, func() { Wrap(handler, WithPlugins(factories...)) }, want)
			requirePluginConfigurationPanic(t, func() { Start(handler, WithPlugins(factories...)) }, want)
			if registrationCalls != 3 || laterCalls != 0 || created != 0 || hooked != 0 {
				t.Errorf("calls: Registration=%d, later=%d, CreatePlugin=%d, hooks=%d; want 3, 0, 0, 0",
					registrationCalls, laterCalls, created, hooked)
			}
		})
	}
}
