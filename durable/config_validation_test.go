package durable

import (
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

func TestWrapPanicsOnEmptyPlugin(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Wrap with empty Plugin should panic")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if !strings.Contains(msg, "plugin at index 0") {
			t.Errorf("panic message should identify the offending plugin: %q", msg)
		}
	}()
	handler := func(_ Context, _ string) (string, error) { return "", nil }
	Wrap(handler, WithPlugins(Plugin{}))
}

func TestWrapPanicsOnSecondEmptyPlugin(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Wrap with second empty Plugin should panic")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if !strings.Contains(msg, "plugin at index 1") {
			t.Errorf("panic message should identify index 1: %q", msg)
		}
	}()
	handler := func(_ Context, _ string) (string, error) { return "", nil }
	valid := Plugin{OnInvocationStart: func(InvocationHookInfo) {}}
	Wrap(handler, WithPlugins(valid, Plugin{}))
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
	plugin := Plugin{OnInvocationStart: func(InvocationHookInfo) {}}
	h := Wrap(handler,
		WithLogger(newDefaultLogger("test")),
		WithPlugins(plugin),
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

func TestWrapAcceptsNilLogger(t *testing.T) {
	// WithLogger(nil) is valid — means "use default structured logger".
	handler := func(_ Context, _ string) (string, error) { return "", nil }
	h := Wrap(handler, WithLogger(nil))
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
			name: "valid plugin",
			opts: &handlerOptions{
				plugins: []Plugin{
					{OnInvocationStart: func(InvocationHookInfo) {}},
				},
			},
			wantErr: false,
		},
		{
			name: "empty plugin (no hooks)",
			opts: &handlerOptions{
				plugins: []Plugin{{}},
			},
			wantErr: true,
			errMsg:  "plugin at index 0",
		},
		{
			name: "multiple plugins, second empty",
			opts: &handlerOptions{
				plugins: []Plugin{
					{OnInvocationEnd: func(InvocationEndHookInfo) {}},
					{},
				},
			},
			wantErr: true,
			errMsg:  "plugin at index 1",
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
