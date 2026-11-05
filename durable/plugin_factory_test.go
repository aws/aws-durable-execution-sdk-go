package durable

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-lambda-go/lambdacontext"
)

// These tests cover the PluginFactory contract: when the handler calls a
// factory, what it passes, how it treats each result, and how the plugins
// a factory returns are isolated between invocations.

func runFactoryHandler(t *testing.T, ctx context.Context, arn string, opts ...HandlerOption) []byte {
	t.Helper()
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s1", func(StepContext) (string, error) { return "one", nil })
	}, append(opts, withLambdaAPI(&fakePluginClient{}))...)
	resp, err := handler(ctx, makePluginPayload(t, arn, "tok1", nil))
	if err != nil {
		t.Fatal(err)
	}
	assertPluginResponseStatus(t, resp, "SUCCEEDED")
	return resp
}

// factoryErrors returns the error-level records the handler logged about
// plugin factories.
func factoryErrors(rec *recordingHandler) []recordedLog {
	var out []recordedLog
	for _, r := range rec.all() {
		if r.level == slog.LevelError && strings.HasPrefix(r.message, "Plugin factory") {
			out = append(out, r)
		}
	}
	return out
}

func TestPluginFactoryCalledOncePerInvocationBeforeHooks(t *testing.T) {
	var calls, hooks atomic.Int32
	var order []string
	var mu sync.Mutex
	factory := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		calls.Add(1)
		mu.Lock()
		order = append(order, "factory")
		mu.Unlock()
		return &Plugin{
			OnInvocationStart: func(context.Context, InvocationHookInfo) {
				mu.Lock()
				order = append(order, "start")
				mu.Unlock()
			},
			OnOperationStart: func(context.Context, OperationHookInfo) { hooks.Add(1) },
			OnInvocationEnd: func(context.Context, InvocationEndHookInfo) {
				mu.Lock()
				order = append(order, "end")
				mu.Unlock()
			},
		}, nil
	})
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s1", func(StepContext) (string, error) { return "one", nil })
	}, WithPlugins(factory), withLambdaAPI(&fakePluginClient{}))

	for i := 0; i < 3; i++ {
		if _, err := handler(makePluginContext(), makePluginPayload(t, "arn:test:factory", fmt.Sprintf("tok%d", i), nil)); err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("factory calls = %d, want one per invocation (3)", got)
	}
	if hooks.Load() == 0 {
		t.Error("the plugin the factory returned received no operation hooks")
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"factory", "start", "end", "factory", "start", "end", "factory", "start", "end"}
	if !reflect.DeepEqual(order, want) {
		t.Errorf("order = %v, want %v: the factory runs before OnInvocationStart on every invocation", order, want)
	}
}

func TestPluginFactoryReceivesSameInfoAsOnInvocationStart(t *testing.T) {
	var fromFactory, fromHook InvocationHookInfo
	var mu sync.Mutex
	factory := PluginFactoryFunc(func(_ context.Context, info InvocationHookInfo) (*Plugin, error) {
		mu.Lock()
		fromFactory = info
		mu.Unlock()
		return &Plugin{OnInvocationStart: func(_ context.Context, info InvocationHookInfo) {
			mu.Lock()
			fromHook = info
			mu.Unlock()
		}}, nil
	})
	lc := &lambdacontext.LambdaContext{AwsRequestID: "req-123"}
	runFactoryHandler(t, lambdacontext.NewContext(context.Background(), lc), "arn:test:factory", WithPlugins(factory))
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(fromFactory, fromHook) {
		t.Errorf("factory info and OnInvocationStart info differ:\nfactory: %+v\nhook:    %+v", fromFactory, fromHook)
	}
	if fromFactory.ExecutionArn != "arn:test:factory" {
		t.Errorf("ExecutionArn = %q", fromFactory.ExecutionArn)
	}
	if fromFactory.RequestID != "req-123" {
		t.Errorf("RequestID = %q, want the Lambda request ID", fromFactory.RequestID)
	}
	if !fromFactory.IsFirstInvocation {
		t.Error("IsFirstInvocation = false on the first invocation")
	}
	if len(fromFactory.Operations) != 1 {
		t.Errorf("Operations = %v, want the execution operation alone", fromFactory.Operations)
	}
}

// The factory receives the invocation context: the one the Lambda runtime
// passed to the handler, carrying the lambdacontext value, and the same one
// OnInvocationStart then receives.
func TestPluginFactoryReceivesInvocationContext(t *testing.T) {
	var fromFactory, fromHook context.Context
	var mu sync.Mutex
	factory := PluginFactoryFunc(func(ctx context.Context, _ InvocationHookInfo) (*Plugin, error) {
		mu.Lock()
		fromFactory = ctx
		mu.Unlock()
		return &Plugin{OnInvocationStart: func(ctx context.Context, _ InvocationHookInfo) {
			mu.Lock()
			fromHook = ctx
			mu.Unlock()
		}}, nil
	})
	lc := &lambdacontext.LambdaContext{AwsRequestID: "req-ctx"}
	runFactoryHandler(t, lambdacontext.NewContext(context.Background(), lc), "arn:test:factory-ctx", WithPlugins(factory))
	mu.Lock()
	defer mu.Unlock()
	if fromFactory == nil {
		t.Fatal("factory received a nil context")
	}
	got, ok := lambdacontext.FromContext(fromFactory)
	if !ok || got.AwsRequestID != "req-ctx" {
		t.Errorf("factory ctx lambdacontext = %+v, %v; want the invocation's (req-ctx)", got, ok)
	}
	if fromFactory != fromHook {
		t.Errorf("factory ctx and OnInvocationStart ctx differ:\nfactory: %v\nhook:    %v", fromFactory, fromHook)
	}
}

func TestPluginFactoryRequestIDEmptyOutsideLambda(t *testing.T) {
	var got InvocationHookInfo
	var mu sync.Mutex
	factory := PluginFactoryFunc(func(_ context.Context, info InvocationHookInfo) (*Plugin, error) {
		mu.Lock()
		got = info
		mu.Unlock()
		return nil, nil
	})
	runFactoryHandler(t, context.Background(), "arn:test:factory", WithPlugins(factory))
	mu.Lock()
	defer mu.Unlock()
	if got.RequestID != "" {
		t.Errorf("RequestID = %q, want empty without a Lambda context", got.RequestID)
	}
}

func TestPluginFactoryNilSkipsSilently(t *testing.T) {
	rec := newRecordingHandler()
	var skipped, hooked atomic.Int32
	skipping := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		skipped.Add(1)
		return nil, nil
	})
	healthy := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		return &Plugin{OnOperationEnd: func(context.Context, OperationHookInfo) { hooked.Add(1) }}, nil
	})
	runFactoryHandler(t, context.Background(), "arn:test:factory", WithPlugins(skipping, healthy), WithLogHandler(rec))
	if skipped.Load() != 1 {
		t.Errorf("skipping factory called %d times, want 1", skipped.Load())
	}
	if hooked.Load() == 0 {
		t.Error("the healthy factory's plugin received no hooks")
	}
	if logs := factoryErrors(rec); len(logs) != 0 {
		t.Errorf("nil, nil must skip without logging; got %v", logs)
	}
}

func TestPluginFactoryErrorSkipsAndLogs(t *testing.T) {
	rec := newRecordingHandler()
	var hooked atomic.Int32
	failing := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		return nil, errors.New("exporter unavailable")
	})
	healthy := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		return &Plugin{OnOperationEnd: func(context.Context, OperationHookInfo) { hooked.Add(1) }}, nil
	})
	runFactoryHandler(t, context.Background(), "arn:test:factory", WithPlugins(failing, healthy), WithLogHandler(rec))
	if hooked.Load() == 0 {
		t.Error("the healthy factory's plugin received no hooks")
	}
	logs := factoryErrors(rec)
	if len(logs) != 1 {
		t.Fatalf("factory error logs = %v, want exactly one", logs)
	}
	if logs[0].attrs["factoryIndex"] != int64(0) {
		t.Errorf("factoryIndex = %v, want 0", logs[0].attrs["factoryIndex"])
	}
	if logs[0].attrs["errorType"] != "Error" {
		t.Errorf("errorType = %v, want Error", logs[0].attrs["errorType"])
	}
	if logs[0].attrs["errorMessage"] != "exporter unavailable" {
		t.Errorf("errorMessage = %v, want the factory's error text", logs[0].attrs["errorMessage"])
	}
}

// A factory error record must carry the same execution attributes every
// other record of the invocation carries, so an operator can join it to the
// Lambda request, the execution, and the tenant. The factories run before
// the execution context exists, so the handler must attach the attributes
// to the factory logger itself.
func TestPluginFactoryErrorLogCarriesExecutionAttrs(t *testing.T) {
	failing := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		return nil, errors.New("exporter unavailable")
	})

	t.Run("with tenant", func(t *testing.T) {
		rec := newRecordingHandler()
		runFactoryHandler(t, lambdaCtx(t, "req-f", "tenant-f"), "arn:test:factory", WithPlugins(failing), WithLogHandler(rec))
		logs := factoryErrors(rec)
		if len(logs) != 1 {
			t.Fatalf("factory error logs = %v, want exactly one", logs)
		}
		attrs := logs[0].attrs
		if attrs[logKeyRequestID] != "req-f" {
			t.Errorf("%s = %v, want req-f", logKeyRequestID, attrs[logKeyRequestID])
		}
		if attrs[logKeyExecutionArn] != "arn:test:factory" {
			t.Errorf("%s = %v, want arn:test:factory", logKeyExecutionArn, attrs[logKeyExecutionArn])
		}
		if attrs[logKeyTenantID] != "tenant-f" {
			t.Errorf("%s = %v, want tenant-f", logKeyTenantID, attrs[logKeyTenantID])
		}
	})

	t.Run("without tenant", func(t *testing.T) {
		rec := newRecordingHandler()
		runFactoryHandler(t, lambdaCtx(t, "req-g", ""), "arn:test:factory", WithPlugins(failing), WithLogHandler(rec))
		logs := factoryErrors(rec)
		if len(logs) != 1 {
			t.Fatalf("factory error logs = %v, want exactly one", logs)
		}
		attrs := logs[0].attrs
		if attrs[logKeyRequestID] != "req-g" {
			t.Errorf("%s = %v, want req-g", logKeyRequestID, attrs[logKeyRequestID])
		}
		if attrs[logKeyExecutionArn] != "arn:test:factory" {
			t.Errorf("%s = %v, want arn:test:factory", logKeyExecutionArn, attrs[logKeyExecutionArn])
		}
		if _, present := attrs[logKeyTenantID]; present {
			t.Errorf("%s = %v, want absent when the invocation has no tenant", logKeyTenantID, attrs[logKeyTenantID])
		}
	})
}

func TestPluginFactoryPanicSkipsAndLogs(t *testing.T) {
	rec := newRecordingHandler()
	var hooked atomic.Int32
	panicking := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		panic("factory defect")
	})
	healthy := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		return &Plugin{OnOperationEnd: func(context.Context, OperationHookInfo) { hooked.Add(1) }}, nil
	})
	runFactoryHandler(t, context.Background(), "arn:test:factory", WithPlugins(panicking, healthy), WithLogHandler(rec))
	if hooked.Load() == 0 {
		t.Error("the healthy factory's plugin received no hooks")
	}
	logs := factoryErrors(rec)
	if len(logs) != 1 {
		t.Fatalf("factory error logs = %v, want exactly one", logs)
	}
	if logs[0].attrs["errorType"] != "Error" {
		t.Errorf("errorType = %v, want Error", logs[0].attrs["errorType"])
	}
	if text, _ := logs[0].attrs["errorMessage"].(string); !strings.Contains(text, "factory defect") {
		t.Errorf("errorMessage = %v, want the recovered panic value", logs[0].attrs["errorMessage"])
	}
}

func TestPluginFactoryHooklessPluginSkipsAndLogs(t *testing.T) {
	rec := newRecordingHandler()
	empty := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		return &Plugin{}, nil
	})
	runFactoryHandler(t, context.Background(), "arn:test:factory", WithPlugins(empty), WithLogHandler(rec))
	logs := factoryErrors(rec)
	if len(logs) != 1 || !strings.Contains(logs[0].message, "no hook functions set") {
		t.Fatalf("factory error logs = %v, want one record about the hookless plugin", logs)
	}
}

// namedFactory exercises the Stringer path of pluginFactoryName.
type namedFactory struct{}

func (namedFactory) CreatePlugin(context.Context, InvocationHookInfo) (*Plugin, error) {
	return nil, errors.New("boom")
}
func (namedFactory) String() string { return "audit-factory" }

func TestPluginFactoryNameInDiagnostics(t *testing.T) {
	rec := newRecordingHandler()
	runFactoryHandler(t, context.Background(), "arn:test:factory", WithPlugins(namedFactory{}), WithLogHandler(rec))
	logs := factoryErrors(rec)
	if len(logs) != 1 || logs[0].attrs["factory"] != "audit-factory" {
		t.Fatalf("factory error logs = %v, want factory=%q", logs, "audit-factory")
	}
	if got := pluginFactoryName(PluginFactoryFunc(nil)); got != "durable.PluginFactoryFunc" {
		t.Errorf("pluginFactoryName(PluginFactoryFunc) = %q", got)
	}
}

// panickingNameFactory has a String() that panics. pluginFactoryName must
// fall back to the dynamic type instead of letting the panic leave Invoke.
type panickingNameFactory struct{ result *Plugin }

func (f panickingNameFactory) CreatePlugin(context.Context, InvocationHookInfo) (*Plugin, error) {
	if f.result != nil {
		return f.result, nil
	}
	return nil, errors.New("boom")
}
func (panickingNameFactory) String() string { panic("String defect") }

func TestPluginFactoryPanickingStringFallsBackToType(t *testing.T) {
	t.Run("error result", func(t *testing.T) {
		rec := newRecordingHandler()
		runFactoryHandler(t, context.Background(), "arn:test:factory", WithPlugins(panickingNameFactory{}), WithLogHandler(rec))
		logs := factoryErrors(rec)
		if len(logs) != 1 {
			t.Fatalf("factory error logs = %v, want exactly one", logs)
		}
		if logs[0].attrs["factory"] != "durable.panickingNameFactory" {
			t.Errorf("factory attr = %v, want the dynamic type", logs[0].attrs["factory"])
		}
	})
	t.Run("hookless result", func(t *testing.T) {
		rec := newRecordingHandler()
		runFactoryHandler(t, context.Background(), "arn:test:factory", WithPlugins(panickingNameFactory{result: &Plugin{}}), WithLogHandler(rec))
		logs := factoryErrors(rec)
		if len(logs) != 1 || !strings.Contains(logs[0].message, "no hook functions set") {
			t.Fatalf("factory error logs = %v, want one record about the hookless plugin", logs)
		}
		if logs[0].attrs["factory"] != "durable.panickingNameFactory" {
			t.Errorf("factory attr = %v, want the dynamic type", logs[0].attrs["factory"])
		}
	})
}

// typedNilErr has a pointer receiver that dereferences itself, so a nil
// *typedNilErr stored in an error interface is non-nil and its Error()
// panics. The SDK's default handler expands error attributes through
// ReplaceAttr (replaceDefaultAttr), which calls Error() before slog's own
// recover in appendValue. So the default handler is the one that reaches
// the panic; a plain slog.NewJSONHandler would render "<nil>" instead.
type typedNilErr struct{ reason string }

func (e *typedNilErr) Error() string { return e.reason }

func TestPluginFactoryTypedNilErrorUnderDefaultLogger(t *testing.T) {
	var buf bytes.Buffer
	jsonLogger := newDefaultLogHandler(&buf, slog.LevelInfo)
	var hooked atomic.Int32
	failing := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		var e *typedNilErr
		return nil, e
	})
	healthy := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		return &Plugin{OnOperationEnd: func(context.Context, OperationHookInfo) { hooked.Add(1) }}, nil
	})
	runFactoryHandler(t, context.Background(), "arn:test:factory", WithPlugins(failing, healthy), WithLogHandler(jsonLogger))
	if hooked.Load() == 0 {
		t.Error("the healthy factory's plugin received no hooks")
	}
	out := buf.String()
	if !strings.Contains(out, "Plugin factory failed") {
		t.Fatalf("default handler output lacks the factory failure record:\n%s", out)
	}
	if !strings.Contains(out, `"errorType":"typedNilErr"`) {
		t.Errorf("default handler output lacks errorType:\n%s", out)
	}
	if !strings.Contains(out, `"errorMessage":"Error() panicked`) {
		t.Errorf("default handler output lacks the recovered Error() text:\n%s", out)
	}
}

// panickingErr panics in Error() for a reason other than a nil receiver,
// so the recover in factoryErrorMessage is shown to be general.
type panickingErr struct{}

func (panickingErr) Error() string { panic("Error defect") }

func TestPluginFactoryPanickingErrorMethodIsContained(t *testing.T) {
	rec := newRecordingHandler()
	failing := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
		return nil, panickingErr{}
	})
	runFactoryHandler(t, context.Background(), "arn:test:factory", WithPlugins(failing), WithLogHandler(rec))
	logs := factoryErrors(rec)
	if len(logs) != 1 {
		t.Fatalf("factory error logs = %v, want exactly one", logs)
	}
	if logs[0].attrs["errorType"] != "panickingErr" {
		t.Errorf("errorType = %v, want panickingErr", logs[0].attrs["errorType"])
	}
	if text, _ := logs[0].attrs["errorMessage"].(string); !strings.Contains(text, "Error() panicked: Error defect") {
		t.Errorf("errorMessage = %v, want the recovered Error() panic", logs[0].attrs["errorMessage"])
	}
}

func TestPluginFactoryWrapOrderIsRegistrationOrder(t *testing.T) {
	var order []string
	var mu sync.Mutex
	wrapper := func(name string) PluginFactory {
		return PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) {
			return &Plugin{WrapInvocation: func(ctx context.Context, _ InvocationHookInfo, fn func(context.Context) (any, error)) (any, error) {
				mu.Lock()
				order = append(order, name+":before")
				mu.Unlock()
				r, err := fn(ctx)
				mu.Lock()
				order = append(order, name+":after")
				mu.Unlock()
				return r, err
			}}, nil
		})
	}
	skipping := PluginFactoryFunc(func(context.Context, InvocationHookInfo) (*Plugin, error) { return nil, nil })
	runFactoryHandler(t, context.Background(), "arn:test:factory", WithPlugins(wrapper("A"), skipping, wrapper("B")))
	mu.Lock()
	defer mu.Unlock()
	want := []string{"A:before", "B:before", "B:after", "A:after"}
	if !reflect.DeepEqual(order, want) {
		t.Errorf("wrap order = %v, want %v: the first registered factory is outermost and a skipped factory does not disturb the order", order, want)
	}
}

func TestPluginFactoryConcurrentInvocationsGetIsolatedPlugins(t *testing.T) {
	// Each plugin records the ARN it was created for and the ARNs its
	// hooks observe. Under -race this also proves the factory and the
	// handler share no unsynchronized state across invocations.
	type observed struct {
		created string
		hooked  []string
	}
	var all []*observed
	var mu sync.Mutex
	factory := PluginFactoryFunc(func(_ context.Context, info InvocationHookInfo) (*Plugin, error) {
		o := &observed{created: info.ExecutionArn}
		mu.Lock()
		all = append(all, o)
		mu.Unlock()
		return &Plugin{OnOperationEnd: func(_ context.Context, info OperationHookInfo) {
			mu.Lock()
			o.hooked = append(o.hooked, info.ExecutionArn)
			mu.Unlock()
		}}, nil
	})
	handler := Wrap(func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s1", func(StepContext) (string, error) { return "one", nil })
	}, WithPlugins(factory), withLambdaAPI(&fakePluginClient{}))

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			arn := fmt.Sprintf("arn:test:concurrent:%d", i)
			if _, err := handler(makePluginContext(), makePluginPayload(t, arn, "tok", nil)); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(all) != n {
		t.Fatalf("plugins created = %d, want %d", len(all), n)
	}
	for _, o := range all {
		if len(o.hooked) == 0 {
			t.Errorf("plugin for %s saw no hooks", o.created)
		}
		for _, arn := range o.hooked {
			if arn != o.created {
				t.Errorf("plugin created for %s observed an operation of %s", o.created, arn)
			}
		}
	}
}
