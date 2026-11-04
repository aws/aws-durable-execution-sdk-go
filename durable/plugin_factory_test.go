package durable

import (
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
	factory := PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) {
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
	factory := PluginFactoryFunc(func(info InvocationHookInfo) (*Plugin, error) {
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

func TestPluginFactoryRequestIDEmptyOutsideLambda(t *testing.T) {
	var got InvocationHookInfo
	var mu sync.Mutex
	factory := PluginFactoryFunc(func(info InvocationHookInfo) (*Plugin, error) {
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
	skipping := PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) {
		skipped.Add(1)
		return nil, nil
	})
	healthy := PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) {
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
	failing := PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) {
		return nil, errors.New("exporter unavailable")
	})
	healthy := PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) {
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
	if err, _ := logs[0].attrs["error"].(error); err == nil || err.Error() != "exporter unavailable" {
		t.Errorf("error attr = %v, want the factory's error", logs[0].attrs["error"])
	}
}

func TestPluginFactoryPanicSkipsAndLogs(t *testing.T) {
	rec := newRecordingHandler()
	var hooked atomic.Int32
	panicking := PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) {
		panic("factory defect")
	})
	healthy := PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) {
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
	if err, _ := logs[0].attrs["error"].(error); err == nil || !strings.Contains(err.Error(), "factory defect") {
		t.Errorf("error attr = %v, want the recovered panic value", logs[0].attrs["error"])
	}
}

func TestPluginFactoryHooklessPluginSkipsAndLogs(t *testing.T) {
	rec := newRecordingHandler()
	empty := PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) {
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

func (namedFactory) CreatePlugin(InvocationHookInfo) (*Plugin, error) { return nil, errors.New("boom") }
func (namedFactory) String() string                                   { return "audit-factory" }

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

func TestPluginFactoryWrapOrderIsRegistrationOrder(t *testing.T) {
	var order []string
	var mu sync.Mutex
	wrapper := func(name string) PluginFactory {
		return PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) {
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
	skipping := PluginFactoryFunc(func(InvocationHookInfo) (*Plugin, error) { return nil, nil })
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
	factory := PluginFactoryFunc(func(info InvocationHookInfo) (*Plugin, error) {
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
