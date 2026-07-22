package plugin

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

type countingPlugin struct {
	NoopPlugin
	starts int32
}

func (p *countingPlugin) OnOperationStart(context.Context, OperationInfo) {
	atomic.AddInt32(&p.starts, 1)
}

func TestDispatch_EmptySlice_IsNoOp(t *testing.T) {
	// Must not panic and must not call anything - there is nothing to
	// call, but this also documents the "zero overhead when unconfigured"
	// contract this function's own doc promises.
	called := false
	Dispatch(nil, func(InstrumentationPlugin) { called = true })
	if called {
		t.Fatal("expected call to never be invoked for an empty/nil plugin slice")
	}
}

func TestDispatch_SinglePlugin_Invoked(t *testing.T) {
	p := &countingPlugin{}
	Dispatch([]InstrumentationPlugin{p}, func(ip InstrumentationPlugin) {
		ip.OnOperationStart(context.Background(), OperationInfo{})
	})
	if p.starts != 1 {
		t.Fatalf("expected 1 call, got %d", p.starts)
	}
}

func TestDispatch_MultiplePlugins_AllInvokedAndAwaited(t *testing.T) {
	const n = 5
	plugins := make([]InstrumentationPlugin, n)
	counters := make([]*countingPlugin, n)
	for i := range plugins {
		c := &countingPlugin{}
		counters[i] = c
		plugins[i] = c
	}

	Dispatch(plugins, func(ip InstrumentationPlugin) {
		ip.OnOperationStart(context.Background(), OperationInfo{})
	})

	// Dispatch must block until every plugin's call has returned (the
	// "awaited" contract) - if it returned early, some counters could
	// still be zero here.
	for i, c := range counters {
		if c.starts != 1 {
			t.Errorf("plugin %d: expected 1 call, got %d", i, c.starts)
		}
	}
}

func TestDispatch_PanicInOnePlugin_DoesNotAffectOthers(t *testing.T) {
	good1 := &countingPlugin{}
	panicky := &panicPlugin{}
	good2 := &countingPlugin{}

	Dispatch([]InstrumentationPlugin{good1, panicky, good2}, func(ip InstrumentationPlugin) {
		ip.OnOperationStart(context.Background(), OperationInfo{})
	})

	if good1.starts != 1 {
		t.Errorf("expected good1 to still be called despite panicky's panic, got %d calls", good1.starts)
	}
	if good2.starts != 1 {
		t.Errorf("expected good2 to still be called despite panicky's panic, got %d calls", good2.starts)
	}
}

func TestDispatch_SinglePanickyPlugin_DoesNotPropagate(t *testing.T) {
	// The single-plugin fast path (len(plugins)==1) is a separate code
	// path from the concurrent multi-plugin one - exercise it
	// independently to make sure the panic recovery applies there too.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic propagated out of Dispatch: %v", r)
		}
	}()
	Dispatch([]InstrumentationPlugin{&panicPlugin{}}, func(ip InstrumentationPlugin) {
		ip.OnOperationStart(context.Background(), OperationInfo{})
	})
}

type panicPlugin struct{ NoopPlugin }

func (panicPlugin) OnOperationStart(context.Context, OperationInfo) { panic("boom") }

func TestNoopPlugin_SatisfiesInterfaceWithoutOverrides(t *testing.T) {
	// A bare NoopPlugin must itself satisfy InstrumentationPlugin (the
	// whole point of the embeddable-no-op pattern - see that type's own
	// doc) and every method must be safely callable without panicking,
	// with the three Wrap* hooks passing fn's own result/error through
	// unchanged (i.e. wrapping nothing).
	var p InstrumentationPlugin = NoopPlugin{}
	p.OnInvocationStart(context.Background(), InvocationInfo{})
	p.OnInvocationEnd(context.Background(), InvocationEndInfo{})
	p.OnOperationStart(context.Background(), OperationInfo{})
	p.OnOperationEnd(context.Background(), OperationEndInfo{})
	p.OnOperationAttemptStart(context.Background(), AttemptInfo{})
	p.OnOperationAttemptEnd(context.Background(), AttemptEndInfo{})
	p.OnOperationChange(context.Background(), OperationChangeInfo{})
	if got := p.EnrichLogContext(); got != nil {
		t.Errorf("expected EnrichLogContext to return nil, got %v", got)
	}

	wantResult := "unchanged"
	wantErr := error(nil)
	gotResult, gotErr := p.WrapInvocation(context.Background(), InvocationInfo{}, func() (any, error) { return wantResult, wantErr })
	if gotResult != wantResult || gotErr != wantErr {
		t.Errorf("WrapInvocation: expected fn's result/error passed through unchanged, got (%v, %v)", gotResult, gotErr)
	}
	gotResult, gotErr = p.WrapChildContextFn(context.Background(), OperationInfo{}, func() (any, error) { return wantResult, wantErr })
	if gotResult != wantResult || gotErr != wantErr {
		t.Errorf("WrapChildContextFn: expected fn's result/error passed through unchanged, got (%v, %v)", gotResult, gotErr)
	}
	gotResult, gotErr = p.WrapOperationAttemptFn(context.Background(), AttemptInfo{}, func() (any, error) { return wantResult, wantErr })
	if gotResult != wantResult || gotErr != wantErr {
		t.Errorf("WrapOperationAttemptFn: expected fn's result/error passed through unchanged, got (%v, %v)", gotResult, gotErr)
	}
}

func TestDispatch_ConcurrentCallsAreIndependent(t *testing.T) {
	// Sanity check that Dispatch really does run plugins concurrently
	// (not just claim to) by having each plugin block on a shared
	// WaitGroup that only releases once ALL of them have started -
	// this would deadlock if Dispatch ran them sequentially and blocked
	// on each one's return before starting the next.
	const n = 4
	var startedWg sync.WaitGroup
	startedWg.Add(n)
	var releaseWg sync.WaitGroup
	releaseWg.Add(1)

	plugins := make([]InstrumentationPlugin, n)
	for i := 0; i < n; i++ {
		plugins[i] = &blockingPlugin{startedWg: &startedWg, releaseWg: &releaseWg}
	}

	done := make(chan struct{})
	go func() {
		Dispatch(plugins, func(ip InstrumentationPlugin) {
			ip.OnOperationStart(context.Background(), OperationInfo{})
		})
		close(done)
	}()

	startedWg.Wait() // all n plugins have entered their hook concurrently
	releaseWg.Done() // let them all finish
	<-done
}

type blockingPlugin struct {
	NoopPlugin
	startedWg *sync.WaitGroup
	releaseWg *sync.WaitGroup
}

func (p *blockingPlugin) OnOperationStart(context.Context, OperationInfo) {
	p.startedWg.Done()
	p.releaseWg.Wait()
}
