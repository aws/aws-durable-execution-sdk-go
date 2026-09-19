package goroutine

import (
	"sync"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"golang.org/x/sync/errgroup"
)

func work(durable.StepContext) (string, error) { return "", nil }

func goStatement(ctx durable.Context) {
	go func() {
		durable.Step(ctx, "a", work) // want `durable.Step runs on a goroutine started by the go statement`
	}()

	go func(c durable.Context) {
		durable.Wait(c, "b", time.Second) // want `durable.Wait runs on a goroutine started by the go statement`
	}(ctx)

	go (func() {
		durable.Step(ctx, "c", work) // want `durable.Step runs on a goroutine started by the go statement`
	})()

	go durable.Wait(ctx, "d", time.Second) // want `durable.Wait is started with the go statement`

	go func() {
		helper := func() {
			durable.Step(ctx, "e", work) // want `durable.Step runs on a goroutine started by the go statement`
		}
		helper()
	}()

	// durable.Go inside a go statement is itself a durable operation on the
	// parent; the step on the child is fine.
	go func() {
		fut := durable.Go(ctx, "f", func(child durable.Context) (string, error) { // want `durable.Go runs on a goroutine started by the go statement`
			return durable.Step(child, "g", work)
		})
		_, _ = fut.Result()
	}()
}

func errGroup(ctx durable.Context) error {
	var g errgroup.Group
	g.Go(func() error {
		_, err := durable.Step(ctx, "a", work) // want `durable.Step runs on a goroutine started by g.Go`
		return err
	})
	g.TryGo(func() error {
		return durable.Wait(ctx, "b", time.Second) // want `durable.Wait runs on a goroutine started by g.TryGo`
	})

	eg, _ := errgroup.WithContext(ctx)
	eg.Go(func() error {
		_, err := durable.Invoke[string](eg2ctx(ctx), "c", "fn", 1) // want `durable.Invoke runs on a goroutine started by eg.Go`
		return err
	})
	return g.Wait()
}

func eg2ctx(ctx durable.Context) durable.Context { return ctx }

func waitGroup(ctx durable.Context) {
	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = durable.Step(ctx, "a", work) // want `durable.Step runs on a goroutine started by wg.Go`
	})
	wg.Wait()
}

func okCases(ctx durable.Context) error {
	// The SDK's own concurrency primitive.
	fut := durable.Go(ctx, "go", func(child durable.Context) (string, error) {
		return durable.Step(child, "s", work)
	})
	_, _ = fut.Result()

	// Async variants run their body on an SDK goroutine.
	f2 := durable.StepAsync(ctx, "async", work)
	_, _ = f2.Result()

	// A goroutine that does no durable work is fine.
	var g errgroup.Group
	g.Go(func() error {
		time.Sleep(time.Millisecond)
		return nil
	})

	// A goroutine inside a step body does plain work.
	_, _ = durable.Step(ctx, "step", func(sc durable.StepContext) (string, error) {
		var wg sync.WaitGroup
		wg.Go(func() { time.Sleep(time.Millisecond) })
		wg.Wait()
		return "", nil
	})

	// Parallel branches and Map items receive their own Context.
	_, _ = durable.Parallel(ctx, "par", []durable.Branch[string]{
		{Name: "x", Func: func(c durable.Context) (string, error) {
			return durable.Step(c, "s", work)
		}},
	})
	return g.Wait()
}
