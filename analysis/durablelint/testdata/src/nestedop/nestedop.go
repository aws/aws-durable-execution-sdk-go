package nestedop

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func work(durable.StepContext) (string, error) { return "", nil }

func inStep(ctx durable.Context) {
	_, _ = durable.Step(ctx, "outer", func(sc durable.StepContext) (string, error) {
		if err := durable.Wait(ctx, "w", time.Second); err != nil { // want `durable.Wait is called inside a step body`
			return "", err
		}
		return durable.Step(ctx, "inner", work) // want `durable.Step is called inside a step body`
	})

	f := durable.StepAsync(ctx, "async", func(sc durable.StepContext) (string, error) {
		return durable.Invoke[string](ctx, "inv", "fn", 1) // want `durable.Invoke is called inside a step body`
	})
	_, _ = f.Result()

	_, _ = durable.WaitForCondition(ctx, "cond", func(sc durable.StepContext, s int) (int, error) {
		_, _ = durable.RunInChildContext(ctx, "child", func(c durable.Context) (int, error) { // want `durable.RunInChildContext is called inside a step body`
			return durable.Step(c, "s", func(durable.StepContext) (int, error) { return 0, nil })
		})
		return s, nil
	}, durable.ConditionConfig[int]{})

	_, _ = durable.WaitForCallback[string](ctx, "cb", func(sc durable.StepContext, id string) error {
		_, err := durable.Step(ctx, "notify", work) // want `durable.Step is called inside a step body`
		return err
	})

	// A helper closure defined inside the step body is still inside it.
	_, _ = durable.Step(ctx, "closure", func(sc durable.StepContext) (string, error) {
		send := func() error { return durable.Wait(ctx, "w", time.Second) } // want `durable.Wait is called inside a step body`
		return "", send()
	})
}

func okCases(ctx durable.Context) {
	// Grouping operations in a child context is the supported pattern.
	_, _ = durable.RunInChildContext(ctx, "group", func(c durable.Context) (string, error) {
		if err := durable.Wait(c, "w", time.Second); err != nil {
			return "", err
		}
		return durable.Step(c, "s", work)
	})

	// Plain work in a step body.
	_, _ = durable.Step(ctx, "plain", func(sc durable.StepContext) (string, error) {
		time.Sleep(time.Millisecond)
		return sc.Value("k").(string), nil
	})

	// The step body argument is a named function, which the rule does not
	// follow.
	_, _ = durable.Step(ctx, "named", work)
}
