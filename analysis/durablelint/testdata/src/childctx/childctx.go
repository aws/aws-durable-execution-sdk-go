package childctx

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func work(durable.StepContext) (string, error) { return "", nil }

func parentInChild(ctx durable.Context) {
	_, _ = durable.RunInChildContext(ctx, "child", func(c durable.Context) (string, error) {
		_ = durable.Wait(ctx, "w", time.Second) // want `durable.Wait uses ctx from an enclosing scope inside a function that receives its own Context; use c`
		return durable.Step(c, "s", work)
	})

	fut := durable.Go(ctx, "go", func(child durable.Context) (string, error) {
		return durable.Step(ctx, "s", work) // want `durable.Step uses ctx from an enclosing scope inside a function that receives its own Context; use child`
	})
	_, _ = fut.Result()

	_, _ = durable.Map(ctx, "map", []int{1, 2}, func(c durable.Context, item int, index int) (string, error) {
		return durable.Step(ctx, "s", work) // want `durable.Step uses ctx from an enclosing scope inside a function that receives its own Context; use c`
	})

	_, _ = durable.Parallel(ctx, "par", []durable.Branch[string]{
		{Name: "b", Func: func(c durable.Context) (string, error) {
			return durable.Step(ctx, "s", work) // want `durable.Step uses ctx from an enclosing scope inside a function that receives its own Context; use c`
		}},
	})

	_, _, _ = durable.Select(ctx, "sel", []durable.Branch[string]{
		{Name: "b", Func: func(c durable.Context) (string, error) {
			return durable.Step(ctx, "s", work) // want `durable.Step uses ctx from an enclosing scope`
		}},
	})

	_, _ = durable.Retry(ctx, "retry", func(c durable.Context, attempt int) (string, error) {
		return durable.Step(ctx, "s", work) // want `durable.Step uses ctx from an enclosing scope`
	}, nil)

	// Nested children: the inner function must use its own Context, not the
	// outer child's.
	_, _ = durable.RunInChildContext(ctx, "outer", func(outer durable.Context) (string, error) {
		return durable.RunInChildContext(outer, "inner", func(inner durable.Context) (string, error) {
			return durable.Step(outer, "s", work) // want `durable.Step uses outer from an enclosing scope inside a function that receives its own Context; use inner`
		})
	})

	// A blank parameter gives the child no name to use.
	_, _ = durable.RunInChildContext(ctx, "blank", func(_ durable.Context) (string, error) {
		return durable.Step(ctx, "s", work) // want `durable.Step uses ctx from an enclosing scope inside a function that receives its own Context; name that parameter and use it`
	})

	// A helper closure without a Context parameter inherits the check from
	// the enclosing child function.
	_, _ = durable.RunInChildContext(ctx, "closure", func(c durable.Context) (string, error) {
		run := func() (string, error) { return durable.Step(ctx, "s", work) } // want `durable.Step uses ctx from an enclosing scope`
		return run()
	})

	// So does a helper closure that has a Context parameter of its own: the
	// check is against the child callback, not the helper.
	_, _ = durable.RunInChildContext(ctx, "helper", func(c durable.Context) (string, error) {
		run := func(h durable.Context) (string, error) { return durable.Step(ctx, "s", work) } // want `durable.Step uses ctx from an enclosing scope inside a function that receives its own Context; use c`
		return run(c)
	})
}

// A closure that is not passed to an SDK function is not a child-context
// callback. Using the handler's Context inside it is correct, whatever
// parameters it declares.
func helperClosures(ctx durable.Context) {
	helper := func(c durable.Context) error { return durable.Wait(ctx, "w", time.Second) }
	_ = helper(ctx)

	run := func(durable.Context, string) (string, error) { return durable.Step(ctx, "s", work) }
	_, _ = run(ctx, "x")

	_, _ = func(c durable.Context) (string, error) { return durable.Step(ctx, "s", work) }(ctx)

	// A helper that uses its own parameter, called with the child inside a
	// child callback, is correct as well.
	_, _ = durable.RunInChildContext(ctx, "child", func(c durable.Context) (string, error) {
		h := func(x durable.Context) (string, error) { return durable.Step(x, "s", work) }
		return h(c)
	})
}

func okCases(ctx durable.Context) {
	_, _ = durable.RunInChildContext(ctx, "child", func(c durable.Context) (string, error) {
		if err := durable.Wait(c, "w", time.Second); err != nil {
			return "", err
		}
		// A local alias is assumed to be the child.
		cc := c
		_, _ = durable.Step(cc, "alias", work)
		// Nested child using its own Context.
		return durable.RunInChildContext(c, "inner", func(inner durable.Context) (string, error) {
			return durable.Step(inner, "s", work)
		})
	})

	fut := durable.Go(ctx, "go", func(child durable.Context) (string, error) {
		return durable.Step(child, "s", work)
	})
	_, _ = fut.Result()

	// Operations on the handler's own Context.
	_, _ = durable.Step(ctx, "top", work)

	// A step body captures the parent Context legitimately for non-durable
	// use, and durable calls in it belong to the nested-operation rule.
	_, _ = durable.Step(ctx, "step", func(sc durable.StepContext) (string, error) {
		_ = ctx.ExecutionArn()
		return "", nil
	})
}
