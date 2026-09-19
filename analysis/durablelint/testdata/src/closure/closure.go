package closure

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func work(durable.StepContext) (string, error) { return "", nil }

var calls int

func writesInStep(ctx durable.Context, event map[string]int) {
	counter := 0
	var result string
	var results []string
	total := 0

	_, _ = durable.Step(ctx, "assign", func(sc durable.StepContext) (string, error) {
		counter = 5 // want `counter is declared outside this step body and is written inside it; the write is skipped on replay, return the value instead`
		return "", nil
	})

	_, _ = durable.Step(ctx, "incdec", func(sc durable.StepContext) (string, error) {
		counter++ // want `counter is declared outside this step body`
		counter-- // want `counter is declared outside this step body`
		return "", nil
	})

	_, _ = durable.Step(ctx, "compound", func(sc durable.StepContext) (string, error) {
		total += 10 // want `total is declared outside this step body`
		return "", nil
	})

	_, _ = durable.Step(ctx, "append", func(sc durable.StepContext) (string, error) {
		results = append(results, "x") // want `results is declared outside this step body`
		return "", nil
	})

	_, _ = durable.Step(ctx, "multi", func(sc durable.StepContext) (string, error) {
		var err error
		result, err = work(sc) // want `result is declared outside this step body`
		return result, err
	})

	_, _ = durable.Step(ctx, "package-level", func(sc durable.StepContext) (string, error) {
		calls++ // want `calls is declared outside this step body`
		return "", nil
	})

	_, _ = durable.Step(ctx, "range-assign", func(sc durable.StepContext) (string, error) {
		var k string
		for k = range event { // k is local: not reported
			_ = k
		}
		for result = range event { // want `result is declared outside this step body`
		}
		return "", nil
	})

	// A helper closure inside the step body is checked against the step.
	_, _ = durable.Step(ctx, "helper", func(sc durable.StepContext) (string, error) {
		bump := func() { counter++ } // want `counter is declared outside this step body`
		bump()
		defer func() { total = 0 }() // want `total is declared outside this step body`
		return "", nil
	})

	_ = durable.StepAsync(ctx, "async", func(sc durable.StepContext) (string, error) {
		counter = 1 // want `counter is declared outside this step body`
		return "", nil
	})

	_, _ = durable.WaitForCondition(ctx, "cond", func(sc durable.StepContext, s int) (int, error) {
		counter = s // want `counter is declared outside this step body`
		return s, nil
	}, durable.ConditionConfig[int]{})

	_, _ = durable.WaitForCallback[string](ctx, "cb", func(sc durable.StepContext, id string) error {
		result = id // want `result is declared outside this step body`
		return nil
	})
}

func writesInChild(ctx durable.Context) {
	counter := 0
	var result string

	_, _ = durable.RunInChildContext(ctx, "child", func(c durable.Context) (string, error) {
		counter++ // want `counter is declared outside this child context function`
		return durable.Step(c, "s", work)
	})

	fut := durable.Go(ctx, "go", func(c durable.Context) (string, error) {
		result = "x" // want `result is declared outside this child context function`
		return result, nil
	})
	_, _ = fut.Result()

	_, _ = durable.Map(ctx, "map", []int{1, 2}, func(c durable.Context, item int, index int) (string, error) {
		counter += item // want `counter is declared outside this child context function`
		return "", nil
	})

	_, _ = durable.Parallel(ctx, "par", []durable.Branch[string]{
		{Name: "b", Func: func(c durable.Context) (string, error) {
			counter = 2 // want `counter is declared outside this child context function`
			return "", nil
		}},
	})

	_, _ = durable.Retry(ctx, "retry", func(c durable.Context, attempt int) (string, error) {
		counter = attempt // want `counter is declared outside this child context function`
		return "", nil
	}, nil)

	// A step inside a child context is checked against the step: the
	// child's own local is captured by the step.
	_, _ = durable.RunInChildContext(ctx, "nested", func(c durable.Context) (string, error) {
		seen := 0
		_, _ = durable.Step(c, "s", func(sc durable.StepContext) (string, error) {
			seen++ // want `seen is declared outside this step body`
			return "", nil
		})
		return "", nil
	})
}

func okCases(ctx durable.Context, event map[string]int) {
	counter := 0
	var results []string

	// Reading a captured variable is allowed.
	_, _ = durable.Step(ctx, "read", func(sc durable.StepContext) (string, error) {
		return results[counter], nil
	})

	// Variables declared inside the function are not captured.
	_, _ = durable.Step(ctx, "local", func(sc durable.StepContext) (string, error) {
		n := 0
		n++
		n += 2
		var list []string
		list = append(list, "x")
		for k := range event {
			list = append(list, k)
		}
		return list[n], nil
	})

	// Parameters and named results belong to the function.
	_, _ = durable.Step(ctx, "params", func(sc durable.StepContext) (out string, err error) {
		sc = nil
		out = "x"
		return out, nil
	})

	// The blank identifier is not a variable.
	_, _ = durable.Step(ctx, "blank", func(sc durable.StepContext) (string, error) {
		_ = counter
		_, _ = work(sc)
		return "", nil
	})

	// Assigning the checkpointed result outside the function is the
	// replay-safe pattern.
	counter, _ = durable.Step(ctx, "outside", func(sc durable.StepContext) (int, error) {
		return counter + 1, nil
	})
	results = append(results, "done")

	// Orchestration code between operations may write freely, and so may a
	// goroutine or helper that is not a checkpointed function.
	_, _ = durable.RunInChildContext(ctx, "child", func(c durable.Context) (string, error) {
		n := 0
		if err := durable.Wait(c, "w", time.Second); err != nil {
			return "", err
		}
		n++
		return "", nil
	})
	helper := func() { counter++ }
	helper()
	go func() { counter++ }()
}
