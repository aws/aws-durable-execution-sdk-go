package nondeterminism

import (
	crand "crypto/rand"
	"math/rand"
	randv2 "math/rand/v2"
	"sort"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/google/uuid"
)

func work(durable.StepContext) (string, error) { return "", nil }

func handler(ctx durable.Context, event map[string]int) (string, error) {
	now := time.Now()                  // want `time.Now is nondeterministic and is called outside a step body`
	_ = time.Since(now)                // want `time.Since is nondeterministic`
	_ = time.Until(now)                // want `time.Until is nondeterministic`
	_ = rand.Intn(10)                  // want `rand.Intn is nondeterministic`
	_ = randv2.IntN(10)                // want `randv2.IntN is nondeterministic`
	_ = randv2.N[int](10)              // want `randv2.N is nondeterministic`
	_ = (randv2.N[int64])(10)          // want `randv2.N is nondeterministic`
	_, _ = crand.Read(make([]byte, 4)) // want `crand.Read is nondeterministic`
	_ = uuid.NewString()               // want `uuid.NewString is nondeterministic`
	_ = ctx.RequestID()                // want `Context.RequestID changes on every invocation`

	for k := range event { // want `range over a map creates durable operation durable.Step in a randomized order`
		_, _ = durable.Step(ctx, k, work)
	}

	_, _ = durable.RunInChildContext(ctx, "child", func(c durable.Context) (string, error) {
		_ = time.Now() // want `time.Now is nondeterministic`
		return durable.Step(c, "s", work)
	})

	_, _ = durable.Parallel(ctx, "par", []durable.Branch[string]{
		{Name: "b", Func: func(c durable.Context) (string, error) {
			_ = rand.Float64() // want `rand.Float64 is nondeterministic`
			return "", nil
		}},
	})

	return "", nil
}

func helperWithContext(ctx durable.Context) time.Time {
	return time.Now() // want `time.Now is nondeterministic`
}

// A blank or unnamed Context parameter still marks orchestration code.
func blankContext(_ durable.Context) time.Time {
	return time.Now() // want `time.Now is nondeterministic`
}

func unnamedContext(durable.Context, string) time.Time {
	return time.Now() // want `time.Now is nondeterministic`
}

func blankContextLiteral(ctx durable.Context) {
	_, _ = durable.RunInChildContext(ctx, "child", func(_ durable.Context) (string, error) {
		_ = time.Now() // want `time.Now is nondeterministic`
		return "", nil
	})
}

// A helper with a Context parameter that is declared inside a step body is
// checkpointed with the step, so it is not orchestration code.
func helperInsideStep(ctx durable.Context) {
	_, _ = durable.Step(ctx, "s", func(sc durable.StepContext) (string, error) {
		stamp := func(c durable.Context) time.Time {
			return time.Now()
		}
		return stamp(ctx).String(), nil
	})
}

func okCases(ctx durable.Context, event map[string]int) {
	// Inside a step body nondeterminism is checkpointed.
	_, _ = durable.Step(ctx, "now", func(sc durable.StepContext) (string, error) {
		_ = time.Now()
		_ = rand.Intn(10)
		_ = ctx.RequestID()
		return uuid.NewString(), nil
	})
	_, _ = durable.WaitForCondition(ctx, "cond", func(sc durable.StepContext, s int) (int, error) {
		return int(time.Now().Unix()), nil
	}, durable.ConditionConfig[int]{})
	_, _ = durable.WaitForCallback[string](ctx, "cb", func(sc durable.StepContext, id string) error {
		_ = time.Now()
		return nil
	})

	// Deterministic alternatives.
	_ = durable.ExecutionStartTime(ctx)
	_ = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = rand.New(rand.NewSource(42))
	_ = randv2.NewPCG(1, 2)

	// Sorted keys, and a map loop that creates no operations.
	keys := make([]string, 0, len(event))
	for k := range event {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = durable.Step(ctx, k, work)
	}
	for k := range event {
		_ = k
	}
	// Ranging over a slice or channel is deterministic.
	for _, k := range keys {
		_, _ = durable.Step(ctx, k, work)
	}
}

// A map range inside a step body belongs to the nested-operation rule, not
// to this one.
func mapRangeInStep(ctx durable.Context, event map[string]int) {
	_, _ = durable.Step(ctx, "loop", func(sc durable.StepContext) (string, error) {
		for k := range event {
			_, _ = durable.Step(ctx, k, work)
		}
		return "", nil
	})
}

// A function without a Context parameter is not orchestration code.
func plainHelper() time.Time { return time.Now() }

type holder struct{ ctx durable.Context }

// A map range in a function without a Context parameter is not analysed,
// even when its body creates durable operations.
func mapRangeWithoutContext(h holder, event map[string]int) {
	for k := range event {
		_, _ = durable.Step(h.ctx, k, work)
	}
}

type Service struct{}

func (Service) Timestamp() time.Time { return time.Now() }
