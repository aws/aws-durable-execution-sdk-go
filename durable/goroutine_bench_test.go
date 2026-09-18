package durable

import (
	"context"
	"strconv"
	"testing"
)

// BenchmarkClaimOperation compares claimOperation with the goroutine
// ownership check active against the same call with the check disabled.
// The difference between the two variants is the per-operation cost of the
// check. It is the measurement behind the decision to run the check in
// every default build; see goroutineOwner.
//
// The check reads the goroutine identity from runtime.Stack, whose cost
// grows with the depth of the calling stack, so each variant runs at two
// extra stack depths. Real handlers sit a few dozen frames deep.
func BenchmarkClaimOperation(b *testing.B) {
	newCtx := func(owner goroutineOwner) *execContext {
		ec := newExecContext(
			context.Background(),
			"arn:test:bench",
			invocationInfo{},
			nopLogger{},
			newExecutionState([]*operation{execOp()}),
		)
		ec.owner = owner
		return ec
	}

	// b.Run executes each sub-benchmark on its own goroutine, so the owner
	// must be captured inside it.
	variants := []struct {
		name  string
		owner func() goroutineOwner
	}{
		{"checked", currentGoroutineOwner},
		{"unchecked", disabledGoroutineOwner},
	}
	depths := []int{0, 16, 64}

	for _, v := range variants {
		for _, depth := range depths {
			b.Run(v.name+"/depth="+strconv.Itoa(depth), func(b *testing.B) {
				ec := newCtx(v.owner())
				b.ReportAllocs()
				atDepth(depth, func() {
					for b.Loop() {
						if _, err := ec.claimOperation(); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}

// atDepth calls f with n extra frames on the stack.
//
//go:noinline
func atDepth(n int, f func()) {
	if n == 0 {
		f()
		return
	}
	atDepth(n-1, f)
}
