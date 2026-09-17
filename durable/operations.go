package durable

import "time"

// WaitDecision is a condition wait strategy's verdict after a check.
//
// A wait strategy returns one of three outcomes:
//   - Continue: keep polling (Continue true, Delay set).
//   - Stop: the condition is met, return the state (Continue false, Err nil).
//   - Fail: an operational limit (such as max attempts) was exceeded
//     (Continue false, Err non-nil). The error is checkpointed and
//     returned as a [*WaitForConditionError].
type WaitDecision struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// Continue indicates whether to keep waiting and check again.
	Continue bool

	// Delay is how long to suspend before the next check. It is ignored
	// when Continue is false.
	Delay time.Duration

	// Err, when non-nil and Continue is false, signals that the
	// operation should fail with this error rather than succeed. Use it
	// to implement max-attempts or timeout strategies.
	Err error
}

// ConditionConfig configures a [WaitForCondition] operation.
//
// Unlike other operations, which take variadic Option arguments,
// WaitForCondition takes this struct positionally: two of its fields
// (InitialState and WaitStrategy) depend on the state type S, and Go's
// non-generic option interfaces cannot carry a type parameter.
type ConditionConfig[S any] struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// InitialState is the state passed to the first check.
	InitialState S

	// WaitStrategy decides, after each check, whether to keep waiting and
	// for how long. attempt is the 1-based number of completed checks.
	// The strategy must be a deterministic function of its arguments,
	// except for randomized jitter in the returned delay.
	//
	// If nil, a default strategy is used: keep polling with exponential
	// backoff — a 5 second initial delay multiplied by 1.5 after each
	// attempt, capped at 300 seconds, with full jitter — and fail the
	// operation once 60 attempts have been made.
	WaitStrategy func(state S, attempt int) WaitDecision

	// Serdes overrides the serializer for the condition state.
	Serdes Serdes
}
