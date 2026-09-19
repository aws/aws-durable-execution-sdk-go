// Command retry-linear demonstrates a step retried with the linear backoff
// strategy. The delay before retry n is
//
//	min(InitialDelay + Increment × (n-1), MaxDelay)
//
// With the configuration below (1 s initial delay, 1 s increment, 10 s cap)
// the delays are 1 s, 2 s, 3 s, 4 s. The third delay is what tells linear
// backoff apart from exponential backoff: an exponential strategy starting
// from the same 1 s initial delay waits 1 s, 2 s, 4 s. The step fails its
// first three attempts and succeeds on the fourth, so the execution records
// exactly those three distinguishing delays.
//
// The example shows both constructors. The default strategy is a hard-coded
// configuration, so it is built once at package initialization with
// [durable.MustLinearBackoff], which panics on an invalid value at startup.
// An input that overrides MaxDelay builds its strategy at run time with
// [durable.LinearBackoff], which returns the validation error instead, and
// the handler returns that error.
//
// An input above MaxAttempts exhausts the retries; the error from the last
// attempt then propagates and the execution fails. Lowering maxDelaySeconds
// in the same input shows MaxDelay capping the sequence.
//
// [durable.JitterNone] keeps the delays exact so the sequence is easy to
// verify. Production code usually prefers [durable.JitterFull] so that
// many executions retrying at once do not all retry at the same instant.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input selects the scenario. Both fields are optional. The zero value
// runs the default scenario, which succeeds on the fourth attempt.
type Input struct {
	// SucceedOnAttempt is the 1-based attempt that succeeds. Default 4.
	// A value above the strategy's 5 attempts exhausts the retries.
	SucceedOnAttempt int `json:"succeedOnAttempt"`

	// MaxDelaySeconds caps each delay. Zero selects the default strategy,
	// whose 10 s cap leaves the default scenario's delays unclamped. Any
	// other value builds a strategy with that cap at run time.
	MaxDelaySeconds int `json:"maxDelaySeconds"`
}

// Output reports which attempt succeeded. The attempt number is returned
// from inside the step, so it is checkpointed with the result and read
// back unchanged on replay.
type Output struct {
	Message  string `json:"message"`
	Attempts int    `json:"attempts"`
}

// maxAttempts bounds the strategy: one initial attempt plus four retries,
// so at most four delays of 1 s, 2 s, 3 s, 4 s before the cap applies.
const maxAttempts = 5

// defaultStrategy is the hard-coded strategy: delays 1 s, 2 s, 3 s, 4 s.
// Its values are fixed at compile time, so an invalid one is a programming
// error. MustLinearBackoff panics on it at startup, before any invocation.
var defaultStrategy = durable.MustLinearBackoff(durable.LinearRetryConfig{
	MaxAttempts:  maxAttempts,
	InitialDelay: 1 * time.Second,
	Increment:    1 * time.Second,
	MaxDelay:     10 * time.Second,
	Jitter:       durable.JitterNone,
})

// strategyFor selects the retry strategy for the input. Without a MaxDelay
// override it returns defaultStrategy. With one it builds a strategy from
// the input; the cap comes from the event, so an invalid value is a request
// error rather than a programming error, and LinearBackoff reports it as an
// error for the handler to return.
func strategyFor(in Input) (durable.RetryStrategy, error) {
	if in.MaxDelaySeconds == 0 {
		return defaultStrategy, nil
	}
	return durable.LinearBackoff(durable.LinearRetryConfig{
		MaxAttempts:  maxAttempts,
		InitialDelay: 1 * time.Second,
		Increment:    1 * time.Second,
		MaxDelay:     time.Duration(in.MaxDelaySeconds) * time.Second,
		Jitter:       durable.JitterNone,
	})
}

func handler(ctx durable.Context, in Input) (Output, error) {
	succeedOnAttempt := in.SucceedOnAttempt
	if succeedOnAttempt == 0 {
		succeedOnAttempt = 4
	}

	strategy, err := strategyFor(in)
	if err != nil {
		return Output{}, err
	}

	return durable.Step(ctx, "flaky-upstream",
		func(sc durable.StepContext) (Output, error) {
			// Simulate an upstream that is unavailable until it warms up.
			// Attempt is 1-based.
			if sc.Attempt() < succeedOnAttempt {
				return Output{}, fmt.Errorf("upstream temporarily unavailable (attempt %d)", sc.Attempt())
			}
			return Output{
				Message:  fmt.Sprintf("request confirmed on attempt %d", sc.Attempt()),
				Attempts: sc.Attempt(),
			}, nil
		},
		durable.WithRetry(strategy),
	)
}

func main() { durable.Start(handler) }
