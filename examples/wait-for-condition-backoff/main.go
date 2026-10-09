// Command wait-for-condition-backoff demonstrates a [durable.WaitForCondition]
// polled on an exponential backoff built from a [durable.WaitConfig]. The
// delay before check n+1 is
//
//	min(InitialDelay × BackoffRate^(n-1), MaxDelay)
//
// With the configuration below (1 s initial delay, rate 2, 10 s cap) the
// delays are 1 s, 2 s, 4 s, 8 s. The handler polls an export job that
// reports ready on its fourth check, so the execution records the delays
// 1 s, 2 s, 4 s. ShouldContinue keeps polling while the job is not ready;
// when it reports the job ready the operation succeeds with that state.
//
// The example shows both constructors, as retry-linear does for retry
// strategies. The default strategy is a hard-coded configuration, so it is
// built once at package initialization with [durable.MustNewWaitStrategy],
// which panics on an invalid value at startup. An input that overrides
// MaxAttempts builds its strategy at run time with
// [durable.NewWaitStrategy], which returns the validation error instead,
// and the handler returns that error.
//
// An input whose MaxAttempts is reached before the job is ready fails the
// operation with a [*durable.WaitForConditionError], and the execution
// fails.
//
// [durable.JitterNone] keeps the delays exact so the sequence is easy to
// verify. The zero Jitter selects [durable.JitterFull], which suits many
// executions polling the same service.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input selects the scenario. Both fields are optional. The zero value
// runs the default scenario, which succeeds on the fourth check.
type Input struct {
	// ReadyOnCheck is the 1-based check on which the job reports ready.
	// Default 4.
	ReadyOnCheck int `json:"readyOnCheck"`

	// MaxAttempts caps the number of checks. Zero selects the default
	// strategy, which allows 5. Any other value builds a strategy with
	// that cap at run time.
	MaxAttempts int `json:"maxAttempts"`
}

// Job is the polled state: how many checks have run and whether the last
// one found the job ready. It is checkpointed after every check and passed
// to the next.
type Job struct {
	Checks int  `json:"checks"`
	Ready  bool `json:"ready"`
}

// pollConfig returns the backoff configuration with the given cap on the
// number of checks.
func pollConfig(maxAttempts int) durable.WaitConfig[Job] {
	return durable.WaitConfig[Job]{
		MaxAttempts:  maxAttempts,
		InitialDelay: 1 * time.Second,
		BackoffRate:  2,
		MaxDelay:     10 * time.Second,
		Jitter:       durable.JitterNone,
		// Keep polling until a check finds the job ready.
		ShouldContinue: func(j Job) bool { return !j.Ready },
	}
}

// defaultStrategy is the hard-coded strategy: at most 5 checks, delays
// 1 s, 2 s, 4 s, 8 s. Its values are fixed at compile time, so an invalid
// one is a programming error. MustNewWaitStrategy panics on it at startup,
// before any invocation.
var defaultStrategy = durable.MustNewWaitStrategy(pollConfig(5))

// strategyFor selects the wait strategy for the input. Without a
// MaxAttempts override it returns defaultStrategy. With one it builds a
// strategy from the input; the cap comes from the event, so an invalid
// value is a request error rather than a programming error, and
// NewWaitStrategy reports it as an error for the handler to return.
func strategyFor(in Input) (durable.WaitStrategy[Job], error) {
	if in.MaxAttempts == 0 {
		return defaultStrategy, nil
	}
	return durable.NewWaitStrategy(pollConfig(in.MaxAttempts))
}

func handler(ctx durable.Context, in Input) (Job, error) {
	readyOnCheck := in.ReadyOnCheck
	if readyOnCheck == 0 {
		readyOnCheck = 4
	}

	strategy, err := strategyFor(in)
	if err != nil {
		return Job{}, err
	}

	return durable.WaitForCondition(ctx, "export-ready",
		func(_ durable.StepContext, j Job) (Job, error) {
			// Simulate a job that finishes after a fixed number of checks.
			j.Checks++
			j.Ready = j.Checks >= readyOnCheck
			return j, nil
		},
		durable.ConditionConfig[Job]{WaitStrategy: strategy},
	)
}

func main() { durable.Start(handler) }
