// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring this repo's other
// examples' handler.go/handler_test.go split.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

// FlakyRequestEvent is this example's input shape.
type FlakyRequestEvent struct {
	RequestID string `json:"requestId"`
	// PresetFailUntilAttempt and CustomFailUntilAttempt independently
	// control each of the two simulated flaky-dependency calls this
	// handler makes (see handler's doc): callFlakyDependency returns an
	// error for every attempt strictly less than the given value, and
	// succeeds from that attempt onward. Separate fields (rather than a
	// single shared one) exist because the two steps' underlying
	// (simulated) dependencies are independent services in this example,
	// and because it lets tests exercise one step's retry path while
	// keeping the other's deterministic and uninvolved - e.g. proving
	// genuine suspend/resume on JUST the custom-strategy step (see
	// suspend_resume_test.go) without also depending on the
	// preset-strategy step's own jittered timing.
	PresetFailUntilAttempt int `json:"presetFailUntilAttempt"`
	CustomFailUntilAttempt int `json:"customFailUntilAttempt"`
}

// FlakyRequestResult is this example's output shape.
type FlakyRequestResult struct {
	RequestID string `json:"requestId"`
	Data      string `json:"data"`
}

// errFlakyDependency is returned by callFlakyDependency to simulate a
// transient failure from an external service (a network blip, a 503, a
// throttling response, etc.) - the class of error operations.Step's
// retry strategies exist to paper over automatically.
var errFlakyDependency = errors.New("flaky dependency: simulated transient failure")

// callFlakyDependency simulates calling an unreliable external service:
// it fails on every attempt before failUntilAttempt and succeeds from
// that attempt onward. In production this would be a real HTTP/AWS SDK
// call made through sc.Context() (for cancellation/deadlines); this
// example simulates it directly so the example is self-contained and its
// test doesn't depend on a real flaky service to reproduce specific
// retry counts deterministically.
func callFlakyDependency(sc types.StepContext, requestID string, failUntilAttempt int) (string, error) {
	attempt := sc.Attempt()
	sc.Logger().Info("calling flaky dependency", map[string]any{"attempt": attempt})
	if attempt < failUntilAttempt {
		return "", errFlakyDependency
	}
	return fmt.Sprintf("data-for-%s", requestID), nil
}

// handler demonstrates operations.Step's retry strategies
// (docs/remaining-work.md §2 task 7, and the retry-delay suspension that
// closed the gap in that same task) in a realistic scenario: a step that
// calls a flaky external dependency and retries with a configured
// backoff strategy.
//
// Two steps, each with a DIFFERENT retry strategy, run in sequence to
// demonstrate both ends of the retry-strategy API surface
// (utils/retry.go):
//
//  1. "call-flaky-dependency-preset" uses utils.Presets.ExponentialBackoff
//     (maxAttempts=3, initialDelay=5s, maxDelay=5m, backoffRate=2, full
//     jitter) - the ready-to-use preset most callers reach for first.
//  2. "call-flaky-dependency-custom" uses WithStepRetryStrategy with a
//     hand-built strategy from utils.CreateRetryStrategy, demonstrating
//     the fully-parameterized API for callers who need control over
//     jitter/backoff rate/caps the presets don't expose. This step is
//     configured to retry with a FIXED, deliberately short (but
//     non-zero) delay via utils.Presets.FixedDelay so this example
//     genuinely exercises the retry-delay SUSPEND/RESUME mechanism (see
//     step.go's retryOrFail doc): a non-zero delay suspends the whole
//     invocation rather than re-executing immediately in-process,
//     exactly like Wait suspends for its duration. A LocalTestRunner
//     test resolves this invisibly via SkipTime (see handler_test.go's
//     TestHandler_*); a lower-level test using the fakeClient pattern
//     (retry_delay_suspend_test.go, mirroring
//     pkg/durable/durable_retry_delay_suspend_test.go) drives the
//     genuine suspend-then-resume across two real invocations directly,
//     without SkipTime hiding it.
func handler(event FlakyRequestEvent, dc types.DurableContext) (FlakyRequestResult, error) {
	dc.Logger().Info("handler started", map[string]any{
		"requestId":              event.RequestID,
		"presetFailUntilAttempt": event.PresetFailUntilAttempt,
		"customFailUntilAttempt": event.CustomFailUntilAttempt,
	})

	presetResult, err := operations.Step(dc, "call-flaky-dependency-preset",
		func(sc types.StepContext) (string, error) {
			return callFlakyDependency(sc, event.RequestID, event.PresetFailUntilAttempt)
		},
		operations.WithStepRetryStrategy[string](utils.Presets.ExponentialBackoff()),
	)
	if err != nil {
		return FlakyRequestResult{}, fmt.Errorf("request %s: calling flaky dependency (preset strategy): %w", event.RequestID, err)
	}

	// A second call to a DIFFERENT simulated flaky dependency, this time
	// behind a custom retry strategy built via utils.CreateRetryStrategy
	// - a fixed 3-attempt cap with a small, constant, NON-ZERO delay
	// (30s) between attempts, deliberately using utils.Presets.FixedDelay
	// rather than exponential backoff so the delay is predictable
	// (unjittered) for this example's own lower-level suspend/resume
	// test (see suspend_resume_test.go).
	customStrategy := utils.Presets.FixedDelay(types.Duration{Seconds: 30}, 3)
	customResult, err := operations.Step(dc, "call-flaky-dependency-custom",
		func(sc types.StepContext) (string, error) {
			return callFlakyDependency(sc, event.RequestID, event.CustomFailUntilAttempt)
		},
		operations.WithStepRetryStrategy[string](customStrategy),
	)
	if err != nil {
		return FlakyRequestResult{}, fmt.Errorf("request %s: calling flaky dependency (custom strategy): %w", event.RequestID, err)
	}

	dc.Logger().Info("handler completed", map[string]any{"presetResult": presetResult, "customResult": customResult})
	return FlakyRequestResult{
		RequestID: event.RequestID,
		Data:      customResult,
	}, nil
}
