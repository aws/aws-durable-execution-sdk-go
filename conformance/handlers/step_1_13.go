// Requirement 1-13: Default retry strategy.
//
// From test-requirements/step/1-13.yaml:
//
//	description: Default retry strategy
//	handler: |
//	  A step that fails on the first two attempts and succeeds on the
//	  third, using the SDK's default retry strategy (no explicit config).
//	  Default: 6 max attempts, 5s initial delay, 60s max delay, 2x
//	  backoff rate, FULL jitter.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	DefaultRetryParameters:
//	  MaxAttempts: 6
//	  InitialDelay: 5s
//	  MaxDelay: 60s
//	  BackoffRate: 2
//	  Jitter: FULL
//	ExpectedExecutionHistory:
//	  ... attempt 1: NextAttemptDelaySeconds: ${/^[0-5]$/}   (5s * jitter[0,1))
//	  ... attempt 2: NextAttemptDelaySeconds: ${/^([0-9]|10)$/} (10s * jitter[0,1))
//	  ... attempt 3: succeeds
//
// # Update: FIXED - this requirement is now genuinely satisfiable
//
// Previously undeliverable: "no explicit config" (operations.Step called
// with NO WithStepRetryStrategy option at all) fell back to
// utils.Presets.NoRetry() (never retry) - genuinely different from this
// requirement's documented cross-SDK default retry policy, with no
// currently-exported API to change that built-in default without
// passing WithStepRetryStrategy explicitly (which would no longer be "no
// explicit config").
//
// That conclusion was re-examined per explicit user request ("is this
// against any Go design pattern? why did we do it this way?") and found
// to be a real, fixable divergence, not a deliberate design choice: the
// JS reference SDK's own step-handler.ts falls back to retryPresets.
// default (6 attempts, 5s initial delay, 60s max delay, 2x backoff, full
// jitter - confirmed by reading retry-presets.ts directly) at BOTH of
// its own retry call sites whenever no retryStrategy option is supplied
// - not retryPresets.noRetry. This Go SDK's own zero-option fallback was
// simply the wrong preset, ported without cross-checking against the
// reference SDK's actual runtime behavior. Fixed in
// pkg/durable/operations/step.go (cfg.retryStrategy now defaults to the
// new utils.Presets.Default(), added to pkg/durable/utils/retry.go
// specifically to match retryPresets.default's exact values) - see both
// of those files' own updated doc comments for the full fix.
//
// This handler exercises that new default directly: no
// WithStepRetryStrategy option at all, relying entirely on Step's own
// zero-option fallback. sc.Attempt() (the SDK's own checkpointed,
// replay-safe attempt counter - see step_1_11.go's identical pattern
// one level up, with an explicit strategy) drives the fail-twice-then-
// succeed behavior the YAML's own scenario describes.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("1-13", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_13Handler, config(client))
	})
}

func step1_13Handler(event any, dc types.DurableContext) (string, error) {
	// Deliberately NO operations.WithStepRetryStrategy option - this is
	// the entire point of the requirement: exercising Step's own
	// zero-option default (utils.Presets.Default(), per step.go's own
	// fallback) rather than any caller-supplied strategy.
	return operations.Step(dc, "unreliable_func", func(sc types.StepContext) (string, error) {
		if sc.Attempt() < 3 {
			return "", errors.New("intentional transient failure for conformance requirement 1-13")
		}
		return "Operation succeeded", nil
	})
}
