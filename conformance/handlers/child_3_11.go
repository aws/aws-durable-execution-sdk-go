// Requirement 3-11: Child context large payload (ReplayChildren mode).
//
// From test-requirements/child/3-11.yaml:
//
//	description: Child context large payload (ReplayChildren mode)
//	handler: |
//	  A child context where the step returns a small value, but the child
//	  context function builds a large result (>256KB) from it. The child
//	  context return value exceeds the checkpoint size limit, triggering
//	  ReplayChildren mode.
//	  The child context body prints the input string to raw stdout outside
//	  of the step.
//	invocations: |
//	  - Handler invokes a child context with a step that returns a small
//	    value, but the child context body builds a large result exceeding
//	    the checkpoint size limit. ContextStarted, StepStarted,
//	    StepSucceeded (small result), ContextSucceeded (ReplayChildren=true,
//	    empty payload), WaitStarted, invocation completes, execution
//	    suspends.
//	  - Replay 1: Re-invoked because wait completed. SDK re-executes the
//	    child context in ReplaySucceededContext mode to reconstruct the
//	    large result. WaitSucceeded, execution succeeds.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	ExpectedLogs:
//	  # Child context body executes twice: once on first invocation, once
//	  # on replay
//	  - pattern: ${INPUT_1}
//	    count: 2
//
// # This requirement documents a genuine, verified Go SDK gap
//
// This YAML describes a specific, named backend/SDK protocol -
// "ReplayChildren mode": when a child context's own return value exceeds
// the checkpoint payload limit, the SDK is expected to checkpoint
// ContextSucceeded with ContextDetails.ReplayChildren=true and an EMPTY
// Result payload (rather than failing outright), and on the NEXT
// invocation re-execute the child context's fn a second time (a genuine,
// deliberate double-execution - see ExpectedLogs' "count: 2") in a
// "ReplaySucceededContext" mode to reconstruct the large result locally
// from its already-completed, replay-skipped nested step, without
// re-checkpointing it.
//
// types.ContextDetails.ReplayChildren (wire.go) and
// types.ContextOptions.ReplayChildren (wire.go, the OperationUpdate-side
// request field) both genuinely exist on the wire types - confirmed by
// reading wire.go directly - but a grep across the entire
// pkg/durable/operations package for "ReplayChildren" (this task's own
// mandatory verify-before-writing step) returns zero matches anywhere in
// invoke.go's RunInChildContext, or anywhere else in this package.
// RunInChildContext's actual, current oversized-result handling is
// checkResultSize (see errors.go's own extensively-documented research
// on this exact question for Step/Invoke/child-context results): it
// unconditionally returns a client-side *ResultTooLargeError BEFORE ever
// calling Checkpoint().Enqueue, for ANY oversized result, with no
// ReplayChildren branch, no partial/empty-payload checkpoint, and no
// second-execution replay mode of any kind. There is no
// currently-exported way for a caller to opt into or trigger the
// YAML-documented ReplayChildren behavior - the SDK's real, current
// behavior for this exact scenario is to fail fast with a clear typed
// error rather than attempt (and get wrong) an unimplemented backend
// protocol.
//
// This handler is still registered and deployed (not left UNCOVERED) so
// this discrepancy is directly, empirically demonstrated against a real
// invocation rather than merely asserted: it builds the same >256KB
// child-context result the YAML describes (a step returns a small
// per-line marker, the child body repeats it past the 750KB
// resultTooLargeThresholdBytes checkResultSize enforces - see errors.go),
// prints the input once per child-context execution exactly as
// ExpectedLogs describes, and lets RunInChildContext's real,
// unmodified checkResultSize path run to see what actually happens. See
// template.yaml's TestingMetadata.NotImplemented block for this
// specific, evidence-based gap statement (distinct from the existing
// Result-field gap already documented there for other requirements).
package handlers

import (
	"fmt"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// child3_11LargeRepeatCount * len(marker) comfortably exceeds
// checkpoint.DefaultLimits().MaxPayloadBytes (750KB) - the same threshold
// checkResultSize enforces (see errors.go) - so the child context's own
// JSON-serialized result genuinely exceeds the size that would trigger
// ReplayChildren mode per this requirement's YAML, regardless of the
// specific input string's own length.
const child3_11LargeRepeatCount = 100_000

func init() {
	Register("3-11", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_11Handler, config(client))
	})
}

func child3_11Handler(event string, dc types.DurableContext) (string, error) {
	_, err := operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		// Raw stdout, deliberately NOT sc.Logger() - ExpectedLogs
		// validates raw standard output, matching step_1_17/1_18's own
		// convention, and validates this child context body itself
		// (not the nested step) runs exactly twice: once on the first
		// invocation, once more were the SDK to actually implement the
		// YAML's described re-execution-on-replay ReplayChildren mode.
		fmt.Println(event)

		small, err := operations.Step(child, "small_step", func(sc types.StepContext) (string, error) {
			return event, nil
		})
		if err != nil {
			return "", err
		}

		// Build a >256KB result from the step's small value - this is
		// what "child context function builds a large result from it"
		// means: the size comes from the CHILD CONTEXT's own return
		// value, not from the nested step's checkpointed result (which
		// stays small, matching the YAML's own StepSucceeded with no
		// Result payload assertion).
		large := strings.Repeat(small+",", child3_11LargeRepeatCount)
		return large, nil
	})
	if err != nil {
		return "", err
	}

	// The YAML's own invocations describe a WaitStarted/suspend/resume
	// cycle following the child context, structurally identical to
	// 3-9's Wait-after-child shape - included here so this handler
	// exercises the full documented scenario even though the genuine
	// gap above (checkResultSize's client-side *ResultTooLargeError) is
	// expected to end this execution before ever reaching this Wait in
	// practice.
	if err := operations.Wait(dc, "wait_after_child", types.Duration{Seconds: 1}); err != nil {
		return "", err
	}

	return "", nil
}
