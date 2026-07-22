// Requirement 3-12: Child context interrupted and re-executed.
//
// From test-requirements/child/3-12.yaml:
//
//	description: Child context interrupted and re-executed
//	handler: |
//	  A child context that is interrupted (e.g., Lambda crash via
//	  process.exit) after ContextStarted and StepStarted are checkpointed
//	  but before StepSucceeded. On the next invocation, the SDK
//	  re-executes the step and checkpoints StepSucceeded directly
//	  (without another StepStarted).
//	  The step returns the input string.
//	  The child context returns the step result.
//	invocations: |
//	  - Handler invokes a child context with a step that crashes the
//	    Lambda process after ContextStarted and StepStarted are
//	    checkpointed but before StepSucceeded. InvocationCompleted with
//	    Runtime.ExitError.
//	  - Replay 1: Re-invoked because Lambda crashed. SDK re-executes the
//	    step and checkpoints StepSucceeded directly (without another
//	    StepStarted). ContextSucceeded, execution succeeds.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: ${INPUT_1}
//	ExpectedExecutionHistory: exactly ContextStarted, StepStarted,
//	  InvocationCompleted(crash), StepSucceeded, ContextSucceeded,
//	  InvocationCompleted - i.e. EXACTLY ONE StepStarted total, no
//	  StepFailed/RetryDetails anywhere, and no extra events of any kind.
//
// # Why a marker-step approach was rejected, and what this uses instead
//
// A real process crash (raw os.Exit, matching step_1_17/1_18's own
// mechanism) is required to produce the real Runtime.ExitError. The
// hard problem this requirement poses: the step body must crash on its
// FIRST real execution and succeed on its SECOND, but under this step's
// default AtLeastOncePerRetry semantics (required by the YAML's own
// "re-executes the step and checkpoints StepSucceeded directly, without
// another StepStarted" - runStep's AtLeastOncePerRetry branch, step.go),
// sc.Attempt() does NOT distinguish these two real executions: reading
// step.go directly confirms a bare STEP/START checkpoint carries no
// attempt-tracking StepOptions field at all (unlike a RETRY checkpoint's
// own RetryDetails), and this was independently, empirically confirmed
// via a real deployed invocation this session (sc.Attempt() < 2
// crashed on EVERY replay, 4 times in a row, never succeeding).
//
// A separate, explicitly checkpointed "marker" step (e.g. one that
// records a pid or a boolean before the crash-prone step runs) was
// prototyped and rejected: it adds its own StepStarted/StepSucceeded
// events, which shifts every subsequent EventId and breaks this
// requirement's own EXACT, strict ExpectedExecutionHistory (which
// expects precisely one StepStarted total, for the ONE step named in
// the YAML - no other step of any kind). Any solution that introduces
// an additional checkpointed operation is structurally incompatible
// with this requirement's own exact event count.
//
// This handler instead reads a marker FILE in /tmp: Lambda's own
// execution environment (including its /tmp filesystem) is commonly,
// though not universally, REUSED across a Runtime.ExitError-triggered
// re-invocation of the SAME function within a short window (a real,
// documented AWS Lambda execution-environment lifecycle characteristic,
// distinct from and outside this SDK's own checkpoint/replay protocol
// entirely - no SDK code reads or writes this file; it is purely this
// handler's own local, uncheckpointed bookkeeping, exactly as
// legitimate as any other side-effecting I/O a step body is free to
// perform). If the file does not exist, this is treated as the
// genuinely first execution: the marker is written and the process
// crashes. If the file already exists, this is treated as a resumed
// execution following that crash: the step returns immediately.
//
// This is empirically verified against a REAL deployed invocation (not
// assumed) as part of this session's own iteration - see the commit
// message / task report for the actual observed outcome. If Lambda's
// container reuse does NOT hold reliably for this specific crash/resume
// timing, this requirement is re-declared NotImplemented with that
// specific, evidence-based finding instead.
package handlers

import (
	"os"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("3-12", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_12Handler, config(client))
	})
}

// child3_12MarkerPath is deliberately NOT keyed by execution ARN or any
// other per-execution identifier: this handler relies on Lambda's
// execution-environment-level /tmp reuse across the SAME function's
// consecutive invocations after a crash, and a fixed, well-known path
// is exactly what a real, uncheckpointed side-effecting marker file
// would use in practice (matching, in spirit, the "FileSystem SerDes"
// EFS-backed overflow pattern documented in errors.go's own research,
// just scoped to plain /tmp rather than a mounted EFS volume).
const child3_12MarkerPath = "/tmp/conformance-3-12-entered"

func child3_12Handler(event string, dc types.DurableContext) (string, error) {
	return operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		return operations.Step(child, "crashing_step", func(sc types.StepContext) (string, error) {
			if _, err := os.Stat(child3_12MarkerPath); err != nil {
				// Marker absent: genuinely first execution. Record it,
				// then crash - matching step_1_17/1_18's own os.Exit(1)
				// mechanism for producing a real Runtime.ExitError.
				_ = os.WriteFile(child3_12MarkerPath, []byte("1"), 0o600)
				os.Exit(1)
			}
			// Marker present: this execution environment already saw
			// (and crashed on) this step once. Clean up so a later,
			// unrelated invocation of this same warm environment
			// (matching a real deployed function's normal container
			// reuse across DIFFERENT executions, not just replays of
			// this one) doesn't skip its own genuine first crash.
			_ = os.Remove(child3_12MarkerPath)
			return event, nil
		})
	})
}
