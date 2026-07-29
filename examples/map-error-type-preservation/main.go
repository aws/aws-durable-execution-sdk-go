// Command map-error-type-preservation demonstrates that a failed map
// item's error preserves its concrete SDK wrapper types through the
// checkpoint-and-replay cycle. After a Wait forces replay, errors.As
// succeeds for both ChildContextError and StepError with field values
// matching the live run. The leaf user-defined error type survives as a
// type name string, which is the expected behavior for user types that
// cannot be generically instantiated.
package main

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// PaymentError is a user-defined error type that the SDK preserves only
// as a type name string through replay (concrete reconstruction is not
// possible for user types).
type PaymentError struct {
	Code string
}

func (e *PaymentError) Error() string {
	return fmt.Sprintf("payment declined: %s", e.Code)
}

// ErrorSnapshot captures structural properties of a failed map item's
// error chain for comparison across replay boundaries.
type ErrorSnapshot struct {
	IsChildCtxErr    bool   `json:"isChildCtxErr"`
	ChildCtxName     string `json:"childCtxName"`
	IsStepErr        bool   `json:"isStepErr"`
	StepName         string `json:"stepName"`
	StepAttempts     int    `json:"stepAttempts"`
	LeafTypeName     string `json:"leafTypeName"`
	LeafMessage      string `json:"leafMessage"`
	IsOperationError bool   `json:"isOperationError"`
}

// Output reports whether error type info is preserved across replay.
type Output struct {
	Preserved    bool          `json:"preserved"`
	BeforeReplay ErrorSnapshot `json:"beforeReplay"`
	AfterReplay  ErrorSnapshot `json:"afterReplay"`
	SuccessCount int           `json:"successCount"`
	FailureCount int           `json:"failureCount"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	items := []int{1, 2, 3}

	results, err := durable.Map(ctx, "payment-batch", items,
		func(ctx durable.Context, item int, _ int) (string, error) {
			return durable.Step(ctx, "charge",
				func(_ durable.StepContext) (string, error) {
					if item == 2 {
						return "", &PaymentError{Code: "INSUFFICIENT_FUNDS"}
					}
					return fmt.Sprintf("receipt-%d", item), nil
				},
				durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
					MaxAttempts: 1,
				})),
			)
		},
	)
	if err != nil {
		return Output{}, err
	}

	// Capture error properties on the live execution (before replay).
	beforeReplay, err := durable.Step(ctx, "snapshot-before",
		func(_ durable.StepContext) (ErrorSnapshot, error) {
			return captureSnapshot(results), nil
		},
		durable.WithRetry(durable.NoRetry()),
	)
	if err != nil {
		return Output{}, err
	}

	// Wait forces a checkpoint; the next invocation replays all
	// preceding operations from stored metadata.
	if err := durable.Wait(ctx, "replay-boundary", 1*time.Second); err != nil {
		return Output{}, err
	}

	// Capture error properties after replay. The BatchResult was
	// reconstructed from checkpoint data; errors.As must still succeed
	// for ChildContextError and StepError.
	afterReplay, err := durable.Step(ctx, "snapshot-after",
		func(_ durable.StepContext) (ErrorSnapshot, error) {
			return captureSnapshot(results), nil
		},
		durable.WithRetry(durable.NoRetry()),
	)
	if err != nil {
		return Output{}, err
	}

	return Output{
		Preserved:    beforeReplay == afterReplay,
		BeforeReplay: beforeReplay,
		AfterReplay:  afterReplay,
		SuccessCount: results.SuccessCount(),
		FailureCount: results.FailureCount(),
	}, nil
}

// captureSnapshot extracts structural error properties from the first
// failed item in the batch result using errors.As to probe the chain.
func captureSnapshot(results durable.BatchResult[string]) ErrorSnapshot {
	errs := results.Errors()
	if len(errs) == 0 {
		return ErrorSnapshot{}
	}
	e := errs[0]

	var snap ErrorSnapshot

	var childErr *durable.ChildContextError
	if errors.As(e, &childErr) {
		snap.IsChildCtxErr = true
		snap.ChildCtxName = childErr.Name
	}

	var stepErr *durable.StepError
	if errors.As(e, &stepErr) {
		snap.IsStepErr = true
		snap.StepName = stepErr.Name
		snap.StepAttempts = stepErr.Attempts
		if stepErr.Err != nil {
			snap.LeafTypeName, snap.LeafMessage = extractLeaf(stepErr.Err)
		}
	}

	var opErr *durable.OperationError
	if errors.As(e, &opErr) {
		snap.IsOperationError = true
	}

	return snap
}

// extractLeaf returns the type name and message of a leaf error uniformly
// for both live and replayed errors:
//   - Live: type name from reflect, message from Error()
//   - Replay (replayedError): Error() is "TypeName: message", and the
//     reflect type is "replayedError" — detect this and parse from Error()
func extractLeaf(err error) (typeName, message string) {
	t := reflect.TypeOf(err)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	name := t.Name()

	// If the concrete type is an SDK internal replay placeholder, the
	// type name and message are encoded in the Error() string.
	if name == "replayedError" || strings.Contains(name, "replayed") {
		s := err.Error()
		if idx := strings.Index(s, ": "); idx > 0 {
			return s[:idx], s[idx+2:]
		}
		return name, s
	}

	// Live execution: use the Go type name and Error() as the message.
	return name, err.Error()
}

func main() { durable.Start(handler) }
