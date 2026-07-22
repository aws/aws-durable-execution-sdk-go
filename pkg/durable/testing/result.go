package testing

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// TestResult is the object returned by LocalTestRunner.Run. It exposes the
// execution status, the final result, any error, and the full operation
// history — mirroring the official cross-SDK Testing API Reference's
// TestResult type (docs.aws.amazon.com/durable-execution/testing/api-reference/,
// "TestResult").
type TestResult struct {
	status        types.ExecutionStatus
	resultPayload *string
	errorMessage  *string
	operations    map[string]types.Operation
	runner        callbackDriver
}

// GetStatus returns the execution's terminal status for this invocation:
// SUCCEEDED, FAILED, or PENDING (the execution suspended on a Wait,
// retry-delay, or Callback and has not yet completed).
func (r TestResult) GetStatus() types.ExecutionStatus { return r.status }

// GetResult deserializes the execution's final output into T. Returns an
// error if the execution did not succeed or produced no result.
//
// Go has no single "untyped JSON result" return type the way TypeScript's
// getResult(): TResult | undefined or Python's raw result string do, so
// this is a generic free function (testing.GetResult[T](result)) rather
// than a method, matching the same pattern used for StepResult.
func GetResult[T any](r TestResult) (T, error) {
	var zero T
	if r.status != types.ExecutionStatusSucceeded {
		return zero, fmt.Errorf("testing.GetResult: execution did not succeed (status=%s)", r.status)
	}
	if r.resultPayload == nil {
		return zero, fmt.Errorf("testing.GetResult: execution succeeded but no result payload was recorded")
	}
	if err := json.Unmarshal([]byte(*r.resultPayload), &zero); err != nil {
		return zero, fmt.Errorf("testing.GetResult: unmarshaling result into %T: %w", zero, err)
	}
	return zero, nil
}

// GetError returns the execution's error message, if it failed. Returns
// ("", false) if the execution succeeded or is still pending.
func (r TestResult) GetError() (string, bool) {
	if r.errorMessage == nil {
		return "", false
	}
	return *r.errorMessage, true
}

// GetOperation returns the top-level operation named name, or ok=false if
// no such operation was recorded. Matches the official runner API's
// getOperation(name) (see the Testing API Reference's "Inspect
// operations").
//
// Only top-level operations are searched; use GetOperationRecursive to
// also search inside child contexts, matching Python's
// get_all_operations() vs. operations distinction.
func (r TestResult) GetOperation(name string) (Operation, bool) {
	for _, op := range r.operations {
		if op.Name == name {
			return Operation{raw: op, runner: r.runner}, true
		}
	}
	return Operation{}, false
}

// GetOperationRecursive returns the operation named name, searching both
// top-level operations and every child context's nested operations.
// Matches Python's result.get_all_operations() combined with a name
// filter.
func (r TestResult) GetOperationRecursive(name string) (Operation, bool) {
	if op, ok := r.GetOperation(name); ok {
		return op, true
	}
	for _, op := range r.operations {
		if op.Type != types.OperationTypeContext {
			continue
		}
		for _, child := range r.GetChildOperations(op.ID) {
			if child.GetName() == name {
				return child, true
			}
		}
	}
	return Operation{}, false
}

// GetOperations returns every top-level operation recorded for this
// execution (not including nested child-context operations — see
// GetChildOperations on a CONTEXT operation for those), matching the
// official runner API's getOperations()/result.operations.
func (r TestResult) GetOperations() []Operation {
	out := make([]Operation, 0, len(r.operations))
	for _, op := range r.operations {
		out = append(out, Operation{raw: op, runner: r.runner})
	}
	return out
}

// GetOperationsByStatus returns every top-level operation with the given
// status, matching the official runner API's
// getOperations({status})/getFailedOperations()/getSucceededOperations().
func (r TestResult) GetOperationsByStatus(status types.OperationStatus) []Operation {
	var out []Operation
	for _, op := range r.operations {
		if op.Status == status {
			out = append(out, Operation{raw: op, runner: r.runner})
		}
	}
	return out
}

// GetChildOperations returns the operations nested directly under the
// CONTEXT operation identified by parentID, matching the official runner
// API's getChildOperations() on a DurableOperation.
//
// Relationship is derived from Operation.ParentID directly - NOT from
// the hierarchical step-ID-prefix convention this method used prior to
// this SDK's SHA-256 operation-ID-hashing change (see
// docs/remaining-work.md's "SHA-256 operation ID hashing" section for
// the full writeup). That prefix-string-parsing approach is now
// IMPOSSIBLE: every operation ID minted by context.Context.NextStepID is
// a SHA-256 hash (see that method's doc), and a hash reveals nothing
// whatsoever about the string it was computed from - "is op.ID prefixed
// by parentID + '-'" can never again be true for any two real IDs, since
// neither ID's bytes bear any relationship to the other's input string.
//
// This is a forced design change, not a style preference: ParentID (see
// dcontext.Context.ParentStepID's doc, and docs/remaining-work.md §0 for
// the earlier session that first populated it on every checkpoint) is
// now the ONLY signal available anywhere in this SDK for reconstructing
// parent/child structure, on this in-memory test client exactly as much
// as on the real backend. Before hashing existed, this method
// deliberately preferred the prefix convention over ParentID as a
// simpler, equally-correct alternative for this SDK-internal client
// specifically, since both signals agreed by construction; that
// equivalence no longer holds now that hashing has made the prefix
// signal permanently unusable, so this method (and
// testing.EventSignatures' Depth/Index computation, which had the exact
// same dependency - see signature.go) both had to switch to ParentID for
// real, not just as a hypothetical fallback.
func (r TestResult) GetChildOperations(parentID string) []Operation {
	var out []Operation
	for _, op := range r.operations {
		if op.ParentID == parentID {
			out = append(out, Operation{raw: op, runner: r.runner})
		}
	}
	return out
}
