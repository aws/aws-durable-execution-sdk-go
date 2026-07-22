package durable

// validateReplayConsistency checks that a checkpointed operation matches
// the Type, SubType, and Name expected by the current code. It fires only
// during replay (op non-nil) and only on genuine mismatch — a nil op means
// no checkpoint exists (first execution) and skips validation.
//
// The JS SDK validates all three fields (Type, SubType, Name). SubType is
// checked strictly: a mismatch is always an error. Name is also checked
// strictly, matching the JS reference SDK's validateReplayConsistency.
//
// Returns nil when:
//   - op is nil (first execution, no checkpoint to validate)
//   - All present fields match
//
// Returns *NonDeterministicReplayError on mismatch.
func validateReplayConsistency(op *operation, expectedType, expectedSubType, expectedName string) error {
	if op == nil {
		return nil
	}
	// Skip if the checkpointed operation has no type (should not occur in
	// practice — the backend always populates Type).
	if op.opType == "" {
		return nil
	}

	if op.opType != expectedType {
		return &NonDeterministicReplayError{
			Name:            expectedName,
			StepID:          op.id,
			ExpectedType:    expectedType,
			ExpectedSubType: expectedSubType,
			ExpectedName:    expectedName,
			ActualType:      op.opType,
			ActualSubType:   op.subType,
			ActualName:      op.name,
		}
	}
	if expectedSubType != "" && op.subType != expectedSubType {
		return &NonDeterministicReplayError{
			Name:            expectedName,
			StepID:          op.id,
			ExpectedType:    expectedType,
			ExpectedSubType: expectedSubType,
			ExpectedName:    expectedName,
			ActualType:      op.opType,
			ActualSubType:   op.subType,
			ActualName:      op.name,
		}
	}
	if op.name != expectedName {
		return &NonDeterministicReplayError{
			Name:            expectedName,
			StepID:          op.id,
			ExpectedType:    expectedType,
			ExpectedSubType: expectedSubType,
			ExpectedName:    expectedName,
			ActualType:      op.opType,
			ActualSubType:   op.subType,
			ActualName:      op.name,
		}
	}
	return nil
}

// checkResultSize validates that a serialized result fits within the
// checkpoint batch payload limit. Returns *ResultTooLargeError if the
// size exceeds [resultSizeLimitBytes].
func checkResultSize(serialized []byte, name string) error {
	if len(serialized) <= resultSizeLimitBytes {
		return nil
	}
	return &ResultTooLargeError{
		Name:       name,
		SizeBytes:  len(serialized),
		LimitBytes: resultSizeLimitBytes,
	}
}
