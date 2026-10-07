package durable

// validateReplayConsistency checks that a checkpointed operation matches
// the Type, SubType, and Name expected by the current code. It fires only
// during replay (op non-nil) and only on genuine mismatch — a nil op means
// no checkpoint exists (first execution) and skips validation.
//
// All three fields (Type, SubType, Name) are validated. SubType is
// checked strictly: a mismatch is always an error. Name is also checked
// strictly.
//
// Returns nil when:
//   - op is nil (first execution, no checkpoint to validate)
//   - All present fields match
//
// Returns *NonDeterministicExecutionError on mismatch.
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
		return &NonDeterministicExecutionError{
			Name:            expectedName,
			StepID:          op.id,
			CurrentType:     expectedType,
			CurrentSubType:  expectedSubType,
			CurrentName:     expectedName,
			RecordedType:    op.opType,
			RecordedSubType: op.subType,
			RecordedName:    op.name,
		}
	}
	if expectedSubType != "" && op.subType != expectedSubType {
		return &NonDeterministicExecutionError{
			Name:            expectedName,
			StepID:          op.id,
			CurrentType:     expectedType,
			CurrentSubType:  expectedSubType,
			CurrentName:     expectedName,
			RecordedType:    op.opType,
			RecordedSubType: op.subType,
			RecordedName:    op.name,
		}
	}
	if op.name != expectedName {
		return &NonDeterministicExecutionError{
			Name:            expectedName,
			StepID:          op.id,
			CurrentType:     expectedType,
			CurrentSubType:  expectedSubType,
			CurrentName:     expectedName,
			RecordedType:    op.opType,
			RecordedSubType: op.subType,
			RecordedName:    op.name,
		}
	}
	return nil
}
