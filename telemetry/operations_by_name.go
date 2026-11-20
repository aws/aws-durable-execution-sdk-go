package telemetry

import "encoding/json"

// OperationSummary aggregates every occurrence of a given operation name
// within a Record's Operations slice. The CloudWatch Logs, Lambda log, and
// DynamoDB exporters always render it. Other exporters render it when
// configured with OperationsFormatByName or OperationsFormatBoth.
type OperationSummary struct {
	Type    string `json:"type"`
	SubType string `json:"subType,omitempty"`

	Count int `json:"count"`

	// The duration fields are nil when no occurrence carried a DurationMs,
	// so the JSON omits them; a recorded duration of 0 ms serializes as 0.
	// This matches the JavaScript implementation.
	MinDurationMs   *int64 `json:"minDurationMs,omitempty"`
	MaxDurationMs   *int64 `json:"maxDurationMs,omitempty"`
	TotalDurationMs *int64 `json:"totalDurationMs,omitempty"`

	FailedCount int `json:"failedCount"`
	MaxAttempt  int `json:"maxAttempt,omitempty"`

	// Status reflects the most recently seen occurrence's status.
	Status string `json:"status"`

	// Error is populated only when the name occurred exactly once and
	// the operation has an error. For repeated names (loops, retries),
	// it is left nil; FailedCount still flags failures.
	Error *ErrorRecord `json:"error,omitempty"`

	// Result is populated only when the name occurred exactly once and
	// the operation has a result.
	Result json.RawMessage `json:"result,omitempty"`
}

// GroupOperationsByName aggregates operations into a by-name view.
// Operations with an empty Name are excluded. The aggregation rules are:
//   - Count, FailedCount, MaxAttempt, and duration metrics are aggregated
//     across all occurrences sharing a name.
//   - Type, SubType, and Status reflect the last occurrence encountered.
//   - Error and Result are included only for names that occurred exactly
//     once.
func GroupOperationsByName(ops []OperationRecord) map[string]OperationSummary {
	result := make(map[string]OperationSummary)
	lastByName := make(map[string]OperationRecord)

	for _, op := range ops {
		if op.Name == "" {
			continue
		}

		agg, exists := result[op.Name]
		if !exists {
			agg = OperationSummary{}
		}

		agg.Count++
		agg.Type = op.Type
		agg.SubType = op.SubType
		agg.Status = op.Status

		if op.Status == "FAILED" {
			agg.FailedCount++
		}
		if op.Attempt > agg.MaxAttempt {
			agg.MaxAttempt = op.Attempt
		}

		if op.DurationMs != nil {
			d := *op.DurationMs
			// A nil MinDurationMs means no occurrence of this name carried a
			// DurationMs yet (e.g. earlier ones were in progress): seed
			// min/max from this first timed occurrence rather than comparing
			// against a zero value.
			if agg.MinDurationMs == nil {
				agg.MinDurationMs = ptrTo(d)
				agg.MaxDurationMs = ptrTo(d)
				agg.TotalDurationMs = ptrTo(d)
			} else {
				if d < *agg.MinDurationMs {
					*agg.MinDurationMs = d
				}
				if d > *agg.MaxDurationMs {
					*agg.MaxDurationMs = d
				}
				*agg.TotalDurationMs += d
			}
		}

		result[op.Name] = agg
		lastByName[op.Name] = op
	}

	// Error and Result are copied only for a name that occurred exactly once.
	//
	// A summary has one Error field and one Result field. A name that
	// occurred more than once (for example a step called in a loop) has one
	// error or result per occurrence. No single occurrence represents all of
	// them. So for a repeated name both fields stay empty. Count,
	// FailedCount, MaxAttempt, and the duration fields describe the
	// occurrences instead. FailedCount > 0 still shows that one failed.
	//
	// The copy runs in this second pass because Count is final only after
	// the first pass has seen every operation.
	//
	// Consequence for Truncate: removing one of two operations that share a
	// name lowers Count to 1. The remaining operation's Error and Result
	// then appear in the summary. So removing an operation can make the
	// by-name rendering larger.
	for name, agg := range result {
		if agg.Count != 1 {
			continue
		}
		op := lastByName[name]
		if op.Error != nil {
			agg.Error = op.Error
		}
		if op.Result != nil {
			agg.Result = op.Result
		}
		result[name] = agg
	}

	return result
}

func ptrTo(v int64) *int64 { return &v }
