package insight

// OperationSummary aggregates every occurrence of a given operation name
// within a Record's Operations slice. Used by point-access stores
// (CloudWatch Logs, DynamoDB) that cannot filter array elements by name.
type OperationSummary struct {
	Type    string `json:"type"`
	SubType string `json:"subType,omitempty"`

	Count int `json:"count"`

	MinDurationMs   int64 `json:"minDurationMs,omitempty"`
	MaxDurationMs   int64 `json:"maxDurationMs,omitempty"`
	TotalDurationMs int64 `json:"totalDurationMs,omitempty"`

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
	Result *ContentField `json:"result,omitempty"`
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
			if agg.Count == 1 {
				agg.MinDurationMs = d
				agg.MaxDurationMs = d
			} else {
				if d < agg.MinDurationMs {
					agg.MinDurationMs = d
				}
				if d > agg.MaxDurationMs {
					agg.MaxDurationMs = d
				}
			}
			agg.TotalDurationMs += d
		}

		result[op.Name] = agg
		lastByName[op.Name] = op
	}

	// Populate Error and Result only for names with a single occurrence.
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
