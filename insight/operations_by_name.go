package insight

// OperationByName aggregates every occurrence of a given operation name
// within a single WorkflowInsightRecord's Operations array, matching the
// JS SDK's own operationsByName entry shape exactly - used by
// point-access stores (CloudWatch Logs, DynamoDB, the Lambda log
// exporter's own "by-name"/"both" rendering) that cannot filter "the
// array element named X" the way an array-native store (S3/Athena,
// OpenSearch, Aurora/Redshift) can, trading per-occurrence array detail
// for a simple dot-path/key lookup instead.
type OperationByName struct {
	Type    string `json:"type"`
	SubType string `json:"subType,omitempty"`

	// Count is the number of times this name occurred in Operations.
	Count int `json:"count"`

	MinDurationMs   int64 `json:"minDurationMs,omitempty"`
	MaxDurationMs   int64 `json:"maxDurationMs,omitempty"`
	TotalDurationMs int64 `json:"totalDurationMs,omitempty"`

	FailedCount int `json:"failedCount"`
	MaxAttempt  int `json:"maxAttempt,omitempty"`

	// Status reflects the MOST RECENTLY SEEN occurrence's own status
	// (matching Type/SubType, both likewise "most recently seen") - see
	// this package's own OperationsByName doc for why, unlike the
	// numeric metrics above, these three fields are not aggregated
	// across occurrences.
	Status string `json:"status"`

	// Error is populated ONLY when this name occurred EXACTLY ONCE
	// (Count == 1) - for a repeated name (loops/retries/map items),
	// there is no single representative value, so it is left nil;
	// FailedCount still flags whether any occurrence failed. Result
	// follows the identical rule, for the identical reason - see this
	// package's own OperationsByName doc.
	Error *ErrorDetail `json:"error,omitempty"`

	// Result mirrors OperationRecord.Result's own doc exactly (same
	// opt-in-via-OperationOverride.Result default, same JS-SDK-inherited
	// caveat around custom Serdes) - populated ONLY when this name
	// occurred EXACTLY ONCE, for the same reason Error is (see above).
	// Since OperationsByName is always called AFTER applyContent has
	// already run (see renderRecord/Plugin.dispatch's own call order),
	// this is simply op.Result's own already-decoded value passed
	// through, not a second independent decode.
	Result any `json:"result,omitempty"`
}

// OperationsByName aggregates ops into the by-name view described on
// OperationByName's own doc, matching the JS SDK's own documented
// aggregation rules exactly:
//
//   - Operations with an empty Name are excluded entirely (they can't be
//     keyed or queried by name).
//   - Count/FailedCount/MaxAttempt/*DurationMs are aggregated across
//     every occurrence sharing a name.
//   - Type/SubType/Status reflect the LAST occurrence encountered for
//     that name, in Operations' own order (i.e. "most recently seen",
//     matching the array's own insertion order - see Plugin's own
//     upsertOperationLocked, which appends new names and updates
//     existing ones IN PLACE at their original index, so "most recently
//     seen" here actually means "most recently updated," consistent
//     with the JS SDK's own semantics for a record built the same way).
//   - Error is populated only for a name that occurred exactly once,
//     and only when the underlying OperationRecord itself already has
//     one (matching whatever Content/OperationsConfig.IncludeErrors
//     already decided upstream, in applyContent - this function never
//     independently re-applies that rule). Result follows the identical
//     single-occurrence rule, also passed through from whatever
//     applyContent already populated (or left nil, its own default).
func OperationsByName(ops []OperationRecord) map[string]OperationByName {
	result := make(map[string]OperationByName)
	// lastByName tracks, per name, the raw OperationRecord most recently
	// merged - needed for Error, which is only knowable to apply once
	// every operation has been visited and a name is confirmed to have
	// occurred exactly once.
	lastByName := make(map[string]OperationRecord)

	for _, op := range ops {
		if op.Name == "" {
			continue
		}
		agg, exists := result[op.Name]
		if !exists {
			agg = OperationByName{}
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
			if !exists || agg.Count == 1 {
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
