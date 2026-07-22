package insight

import "encoding/json"

// OperationsFormat controls how an exporter renders a record's own
// operations for exporters that support both the canonical array and the
// by-name aggregation, matching the JS SDK's own operationsFormat option
// (shared across FirehoseExporter, EventBridgeExporter, SQSExporter,
// OTelExporter, HttpExporter, and FileExporter - the array-native
// exporters, S3/OpenSearch/Aurora/Redshift, and the two ALWAYS-by-name
// point-access exporters, CloudWatchLogs/DynamoDB, do not have this
// option at all - see each of those exporters' own doc for why).
type OperationsFormat string

const (
	// OperationsFormatArray renders only the canonical Operations array
	// (WorkflowInsightRecord's own shape, unchanged). The default.
	OperationsFormatArray OperationsFormat = "array"

	// OperationsFormatByName renders only the OperationsByName
	// aggregation, omitting the Operations array entirely.
	OperationsFormatByName OperationsFormat = "by-name"

	// OperationsFormatBoth renders both the Operations array AND the
	// OperationsByName aggregation together.
	OperationsFormatBoth OperationsFormat = "both"
)

// renderedRecord is the JSON shape actually sent by an exporter that
// supports OperationsFormat - a WorkflowInsightRecord with its own
// "operations" field REPLACED (never both under the same key) by
// whichever of Operations/OperationsByName the configured format calls
// for, plus an additional "operationsByName" key when format is
// OperationsFormatBoth. Marshaled by hand (rather than via a struct with
// omitempty tags) because the KEY to omit depends on a runtime value
// (format), not a static Go type shape - a single struct type cannot
// conditionally omit "operations" for by-name mode while conditionally
// omitting "operationsByName" for array mode using field tags alone.
func renderRecord(record WorkflowInsightRecord, format OperationsFormat) ([]byte, error) {
	if format == "" {
		format = OperationsFormatArray
	}

	// Marshal the record once to get every OTHER field's own JSON
	// representation, then splice in whichever operations-shaped field(s)
	// this format calls for - avoids hand-duplicating every single field
	// WorkflowInsightRecord has (which would silently drift out of sync
	// with record.go over time) just to change how ONE field renders.
	base, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(base, &fields); err != nil {
		return nil, err
	}

	switch format {
	case OperationsFormatByName:
		delete(fields, "operations")
		byNameJSON, err := json.Marshal(OperationsByName(record.Operations))
		if err != nil {
			return nil, err
		}
		fields["operationsByName"] = byNameJSON
	case OperationsFormatBoth:
		byNameJSON, err := json.Marshal(OperationsByName(record.Operations))
		if err != nil {
			return nil, err
		}
		fields["operationsByName"] = byNameJSON
	default: // OperationsFormatArray - fields["operations"] is already correct as-is.
	}

	return json.Marshal(fields)
}
