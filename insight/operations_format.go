package insight

import "encoding/json"

// OperationsFormat controls how an exporter renders a record's operations.
type OperationsFormat string

const (
	// OperationsFormatArray renders only the canonical Operations array.
	// This is the default.
	OperationsFormatArray OperationsFormat = "array"

	// OperationsFormatByName renders only the by-name aggregation,
	// omitting the Operations array.
	OperationsFormatByName OperationsFormat = "by-name"

	// OperationsFormatBoth renders both the Operations array and the
	// by-name aggregation.
	OperationsFormatBoth OperationsFormat = "both"
)

// RenderRecord marshals a Record to JSON with operations formatted
// according to the specified format. When format is empty, it defaults
// to OperationsFormatArray.
func RenderRecord(record Record, format OperationsFormat) ([]byte, error) {
	if format == "" {
		format = OperationsFormatArray
	}

	if format == OperationsFormatArray {
		return json.Marshal(record)
	}

	// Marshal once to get the base fields, then manipulate the operations
	// representation.
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
		byName, err := json.Marshal(GroupOperationsByName(record.Operations))
		if err != nil {
			return nil, err
		}
		fields["operationsByName"] = byName
	case OperationsFormatBoth:
		byName, err := json.Marshal(GroupOperationsByName(record.Operations))
		if err != nil {
			return nil, err
		}
		fields["operationsByName"] = byName
	}

	return json.Marshal(fields)
}
