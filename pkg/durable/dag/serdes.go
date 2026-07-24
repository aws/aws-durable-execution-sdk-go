package dag

import (
	"encoding/json"
	"time"
)

// unmarshalResult decodes a JSON-encoded task result into T. Used on the
// replay/deserialization path by Result[T]. See DAG_SPEC_GO.md §8.
func unmarshalResult[T any](raw []byte) (T, error) {
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, err
	}
	return v, nil
}

// errorObject is the persisted form of a task failure (message + type),
// modeled on the base SDK's own error persistence. It is reconstructed on
// replay as a *replayedError.
type errorObject struct {
	Type    string `json:"type,omitempty"`
	Message string `json:"message"`
}

// replayedError is a value-typed error reconstructed from an errorObject on
// the deserialization path.
type replayedError struct {
	errType string
	message string
}

func (e *replayedError) Error() string { return e.message }

// ErrorType returns the persisted error type name, if any.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (e *replayedError) ErrorType() string { return e.errType }

func toErrorObject(err error) *errorObject {
	if err == nil {
		return nil
	}
	return &errorObject{Type: errorTypeName(err), Message: err.Error()}
}

func errorTypeName(err error) string {
	switch err.(type) {
	case *DagExecutionError:
		return "DagExecutionError"
	default:
		return "error"
	}
}

// serializedTaskExecution is the wire form of a TaskExecution. The result is
// stored as raw JSON and unmarshaled lazily into T at Result[T]/Get[T].
type serializedTaskExecution struct {
	Name        string          `json:"name"`
	Status      TaskStatus      `json:"status"`
	SkipReason  SkipReason      `json:"skipReason,omitempty"`
	Kind        resultKind      `json:"resultKind,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	Err         *errorObject    `json:"error,omitempty"`
	StartedAt   *time.Time      `json:"startedAt,omitempty"`
	CompletedAt *time.Time      `json:"completedAt,omitempty"`
}

// serializedDagResult is the wire form of a DagResult. Summary is
// observability-only and never read on replay for control flow.
type serializedDagResult struct {
	Tasks            []serializedTaskExecution `json:"tasks"`
	CompletionReason CompletionReason          `json:"completionReason"`
	Summary          string                    `json:"summary,omitempty"`
}

func serializeTaskExecution(te TaskExecution) (serializedTaskExecution, error) {
	out := serializedTaskExecution{
		Name:       te.Name,
		Status:     te.Status,
		SkipReason: te.SkipReason,
		Kind:       te.kind,
		Err:        toErrorObject(te.Err),
	}
	if !te.StartedAt.IsZero() {
		s := te.StartedAt
		out.StartedAt = &s
	}
	if !te.CompletedAt.IsZero() {
		c := te.CompletedAt
		out.CompletedAt = &c
	}
	if te.Status == StatusSucceeded {
		var (
			raw []byte
			err error
		)
		if te.kind == kindDag {
			// Nested DAG: recurse into its own serialized form.
			if sub, ok := te.result.(*DagResult); ok {
				raw, err = serializeDagResult(sub)
			} else if len(te.rawResult) > 0 {
				raw = te.rawResult
			} else {
				raw, err = json.Marshal(te.result)
			}
		} else if len(te.rawResult) > 0 {
			raw = te.rawResult
		} else {
			raw, err = json.Marshal(te.result)
		}
		if err != nil {
			return out, err
		}
		out.Result = raw
	}
	return out, nil
}

// serializeDagResult encodes a DagResult to JSON. It is an internal helper
// (not part of the replay path: the DAG re-executes register + reads
// per-task checkpoints on replay - see DAG_SPEC_GO.md §7/§14 - so this is
// NOT wired to the aggregate-checkpoint/large-payload offload yet, hence
// unexported until it is).
func serializeDagResult(r *DagResult) ([]byte, error) {
	sr := serializedDagResult{CompletionReason: r.reason, Summary: r.summary}
	for _, te := range r.tasks {
		ste, err := serializeTaskExecution(te)
		if err != nil {
			return nil, err
		}
		sr.Tasks = append(sr.Tasks, ste)
	}
	return json.Marshal(sr)
}

// restoreDagResult decodes a DagResult from JSON, recursively restoring
// nested DAG results. Plain/batch results are kept as raw JSON and typed
// lazily by Result[T] (batch results rely on BatchResult.UnmarshalJSON).
// Internal helper - see serializeDagResult's doc for why it is unexported.
func restoreDagResult(data []byte) (*DagResult, error) {
	var sr serializedDagResult
	if err := json.Unmarshal(data, &sr); err != nil {
		return nil, err
	}
	execs := make([]TaskExecution, 0, len(sr.Tasks))
	for _, ste := range sr.Tasks {
		te := TaskExecution{
			Name:       ste.Name,
			Status:     ste.Status,
			SkipReason: ste.SkipReason,
			kind:       ste.Kind,
		}
		if ste.StartedAt != nil {
			te.StartedAt = *ste.StartedAt
		}
		if ste.CompletedAt != nil {
			te.CompletedAt = *ste.CompletedAt
		}
		if ste.Err != nil {
			te.Err = &replayedError{errType: ste.Err.Type, message: ste.Err.Message}
		}
		if ste.Status == StatusSucceeded && len(ste.Result) > 0 {
			if ste.Kind == kindDag {
				sub, err := restoreDagResult(ste.Result)
				if err != nil {
					return nil, err
				}
				te.result = sub // in-memory; Result[*DagResult] returns directly
			} else {
				te.rawResult = ste.Result // lazy typing by Result[T]
			}
		}
		execs = append(execs, te)
	}
	return newDagResult(execs, sr.CompletionReason), nil
}
