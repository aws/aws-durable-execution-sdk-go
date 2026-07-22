// Command map_op_serde implements conformance requirement 9-19: Map with an
// operation-level (whole-result) serdes — serialize on a fresh operation.
package main

import (
	"fmt"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// opSerdes serializes the whole BatchResult as "OPSERDE:X,Y".
type opSerdes struct{}

func (opSerdes) Marshal(v any) ([]byte, error) {
	br, ok := v.(durable.BatchResult[string])
	if !ok {
		return nil, fmt.Errorf("opSerdes: unexpected type %T", v)
	}
	return []byte("OPSERDE:" + strings.Join(br.Results(), ",")), nil
}

func (opSerdes) Unmarshal(data []byte, v any) error {
	s := string(data)
	if !strings.HasPrefix(s, "OPSERDE:") {
		return fmt.Errorf("opSerdes: unexpected format %q", s)
	}
	vals := strings.Split(strings.TrimPrefix(s, "OPSERDE:"), ",")
	items := make([]durable.BatchItem[string], len(vals))
	for i, val := range vals {
		items[i] = durable.BatchItem[string]{
			Index:  i,
			Status: durable.BatchItemSucceeded,
			Result: val,
		}
	}
	ptr := v.(*durable.BatchResult[string])
	*ptr = durable.BatchResult[string]{Items: items, Reason: durable.CompletionAllCompleted}
	return nil
}

func handler(ctx durable.Context, _ any) ([]string, error) {
	items := []string{"x", "y"}
	result, err := durable.Map(ctx, "op-serde", items, func(_ durable.Context, item string, _ int) (string, error) {
		return strings.ToUpper(item), nil
	}, durable.WithMaxConcurrency(1), durable.WithBatchResultSerdes(opSerdes{}))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler)
}
