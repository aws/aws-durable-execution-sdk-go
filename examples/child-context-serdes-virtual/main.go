// Command child-context-serdes-virtual demonstrates
// [durable.RunInChildContextAsync] with [durable.WithChildVirtual] and a
// custom [durable.WithChildSerdes]. A virtual child context is never
// checkpointed, so nothing is stored for its result; the SDK still passes
// the result through the child's serdes on every run, so the value the
// caller sees is the same live and on replay. The uppercasing serdes below
// makes that round trip visible: the handler returns the uppercased value
// although the step inside produced lowercase.
package main

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// uppercaseSerdes uppercases on marshal and passes through on unmarshal.
type uppercaseSerdes struct{}

func (uppercaseSerdes) Marshal(_ context.Context, _ durable.SerdesContext, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return []byte(strings.ToUpper(string(b))), nil
}

func (uppercaseSerdes) Unmarshal(_ context.Context, _ durable.SerdesContext, data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func handler(ctx durable.Context, _ any) (string, error) {
	f := durable.RunInChildContextAsync(ctx, "serdes-virtual-child",
		func(child durable.Context) (string, error) {
			return durable.Step(child, "inner-step", func(_ durable.StepContext) (string, error) {
				return "hello from virtual", nil
			})
		},
		durable.WithChildVirtual(),
		durable.WithChildSerdes(uppercaseSerdes{}))

	return f.Result()
}

func main() { durable.Start(handler) }
