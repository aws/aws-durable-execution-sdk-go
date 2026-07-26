// Command child-context-serdes-virtual demonstrates
// [durable.RunInChildContextAsync] with a custom [durable.WithChildSerdes]
// option. The async child context (virtual context) runs concurrently and
// returns a [*durable.Future] whose result is serialized through the
// custom serdes, verifying that the serdes round-trip works on the
// asynchronous child context path.
package main

import (
	"encoding/json"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// uppercaseSerdes uppercases on marshal and passes through on unmarshal.
type uppercaseSerdes struct{}

func (uppercaseSerdes) Marshal(_ durable.SerdesContext, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return []byte(strings.ToUpper(string(b))), nil
}

func (uppercaseSerdes) Unmarshal(_ durable.SerdesContext, data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func handler(ctx durable.Context, _ any) (string, error) {
	f := durable.RunInChildContextAsync(ctx, "serdes-virtual-child",
		func(child durable.Context) (string, error) {
			return durable.Step(child, "inner-step", func(_ durable.StepContext) (string, error) {
				return "hello from virtual", nil
			})
		},
		durable.WithChildSerdes(uppercaseSerdes{}))

	return f.Result()
}

func main() { durable.Start(handler) }
