// Command step_complex_object implements conformance requirement 1-4: a
// step returning a nested object with arrays and mixed types.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

type input struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

type user struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

type response struct {
	User  user `json:"user"`
	Count int  `json:"count"`
}

func handler(ctx durable.Context, event input) (response, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (response, error) {
		return response{
			User:  user{Name: event.Name, Tags: event.Tags},
			Count: len(event.Tags),
		}, nil
	})
}

func main() {
	durable.Start(handler)
}
