// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring examples/wait-for-callback-go's
// own handler.go/handler_test.go split.
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// NestedCallbackEvent is this example's input shape.
type NestedCallbackEvent struct {
	RequestID string `json:"requestId"`
}

// NestedCallbackResult is this example's output shape.
type NestedCallbackResult struct {
	RequestID     string `json:"requestId"`
	ParentResult  string `json:"parentResult"`
	ChildResult   string `json:"childResult"`
	ChildFinished bool   `json:"childFinished"`
}

// handler demonstrates operations.WaitForCallback composed with
// operations.RunInChildContext - both a callback at the TOP level and a
// second, independent callback INSIDE a child context, each with their
// own isolated checkpoint namespace. Mirrors the JS reference SDK's own
// wait-for-callback/child-context and wait-for-callback/nested examples
// (consolidated into one Go example, since both demonstrate the same
// underlying composition - WaitForCallback nested inside
// RunInChildContext - at different nesting depths; this Go SDK has no
// distinct mechanism the JS examples' extra nesting level would exercise
// that a single child-context level doesn't already cover).
func handler(event NestedCallbackEvent, dc types.DurableContext) (NestedCallbackResult, error) {
	parentResult, err := operations.WaitForCallback[string](dc, "parent-callback",
		func(sc types.StepContext, callbackID string) error {
			sc.Logger().Info("registered parent callback", map[string]any{"callbackId": callbackID})
			return nil
		},
	)
	if err != nil {
		return NestedCallbackResult{}, err
	}

	childResult, err := operations.RunInChildContext(dc, "child-context-with-callback", func(child types.DurableContext) (string, error) {
		if err := operations.Wait(child, "child-wait", types.Duration{Seconds: 1}); err != nil {
			return "", err
		}

		return operations.WaitForCallback[string](child, "child-callback",
			func(sc types.StepContext, callbackID string) error {
				sc.Logger().Info("registered child callback", map[string]any{"callbackId": callbackID})
				return nil
			},
		)
	})
	if err != nil {
		return NestedCallbackResult{}, err
	}

	return NestedCallbackResult{
		RequestID:     event.RequestID,
		ParentResult:  parentResult,
		ChildResult:   childResult,
		ChildFinished: true,
	}, nil
}
