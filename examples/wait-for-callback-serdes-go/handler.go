// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring examples/wait-for-callback-go's
// own handler.go/handler_test.go split.
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// TaskEvent is this example's input shape.
type TaskEvent struct {
	RequestID string `json:"requestId"`
}

// TaskResult is this example's output shape.
type TaskResult struct {
	RequestID       string `json:"requestId"`
	Message         string `json:"message"`
	CompletedAtDate bool   `json:"completedAtIsDate"`
}

// completionData is the custom struct this example's callback result
// deserializes into - includes a time.Time field, which encoding/json's
// own DEFAULT (de)serialization handles correctly already (unlike the JS
// reference SDK's own Date-losing-its-prototype problem, which motivates
// its wait-for-callback/serdes example) - this example's OWN custom
// serdes instead demonstrates a genuinely non-default wire format
// (uppercased JSON keys), to have a real, verifiable difference from the
// SDK's own default JSON serdes.
type completionData struct {
	Message     string    `json:"message"`
	CompletedAt time.Time `json:"completedAt"`
}

// upperKeySerdes is this example's own custom, non-identity serdes for
// the callback result: serializes completionData with UPPERCASE JSON
// keys (a real, verifiable, non-default wire format), and reverses that
// encoding on deserialize.
type upperKeySerdes struct{}

type upperKeyWire struct {
	MESSAGE     string    `json:"MESSAGE"`
	COMPLETEDAT time.Time `json:"COMPLETEDAT"`
}

func (upperKeySerdes) Serialize(value any, entityID string, executionARN string) (string, error) {
	data, ok := value.(completionData)
	if !ok {
		return "", fmt.Errorf("upperKeySerdes.Serialize: expected completionData, got %T", value)
	}
	b, err := json.Marshal(upperKeyWire{MESSAGE: data.Message, COMPLETEDAT: data.CompletedAt})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (upperKeySerdes) Deserialize(pointer string, entityID string, executionARN string) (any, error) {
	var wire upperKeyWire
	if err := json.Unmarshal([]byte(pointer), &wire); err != nil {
		return nil, err
	}
	return completionData{Message: wire.MESSAGE, CompletedAt: wire.COMPLETEDAT}, nil
}

// handler demonstrates operations.WithWaitForCallbackSerdes: a custom
// serializer/deserializer for the callback's own result, verified here
// by checking the checkpointed CALLBACK operation's own payload is
// genuinely in the custom UPPERCASE-key wire format, not the SDK's
// default JSON encoding. Mirrors the JS reference SDK's own
// wait-for-callback/serdes example ("Demonstrates waitForCallback with
// custom serialization/deserialization"), simplified to avoid that
// example's own Date-prototype-loss motivation (encoding/json's default
// handling of time.Time already round-trips correctly in Go, unlike JS's
// JSON.stringify/parse losing Date's prototype - so this example's own
// custom serdes demonstrates a genuinely different, verifiable wire
// format instead, to have a real reason to exist).
func handler(event TaskEvent, dc types.DurableContext) (TaskResult, error) {
	result, err := operations.WaitForCallback[completionData](dc, "custom-serdes-callback",
		func(sc types.StepContext, callbackID string) error {
			return nil
		},
		operations.WithWaitForCallbackSerdes[completionData](upperKeySerdes{}),
	)
	if err != nil {
		return TaskResult{}, err
	}

	return TaskResult{
		RequestID:       event.RequestID,
		Message:         result.Message,
		CompletedAtDate: !result.CompletedAt.IsZero(),
	}, nil
}
