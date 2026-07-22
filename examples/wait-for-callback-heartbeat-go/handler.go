// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring examples/wait-for-callback-go's
// own handler.go/handler_test.go split.
package main

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// HeartbeatEvent is this example's input shape.
type HeartbeatEvent struct {
	RequestID string `json:"requestId"`
}

// HeartbeatResult is this example's output shape.
type HeartbeatResult struct {
	RequestID string `json:"requestId"`
	Completed bool   `json:"completed"`
}

// handler demonstrates operations.WithWaitForCallbackHeartbeatTimeout: a
// callback that requires the external system to periodically confirm
// it's still working (rather than one fixed overall timeout), by
// checkpointing a heartbeat before that timeout would otherwise elapse.
// Mirrors the JS reference SDK's own wait-for-callback/heartbeat-sends
// example ("Demonstrates sending heartbeats during long-running callback
// processing").
//
// This Go SDK has no distinct heartbeat-SEND API surfaced on
// types.StepContext (unlike the JS reference SDK's own submitter, which
// can call an explicit heartbeat function during a long-running
// submitter) - the heartbeat TIMEOUT itself is configured here via
// WithWaitForCallbackHeartbeatTimeout exactly like
// WithWaitForCallbackTimeout, but this example's own submitter
// completes quickly (it only registers the callback), and the external
// system's own SendDurableExecutionCallbackHeartbeat calls (made
// out-of-band, against the real backend, not modeled by this handler's
// own code) are what actually keep the callback alive between the
// submitter finishing and the eventual SendDurableExecutionCallbackSuccess.
func handler(event HeartbeatEvent, dc types.DurableContext) (HeartbeatResult, error) {
	result, err := operations.WaitForCallback[string](dc, "long-running-task-callback",
		func(sc types.StepContext, callbackID string) error {
			sc.Logger().Info("registered long-running task", map[string]any{"callbackId": callbackID})
			return nil
		},
		operations.WithWaitForCallbackHeartbeatTimeout[string](types.Duration{Seconds: 30}),
	)
	if err != nil {
		return HeartbeatResult{}, err
	}

	_ = result
	return HeartbeatResult{RequestID: event.RequestID, Completed: true}, nil
}
