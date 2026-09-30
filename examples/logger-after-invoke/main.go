// Command logger-after-invoke demonstrates replay-aware logging across an
// Invoke's suspend/resume boundary.
//
// The first invocation logs "before-invoke", starts the invoke-simple-target
// companion, and suspends. The service invokes the handler again when the
// target completes. That invocation replays the handler from the start. It
// suppresses "before-invoke", because the first invocation already wrote
// that line. Invoke then returns the target's checkpointed result. No
// earlier invocation ran the code after the Invoke, so the context leaves
// replay as Invoke returns. So "after-invoke" is logged once, in the second
// invocation, and ctx.IsReplaying() reports false on that line.
//
// The handler counts the "after-invoke" records its log handler receives in
// the current invocation and returns the count. It also returns
// ctx.IsReplaying() as read at the start of the handler and right before
// the line. The start value is true on the invocation that returns, which
// shows that the line ran in a resumed invocation.
//
// The target function name comes from the event, falling back to the
// FUNCTION_NAME_PREFIX environment variable for cloud deployments, as in
// invoke-simple.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync/atomic"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// lineMessage is the message of the line logged right after the Invoke.
const lineMessage = "after-invoke"

// Input optionally names the target function.
type Input struct {
	FunctionName string `json:"functionName"`
}

// targetInput is the payload sent to the target function.
type targetInput struct {
	Message string `json:"message"`
}

// result is the handler output.
type result struct {
	Message string          `json:"message"`
	Target  json.RawMessage `json:"target"`
	// Logged is the number of lineMessage records the log handler
	// received in the invocation that returned this result.
	Logged int64 `json:"logged"`
	// ReplayingAtLine is ctx.IsReplaying() right before the line.
	ReplayingAtLine bool `json:"replayingAtLine"`
	// ResumedInReplay is ctx.IsReplaying() at the start of the handler.
	ResumedInReplay bool `json:"resumedInReplay"`
}

func handler(ctx durable.Context, event Input) (result, error) {
	// A new counter for every invocation: Lambda reuses the process, so
	// package state would carry counts across invocations.
	counter := newCountingHandler(slog.NewJSONHandler(os.Stderr, nil), lineMessage)
	if err := durable.ConfigureLogging(ctx, durable.LogConfig{Handler: counter}); err != nil {
		return result{}, err
	}
	resumedInReplay := ctx.IsReplaying()

	functionName := event.FunctionName
	if functionName == "" {
		prefix := os.Getenv("FUNCTION_NAME_PREFIX")
		if prefix == "" {
			prefix = "v2-"
		}
		functionName = prefix + "go-invoke-simple-target:$LATEST"
	}

	ctx.Logger().Info("before-invoke")
	target, err := durable.Invoke[json.RawMessage](ctx, "invoke", functionName, targetInput{Message: "hello"})
	if err != nil {
		return result{}, err
	}

	replayingAtLine := ctx.IsReplaying()
	ctx.Logger().Info(lineMessage)

	return result{
		Message:         "done",
		Target:          target,
		Logged:          counter.count.Load(),
		ReplayingAtLine: replayingAtLine,
		ResumedInReplay: resumedInReplay,
	}, nil
}

// countingHandler forwards every record to next and counts the records
// whose message is msg. The SDK derives handlers from it with WithAttrs
// and WithGroup; every derived handler shares the same counter.
type countingHandler struct {
	next  slog.Handler
	msg   string
	count *atomic.Int64
}

func newCountingHandler(next slog.Handler, msg string) countingHandler {
	return countingHandler{next: next, msg: msg, count: new(atomic.Int64)}
}

func (h countingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h countingHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == h.msg {
		h.count.Add(1)
	}
	return h.next.Handle(ctx, r)
}

func (h countingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return countingHandler{next: h.next.WithAttrs(attrs), msg: h.msg, count: h.count}
}

func (h countingHandler) WithGroup(name string) slog.Handler {
	return countingHandler{next: h.next.WithGroup(name), msg: h.msg, count: h.count}
}

func main() { durable.Start(handler) }
