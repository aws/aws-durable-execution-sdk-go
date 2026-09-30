// Command logger-after-future demonstrates replay-aware logging after
// Future.Result and Callback.Result.
//
// The handler creates a callback, completes it from a step, and starts a
// five second wait with WaitAsync. The first invocation suspends at the
// wait's Result. The second invocation replays the callback, the step, and
// the wait. No earlier invocation ran the code after the wait's Result, so
// the context leaves replay as Result returns. So "after-wait" and
// "after-callback" are each logged once, in the second invocation, and
// ctx.IsReplaying() reports false on both lines.
//
// The handler counts the records its log handler receives in the current
// invocation and returns the counts. It also returns ctx.IsReplaying() as
// read at the start of the handler, which is true on the invocation that
// returns, and right before the first line.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

const (
	afterWait     = "after-wait"
	afterCallback = "after-callback"
)

// result is the handler output.
type result struct {
	Message string `json:"message"`
	// LoggedAfterWait and LoggedAfterCallback are the number of records
	// with each message that the log handler received in the invocation
	// that returned this result.
	LoggedAfterWait     int `json:"loggedAfterWait"`
	LoggedAfterCallback int `json:"loggedAfterCallback"`
	// ReplayingAtLine is ctx.IsReplaying() right before "after-wait".
	ReplayingAtLine bool `json:"replayingAtLine"`
	// ResumedInReplay is ctx.IsReplaying() at the start of the handler.
	ResumedInReplay bool `json:"resumedInReplay"`
}

// sendCallback completes the callback. The local test replaces it, because
// it has no Lambda API to call.
var sendCallback = completeCallback

func handler(ctx durable.Context, _ any) (result, error) {
	// A new counter for every invocation: Lambda reuses the process, so
	// package state would carry counts across invocations.
	counter := newCountingHandler(slog.NewJSONHandler(os.Stderr, nil))
	if err := durable.ConfigureLogging(ctx, durable.LogConfig{Handler: counter}); err != nil {
		return result{}, err
	}
	resumedInReplay := ctx.IsReplaying()

	cb, err := durable.CreateCallback[string](ctx, "approval", durable.WithCallbackTimeout(time.Minute))
	if err != nil {
		return result{}, err
	}
	if _, err := durable.Step(ctx, "send", func(sc durable.StepContext) (string, error) {
		return "sent", sendCallback(sc, cb.ID(), "approved")
	}, durable.WithRetry(durable.NoRetry())); err != nil {
		return result{}, err
	}
	wait := durable.WaitAsync(ctx, "pause", 5*time.Second)

	if _, err := wait.Result(); err != nil {
		return result{}, err
	}
	replayingAtLine := ctx.IsReplaying()
	ctx.Logger().Info(afterWait)

	if _, err := cb.Result(); err != nil {
		return result{}, err
	}
	ctx.Logger().Info(afterCallback)

	return result{
		Message:             "done",
		LoggedAfterWait:     counter.count(afterWait),
		LoggedAfterCallback: counter.count(afterCallback),
		ReplayingAtLine:     replayingAtLine,
		ResumedInReplay:     resumedInReplay,
	}, nil
}

func completeCallback(ctx context.Context, callbackID, value string) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	payload, _ := json.Marshal(value)
	_, err = lambdasvc.NewFromConfig(cfg).SendDurableExecutionCallbackSuccess(ctx,
		&lambdasvc.SendDurableExecutionCallbackSuccessInput{CallbackId: &callbackID, Result: payload})
	return err
}

// countingHandler forwards every record to next and counts records by
// message. The SDK derives handlers from it with WithAttrs and WithGroup;
// every derived handler shares the same counts.
type countingHandler struct {
	next   slog.Handler
	counts *counts
}

type counts struct {
	mu sync.Mutex
	n  map[string]int
}

func newCountingHandler(next slog.Handler) countingHandler {
	return countingHandler{next: next, counts: &counts{n: map[string]int{}}}
}

func (h countingHandler) count(msg string) int {
	h.counts.mu.Lock()
	defer h.counts.mu.Unlock()
	return h.counts.n[msg]
}

func (h countingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h countingHandler) Handle(ctx context.Context, r slog.Record) error {
	h.counts.mu.Lock()
	h.counts.n[r.Message]++
	h.counts.mu.Unlock()
	return h.next.Handle(ctx, r)
}

func (h countingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return countingHandler{next: h.next.WithAttrs(attrs), counts: h.counts}
}

func (h countingHandler) WithGroup(name string) slog.Handler {
	return countingHandler{next: h.next.WithGroup(name), counts: h.counts}
}

func main() { durable.Start(handler) }
