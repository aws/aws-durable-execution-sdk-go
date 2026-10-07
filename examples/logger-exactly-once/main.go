// Command logger-exactly-once demonstrates that every log line is written
// exactly once over a durable execution, in the invocation that first runs
// it.
//
// The handler logs a line after each kind of awaited outcome: a Wait, a
// Future.Result, a Callback.Result, WaitForCallback, the All combinator,
// Map, Parallel, and a Go branch that reads a future its parent created
// and then its own. It also logs lines between starting an asynchronous
// operation and reading its result. Each await ends an invocation, so the
// next invocation replays the code before it and drops those lines.
//
// The invocation that resumes after the wait "crash-pause" logs the line
// "crash-line" and then exits the process, once. The service invokes the
// function again. The SDK cannot know which lines the crashed invocation
// wrote, so "crash-line" is written a second time. Every other line is
// written once.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// sendCallback completes a callback with value. The local test replaces
// it, because it has no Lambda API to call.
var sendCallback = completeCallback

// crash ends the process. The local test replaces it, because the local
// runner does not retry a crashed invocation.
var crash = defaultCrash

func defaultCrash() { os.Exit(1) }

// lines lists every line the handler writes, in program order.
var lines = []string{
	"start", "after-wait", "between-callback", "between-wait-async",
	"after-wait-async", "after-callback", "after-wait-for-cb", "after-all",
	"branch-after-parent", "branch-after-own", "after-branch", "after-map",
	"after-parallel", "crash-line", "after-crash",
}

// processToken identifies this process. The crash gate step records the
// token of the process that ran it, and only that process crashes.
var processToken = newToken()

func handler(ctx durable.Context, _ any) (string, error) {
	log := func(c durable.Context, msg string) { c.Logger().Info(msg) }
	pause := func(c durable.Context, name string) error { return durable.Wait(c, name, time.Second) }

	if _, err := durable.Step(ctx, "fetch", func(durable.StepContext) (string, error) { return "x", nil }); err != nil {
		return "", err
	}
	log(ctx, "start")

	// A line after a Wait.
	if err := pause(ctx, "pause"); err != nil {
		return "", err
	}
	log(ctx, "after-wait")

	// Lines between starting an asynchronous operation and reading its
	// result, and lines after Future.Result and Callback.Result.
	cb, err := durable.CreateCallback[string](ctx, "approval", durable.WithCallbackTimeout(time.Minute))
	if err != nil {
		return "", err
	}
	log(ctx, "between-callback")
	if _, err := durable.Step(ctx, "send", func(sc durable.StepContext) (string, error) {
		return "sent", sendCallback(sc, cb.ID(), "approved")
	}, durable.WithRetry(durable.NoRetry())); err != nil {
		return "", err
	}
	wait := durable.WaitAsync(ctx, "pause-async", time.Second)
	log(ctx, "between-wait-async")
	if _, err := wait.Result(ctx); err != nil {
		return "", err
	}
	log(ctx, "after-wait-async")
	if _, err := cb.Result(ctx); err != nil {
		return "", err
	}
	log(ctx, "after-callback")

	// A line after WaitForCallback.
	if _, err := durable.WaitForCallback[string](ctx, "approval-2",
		func(sc durable.StepContext, callbackID string) error {
			return sendCallback(sc, callbackID, "approved")
		},
		durable.WithCallbackTimeout(time.Minute)); err != nil {
		return "", err
	}
	log(ctx, "after-wait-for-cb")

	// A line after a combinator.
	waits := []*durable.Future[durable.Void]{
		durable.WaitAsync(ctx, "all-a", time.Second),
		durable.WaitAsync(ctx, "all-b", time.Second),
	}
	if _, err := durable.All(ctx, "all", waits); err != nil {
		return "", err
	}
	log(ctx, "after-all")

	// Lines in a Go branch after it reads a future its parent created,
	// and after it reads its own future. Result takes the branch's
	// context in both cases.
	parentWait := durable.WaitAsync(ctx, "parent-pause", time.Second)
	branch := durable.Go(ctx, "branch", func(c durable.Context) (int, error) {
		if _, err := durable.Step(c, "pre", func(durable.StepContext) (int, error) { return 1, nil }); err != nil {
			return 0, err
		}
		if _, err := parentWait.Result(c); err != nil {
			return 0, err
		}
		log(c, "branch-after-parent")
		if _, err := durable.WaitAsync(c, "own-pause", time.Second).Result(c); err != nil {
			return 0, err
		}
		log(c, "branch-after-own")
		return 1, nil
	})
	if _, err := branch.Result(ctx); err != nil {
		return "", err
	}
	log(ctx, "after-branch")

	// Lines after Map and Parallel.
	if _, err := durable.Map(ctx, "map", []int{1, 2}, func(c durable.Context, item, _ int) (int, error) {
		return item, pause(c, "item-pause")
	}); err != nil {
		return "", err
	}
	log(ctx, "after-map")
	branchPause := func(c durable.Context) (int, error) { return 1, pause(c, "branch-pause") }
	if _, err := durable.Parallel(ctx, "parallel", []durable.Branch[int]{
		{Name: "a", Func: branchPause},
		{Name: "b", Func: branchPause},
	}); err != nil {
		return "", err
	}
	log(ctx, "after-parallel")

	// The invocation that resumes after this wait writes "crash-line"
	// and exits, once. The gate step records the token of the process
	// that ran it. The invocation that replays the step runs in a new
	// process, so its token differs and it does not exit.
	if err := pause(ctx, "crash-pause"); err != nil {
		return "", err
	}
	log(ctx, "crash-line")
	gate, err := durable.Step(ctx, "crash-gate", func(durable.StepContext) (string, error) { return processToken, nil })
	if err != nil {
		return "", err
	}
	if gate == processToken {
		crash()
	}
	log(ctx, "after-crash")
	return "done", nil
}

func completeCallback(ctx context.Context, callbackID, value string) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = lambdasvc.NewFromConfig(cfg).SendDurableExecutionCallbackSuccess(ctx,
		&lambdasvc.SendDurableExecutionCallbackSuccessInput{CallbackId: &callbackID, Result: payload})
	return err
}

func newToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func main() { durable.Start(handler) }
