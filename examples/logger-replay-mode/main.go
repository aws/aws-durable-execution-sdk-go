// Command logger-replay-mode demonstrates choosing what happens to log
// records written while the handler replays: [durable.WithReplayLogMode]
// sets it for every execution of the function, and [durable.ConfigureLogging]
// changes it for one execution from inside the handler.
//
// The handler logs, waits, and logs again. The wait ends the first
// invocation, and the second invocation runs the handler from its start, so
// the line before the wait is written again while the context replays. By
// default, [durable.ReplayLogModeSuppress], that replayed line is dropped
// and every line appears once. Under [durable.ReplayLogModeEmit] it is
// written again with the attribute replay=true, which shows what the
// replayed code did when diagnosing a replay problem.
//
// The REPLAY_LOGS environment variable selects the function-wide mode, and
// the event's replayLogs field overrides it for one execution. Both accept
// "emit" or "suppress"; any other value, including none, is
// [durable.ReplayLogModeUnchanged]. WithReplayLogMode treats that as the
// default, and ConfigureLogging treats it as "keep the mode in effect", so
// an event without the field leaves the function-wide mode alone.
package main

import (
	"os"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input optionally overrides the replay log mode for this execution.
type Input struct {
	ReplayLogs string `json:"replayLogs"`
}

// parseReplayLogMode maps a setting to a mode. Unknown and empty settings
// map to ReplayLogModeUnchanged.
func parseReplayLogMode(s string) durable.ReplayLogMode {
	switch s {
	case "emit":
		return durable.ReplayLogModeEmit
	case "suppress":
		return durable.ReplayLogModeSuppress
	default:
		return durable.ReplayLogModeUnchanged
	}
}

func handler(ctx durable.Context, in Input) (string, error) {
	// The setting lasts for the current invocation only, so it is applied
	// before the first operation, where every invocation passes. It claims
	// no operation and writes no checkpoint, so it may differ between
	// executions without affecting replay.
	if err := durable.ConfigureLogging(ctx, durable.LogConfig{
		ReplayLogMode: parseReplayLogMode(in.ReplayLogs),
	}); err != nil {
		return "", err
	}

	// Written live on the first invocation and replayed on the second.
	ctx.Logger().Info("before wait")

	if err := durable.Wait(ctx, "pause", 1*time.Second); err != nil {
		return "", err
	}

	// Claiming this step, which has no checkpoint yet, ends the replay, so
	// the records from here on are live on the second invocation.
	_, err := durable.Step(ctx, "after-wait", func(sc durable.StepContext) (string, error) {
		sc.Logger().Info("inside step")
		return "ok", nil
	})
	if err != nil {
		return "", err
	}
	ctx.Logger().Info("after step")

	return "done", nil
}

func main() {
	durable.Start(handler,
		durable.WithReplayLogMode(parseReplayLogMode(os.Getenv("REPLAY_LOGS"))))
}
