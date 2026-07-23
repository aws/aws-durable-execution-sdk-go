package durable

import (
	"time"

	dcontext "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/context"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// CurrentTime returns a replay-safe "current time" for dc, backed by the
// execution's checkpointed start time rather than a live call to
// time.Now(). This is the root Execution operation's checkpoint timestamp
// (recorded once, at the execution's true first invocation), so every
// replay of the same execution observes the same value.
//
// # Determinism
//
// Code that runs between durable operations (i.e. outside an
// operations.Step callback) is re-executed on every replay. Any
// non-deterministic value computed there - wall-clock time, random
// numbers, UUIDs, unordered map iteration, direct API/DB calls - will
// differ between the original execution and its replays, which breaks the
// checkpoint/replay contract (the SDK expects replayed code paths to
// reproduce prior decisions exactly).
//
// Use CurrentTime instead of time.Now() for any timestamp read outside a
// Step. Non-deterministic values that must be read fresh (e.g. an actual
// wall-clock timestamp for business logic, a random ID) should be computed
// inside an operations.Step so they are checkpointed once and reused
// verbatim on replay.
//
// The bool return reports whether a checkpointed time value was available;
// it is false only if dc was not created by this SDK's runtime, or the
// root Execution operation has not yet been checkpointed (should not
// happen for a dc handed to a handler by WithDurableExecution, since the
// root operation is seeded from the invocation input before the handler
// runs).
func CurrentTime(dc types.DurableContext) (time.Time, bool) {
	c, ok := dc.(*dcontext.Context)
	if !ok {
		return time.Time{}, false
	}

	op, found := c.ExecManager().RootExecutionOperation()
	if !found || op.StartTimestamp == nil {
		return time.Time{}, false
	}
	return op.StartTimestamp.Time, true
}
