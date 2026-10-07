// Command serde-retryable-error demonstrates [durable.RetryableSerdesError],
// which a custom serdes returns to mark a failure as transient. A serdes
// error is permanent by default: the operation fails and is never retried.
// A RetryableSerdesError instead ends only the current invocation. The
// operation records no outcome, and the service invokes the execution
// again from its last checkpoint, so the operation runs again once the
// cause has cleared.
//
// The "charge" step uses archiveSerdes, which copies the step's result to
// an archive before the result is checkpointed, so that every checkpointed
// receipt has an archived copy. An archive that is temporarily unavailable
// is a transient cause: archiveSerdes wraps that error with
// RetryableSerdesError. An archive that rejects the record cannot accept
// it on a later attempt either, so that error is returned unchanged and
// fails the step. The handler recognises the transient case with
// errors.Is and [durable.ErrRetryableSerdes]. It can log it, but it cannot
// recover from it: the invocation ends with the error whatever the
// handler returns.
//
// The deployed function archives to its own log, which does not fail, so
// the default event succeeds. The tests replace the archive to show the
// transient and the permanent failure.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

var (
	// errArchiveUnavailable is a transient archive failure, such as a
	// timeout or throttling.
	errArchiveUnavailable = errors.New("archive unavailable")

	// errArchiveRejected is a permanent archive failure, such as a record
	// the archive refuses to store.
	errArchiveRejected = errors.New("archive rejected the record")
)

// archive stores a copy of a checkpointed result under a key.
type archive interface {
	Put(ctx context.Context, key string, data []byte) error
}

// logArchive stands in for an external store such as an S3 bucket. It
// writes each record to the function's log, which does not fail. A real
// store fails transiently, with errArchiveUnavailable, or permanently, with
// errArchiveRejected.
type logArchive struct{}

func (logArchive) Put(_ context.Context, key string, data []byte) error {
	slog.Info("archived step result", "key", key, "record", string(data))
	return nil
}

// store is the archive archiveSerdes writes to.
var store archive = logArchive{}

// Receipt is the result of the "charge" step.
type Receipt struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
}

// archiveSerdes encodes a Receipt as JSON and archives the encoded record
// before the SDK checkpoints it. The key combines the execution ARN and
// the operation ID, so each step result has its own record.
var archiveSerdes = durable.SerdesOf(
	func(ctx context.Context, meta durable.SerdesContext, r Receipt) ([]byte, error) {
		data, err := json.Marshal(r)
		if err != nil {
			return nil, err
		}
		key := meta.DurableExecutionArn + "/" + meta.OperationID
		if err := store.Put(ctx, key, data); err != nil {
			if errors.Is(err, errArchiveUnavailable) {
				// Transient: end this invocation and record nothing, so
				// the next invocation runs the step again.
				return nil, durable.RetryableSerdesError(err)
			}
			// Permanent: the step fails and is not retried.
			return nil, err
		}
		return data, nil
	},
	func(_ context.Context, _ durable.SerdesContext, data []byte) (Receipt, error) {
		var r Receipt
		return r, json.Unmarshal(data, &r)
	},
)

// Input is the order to charge.
type Input struct {
	OrderID string `json:"orderId"`
}

func handler(ctx durable.Context, in Input) (Receipt, error) {
	reserved, err := durable.Step(ctx, "reserve", func(_ durable.StepContext) (string, error) {
		return "reserved " + in.OrderID, nil
	})
	if err != nil {
		return Receipt{}, err
	}

	receipt, err := durable.Step(ctx, "charge", func(_ durable.StepContext) (Receipt, error) {
		return Receipt{OrderID: in.OrderID, Status: "charged after " + reserved}, nil
	}, durable.WithStepSerdes(archiveSerdes))
	if err != nil {
		if errors.Is(err, durable.ErrRetryableSerdes) {
			// The invocation ends with this error whatever the handler
			// returns. "reserve" is checkpointed, so the next invocation
			// replays it and runs "charge" again.
			ctx.Logger().Warn("archive unavailable, charge resumes in a new invocation", "error", err)
		}
		return Receipt{}, fmt.Errorf("charge: %w", err)
	}
	return receipt, nil
}

func main() { durable.Start(handler) }
