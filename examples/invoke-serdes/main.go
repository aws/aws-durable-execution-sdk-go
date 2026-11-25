// Command invoke-serdes demonstrates per-invoke serializers set with
// [durable.WithInvokePayloadSerdes] and [durable.WithInvokeResultSerdes].
// The payload serdes encodes the input sent to the invoke-simple-target
// function; the result serdes decodes what that function returns. Both are
// built with [durable.SerdesOf] on [durable.JSONSerdes] and rewrite one
// field, so the effect of each is visible in the handler's result:
//
//   - requestSerdes stamps Message on the outgoing request. The target
//     echoes the input it received, so the stamp comes back in
//     Result.Input. Default encoding would send Message empty.
//   - resultSerdes uppercases Status. The target reports "completed", so
//     "COMPLETED" shows the result was decoded by resultSerdes.
//
// The options apply to this invoke only. Without them an invoke encodes
// its input and decodes its result with the handler-level serdes, which
// defaults to JSON (see serde-custom-config for [durable.WithSerdes]).
package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input carries the target function name and the order to process.
type Input struct {
	TargetFunction string `json:"targetFunction"`
	OrderID        string `json:"orderId"`
}

// StageRequest is sent to the target function.
type StageRequest struct {
	OrderID string `json:"orderId"`
	Stage   string `json:"stage"`
	Message string `json:"message,omitempty"`
}

// StageResult is returned by the target function. Input is the request
// as the target decoded it.
type StageResult struct {
	OrderID string          `json:"orderId"`
	Stage   string          `json:"stage"`
	Status  string          `json:"status"`
	Input   json.RawMessage `json:"input"`
}

// requestStamp is the Message requestSerdes writes into every request.
const requestStamp = "encoded by requestSerdes"

// requestSerdes encodes the invoke's input. The SDK encodes the input when
// it starts the invoke and never decodes it, so Unmarshal only delegates to
// durable.JSONSerdes.
var requestSerdes = durable.SerdesOf(
	func(ctx context.Context, meta durable.SerdesContext, r StageRequest) ([]byte, error) {
		r.Message = requestStamp
		return durable.JSONSerdes.Marshal(ctx, meta, r)
	},
	func(ctx context.Context, meta durable.SerdesContext, data []byte) (StageRequest, error) {
		var r StageRequest
		return r, durable.JSONSerdes.Unmarshal(ctx, meta, data, &r)
	},
)

// resultSerdes decodes the invoke's result. The target function produces
// the result bytes and the SDK decodes them, on the invocation that
// observes the result and again on every replay; Marshal only delegates
// to durable.JSONSerdes.
var resultSerdes = durable.SerdesOf(
	func(ctx context.Context, meta durable.SerdesContext, r StageResult) ([]byte, error) {
		return durable.JSONSerdes.Marshal(ctx, meta, r)
	},
	func(ctx context.Context, meta durable.SerdesContext, data []byte) (StageResult, error) {
		var r StageResult
		if err := durable.JSONSerdes.Unmarshal(ctx, meta, data, &r); err != nil {
			return r, err
		}
		r.Status = strings.ToUpper(r.Status)
		return r, nil
	},
)

func handler(ctx durable.Context, event Input) (StageResult, error) {
	targetFunction := event.TargetFunction
	if targetFunction == "" {
		prefix := os.Getenv("FUNCTION_NAME_PREFIX")
		if prefix == "" {
			prefix = "v2-"
		}
		targetFunction = prefix + "go-invoke-simple-target:$LATEST"
	}

	orderID := event.OrderID
	if orderID == "" {
		orderID = "ORD-DEFAULT"
	}

	return durable.Invoke[StageResult](ctx, "validate", targetFunction,
		StageRequest{OrderID: orderID, Stage: "validate"},
		durable.WithInvokePayloadSerdes(requestSerdes),
		durable.WithInvokeResultSerdes(resultSerdes),
	)
}

func main() { durable.Start(handler) }
