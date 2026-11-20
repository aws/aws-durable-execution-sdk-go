// Command custom-entry-point demonstrates [durable.Wrap], which adapts a
// durable handler into a raw payload function for a program that composes
// its own Lambda entry point. [durable.Start] is Wrap followed by the
// registration this program does itself; composing the entry point lets
// the program run code around every invocation, here a middleware that
// logs how long each invocation took.
//
// The function Wrap returns takes and returns the invocation payload as
// bytes. It is registered through aws-lambda-go's raw byte interface,
// [lambda.Handler], whose Invoke method receives the payload unmodified.
// Passing the function to lambda.Start directly would not work: the
// reflective handler path JSON-decodes the payload into the function's
// []byte parameter and expects base64 text, while the durable invocation
// payload is a JSON object.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input is the order the handler confirms.
type Input struct {
	OrderID string `json:"orderId"`
}

// Output is the handler result.
type Output struct {
	Message string `json:"message"`
}

func handler(ctx durable.Context, in Input) (Output, error) {
	msg, err := durable.Step(ctx, "confirm", func(_ durable.StepContext) (string, error) {
		return "confirmed order " + in.OrderID, nil
	})
	if err != nil {
		return Output{}, err
	}
	return Output{Message: msg}, nil
}

// timedEntryPoint is the program's Lambda entry point. It implements
// lambda.Handler, so the runtime passes it each invocation payload byte
// for byte, and it hands the payload to next, the function durable.Wrap
// returned, unchanged. Around the call it logs the invocation's duration
// and outcome.
//
// An invocation here is one Lambda invocation of the durable execution,
// not the whole execution: an execution that suspends, for example on a
// wait, is invoked again later and logs a line for each invocation.
type timedEntryPoint struct {
	next   func(context.Context, []byte) ([]byte, error)
	logger *slog.Logger
}

func (e timedEntryPoint) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	start := time.Now()
	response, err := e.next(ctx, payload)

	attrs := []any{
		"durationMs", time.Since(start).Milliseconds(),
		"payloadBytes", len(payload),
		"responseBytes", len(response),
	}
	if lc, ok := lambdacontext.FromContext(ctx); ok {
		attrs = append(attrs, "requestId", lc.AwsRequestID)
	}
	if err != nil {
		e.logger.ErrorContext(ctx, "invocation failed", append(attrs, "error", err)...)
	} else {
		e.logger.InfoContext(ctx, "invocation finished", attrs...)
	}
	return response, err
}

var _ lambda.Handler = timedEntryPoint{}

// newEntryPoint wraps handler with durable.Wrap and puts the timing
// middleware around it.
func newEntryPoint(logger *slog.Logger) timedEntryPoint {
	return timedEntryPoint{next: durable.Wrap(handler), logger: logger}
}

func main() {
	lambda.Start(newEntryPoint(slog.New(slog.NewJSONHandler(os.Stderr, nil))))
}
