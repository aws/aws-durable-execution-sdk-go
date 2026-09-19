# AWS Durable Execution SDK for Go

The AWS Durable Execution SDK for Go lets you write AWS Lambda handlers whose
progress is automatically checkpointed after each operation. When a Lambda
invocation is paused, times out, or restarts, the SDK replays the function from
its checkpoint log using deterministic re-execution, skipping completed
operations and resuming exactly where it left off. This gives you durable steps,
waits, callbacks, and parallel fan-out without managing state machines or
external orchestrators.

> **Pre-release.**
>
> Do not use this code for production purposes. Do not rely on this code for
> anything whatsoever.
>
> This code is experimental and liable to change without notice. Any aspect of
> this code that works today can stop working at any time. The one exception
> is the plugin instrumentation API, which follows the compatibility policy
> in the [Plugin API](#plugin-api) section below.
>
> This code is a preview of what a Go SDK might look like. There is no
> guarantee that it will necessarily become a final product.

## Quick Start

Install the SDK:

```console
go get github.com/aws/aws-durable-execution-sdk-go
```

Create a durable Lambda handler:

```go
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type OrderEvent struct {
	OrderID string `json:"orderId"`
	Message string `json:"message"`
}

type OrderResult struct {
	OrderID  string `json:"orderId"`
	Greeting string `json:"greeting"`
}

func handler(ctx durable.Context, event OrderEvent) (OrderResult, error) {
	greeting, err := durable.Step(ctx, "create-greeting", func(sc durable.StepContext) (string, error) {
		return fmt.Sprintf("Hello, %s!", event.Message), nil
	})
	if err != nil {
		return OrderResult{}, err
	}
	return OrderResult{OrderID: event.OrderID, Greeting: greeting}, nil
}

func main() {
	durable.Start(handler)
}
```

## Setup

Requires Go 1.25 or later.

Build a static binary for deployment to AWS Lambda (`provided.al2023` runtime):

```console
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bootstrap
```

The `examples/` directory contains a working SAM template and build script for deploying durable functions to Lambda.

## Documentation

- [AWS Durable Execution Documentation](https://docs.aws.amazon.com/durable-execution/) - Concepts, getting started, core operations, advanced topics, and API reference
- [AWS Lambda Durable Functions Guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html) - How durable functions work on Lambda

## Operations

The `durable` package provides the following operations:

| Operation | Description |
| --- | --- |
| `Step` | Run a function with automatic checkpointing and configurable retry. |
| `StepAsync` | Asynchronous Step returning a `*Future`. |
| `Wait` | Pause execution for a duration without blocking Lambda. |
| `WaitAsync` | Asynchronous Wait returning a `*Future`. |
| `Invoke` | Call another durable function and wait for its result. |
| `InvokeAsync` | Asynchronous Invoke returning a `*Future`. |
| `RunInChildContext` | Execute a subflow in an isolated child context. |
| `RunInChildContextAsync` | Asynchronous RunInChildContext returning a `*Future`. |
| `Go` | Launch a replay-safe concurrent subflow (shorthand for `RunInChildContextAsync`). |
| `WaitForCondition` | Poll a check function until a condition is met or a strategy stops. |
| `CreateCallback` | Register a callback and wait for external resolution. |
| `WaitForCallback` | Create a callback, invoke a submitter with the callback ID, and wait. |
| `Map` | Fan out a function over items with configurable concurrency and completion. |
| `Parallel` | Run named branches concurrently with configurable completion. |
| `All` | Wait for all futures to succeed. Returns results or the first error. |
| `AllSettled` | Wait for all futures to settle. Returns all outcomes. |
| `Any` | Return the first future to succeed. Errors if all fail. |
| `Race` | Return the result of the first future to settle. |
| `Join` | Wait for futures of different result types to settle. Returns the first error in argument order. |
| `Select` | Run named branches concurrently; return the first to settle along with its name. |
| `Retry` | Retry a function containing durable operations as a unit, suspending between attempts. |

## A complete example

The handler below is one order-processing workflow that uses every operation
above. Helper functions such as `reserveInventory` stand in for application
code. To compile it yourself, define the event types (`OrderEvent`,
`LineItem`, `OrderResult`) and the application helpers it calls. The handler
is compile-verified against the SDK, and the test in the next section runs it
end to end.

```go
func handler(ctx durable.Context, event OrderEvent) (OrderResult, error) {
	// Step: run a function once, checkpoint the result, skip it on replay.
	orderID, err := durable.Step(ctx, "validate-order", func(sc durable.StepContext) (string, error) {
		if len(event.Items) == 0 {
			return "", fmt.Errorf("order %s has no items", event.OrderID)
		}
		return event.OrderID, nil
	})
	if err != nil {
		return OrderResult{}, err
	}

	// Parallel: run named branches concurrently. The default completion
	// policy is fail-fast: a failed branch is returned as a *BatchError
	// together with the partial result.
	_, err = durable.Parallel(ctx, "pre-flight", []durable.Branch[string]{
		{Name: "payment", Func: func(c durable.Context) (string, error) {
			return authorizePayment(orderID)
		}},
		{Name: "fraud", Func: func(c durable.Context) (string, error) {
			return screenForFraud(orderID)
		}},
	})
	if err != nil {
		return OrderResult{}, err
	}

	// Map: fan a function out over the line items with bounded concurrency.
	_, err = durable.Map(ctx, "reserve-items", event.Items,
		func(c durable.Context, item LineItem, index int) (string, error) {
			return reserveInventory(item.SKU, item.Quantity)
		},
		durable.WithMaxConcurrency(3))
	if err != nil {
		return OrderResult{}, err
	}

	// RunInChildContext: group the fulfillment phase in its own context.
	tracking, err := durable.RunInChildContext(ctx, "fulfillment", func(c durable.Context) (string, error) {
		// Invoke: call another durable function and wait for its result.
		label, err := durable.Invoke[string](c, "print-label", "shipping-labels-function", orderID)
		if err != nil {
			return "", err
		}

		// WaitForCallback: hand a callback ID to an external system and
		// suspend until that system resolves it through the callback API.
		return durable.WaitForCallback[string](c, "warehouse-pick",
			func(sc durable.StepContext, callbackID string) error {
				return notifyWarehouse(callbackID, label)
			})
	})
	if err != nil {
		return OrderResult{}, err
	}

	// durable.Go: replay-safe concurrent subflows returning futures.
	email := durable.Go(ctx, "email", func(c durable.Context) (string, error) {
		return sendEmail(orderID)
	})
	sms := durable.Go(ctx, "sms", func(c durable.Context) (string, error) {
		return sendSMS(orderID)
	})

	// All: join the futures. AllSettled, Any, and Race take the same shape.
	if _, err := durable.All(ctx, "notify", []*durable.Future[string]{email, sms}); err != nil {
		return OrderResult{}, err
	}

	// WaitForCondition: poll a check function until the condition is met.
	status, err := durable.WaitForCondition(ctx, "await-delivery",
		func(sc durable.StepContext, state string) (string, error) {
			return carrierStatus(tracking)
		},
		durable.ConditionConfig[string]{
			InitialState: "IN_TRANSIT",
			WaitStrategy: func(state string, attempt int) durable.WaitDecision {
				if state == "DELIVERED" {
					return durable.WaitDecision{Continue: false}
				}
				return durable.WaitDecision{Continue: true, Delay: 15 * time.Minute}
			},
		})
	if err != nil {
		return OrderResult{}, err
	}

	// Wait: suspend for a duration without holding compute.
	if err := durable.Wait(ctx, "settlement-delay", 24*time.Hour); err != nil {
		return OrderResult{}, err
	}

	// Step: capture the payment after the settlement delay.
	if _, err := durable.Step(ctx, "capture-payment", func(sc durable.StepContext) (string, error) {
		return capturePayment(orderID)
	}); err != nil {
		return OrderResult{}, err
	}

	return OrderResult{OrderID: orderID, Status: status}, nil
}

func main() {
	durable.Start(handler)
}
```

`Step`, `Wait`, `Invoke`, and `RunInChildContext` also have Async variants
(`StepAsync`, `WaitAsync`, `InvokeAsync`, `RunInChildContextAsync`) that
return a `*Future` immediately, the same pattern `durable.Go` shows above.

## Determinism

Replay pairs stored results with operations by position, so a handler must
create its durable operations in the same order on every invocation. Two
rules follow from this:

- Code between operations must depend only on the handler input and on
  results returned by earlier operations. Anything nondeterministic (random
  values, the current time, network calls) belongs inside a `Step` body,
  whose result is checkpointed once and replayed afterwards.
- A `durable.Context` is owned by the goroutine it was created on. Invoking
  a durable operation on it from any other goroutine (a `go` statement, an
  `errgroup.Go` callback) fails with `durable.ErrWrongGoroutine`. Use
  `durable.Go` to run durable work concurrently; it gives the new goroutine
  a child context of its own. The check runs in every default build.

The package documentation for `durable` covers both rules in detail under
"Determinism" and "Goroutine Ownership".

## Testing

The `durable/durabletest` package provides an in-memory test runner that
executes durable handlers without network access or AWS credentials. The test
below runs the complete example above end to end. `RunUntilComplete` invokes
the handler repeatedly, advancing waits and retries automatically, and returns
when the execution finishes or blocks on external action. The test resolves
the chained invoke and the warehouse callback the way the service would.

```go
func TestOrderWorkflow(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)

	event := OrderEvent{
		OrderID: "order-42",
		Items:   []LineItem{{SKU: "widget", Quantity: 2}},
	}

	// Run until the workflow suspends on the chained invoke.
	result := runner.RunUntilComplete(t, event)

	// Resolve the invoked shipping-labels function.
	if err := runner.CompleteChainedInvoke("print-label", "label-7"); err != nil {
		t.Fatal(err)
	}
	result = runner.RunUntilComplete(t, event)

	// Resolve the warehouse callback the workflow is now blocked on.
	cb := runner.OpenCallbacks()[0]
	if err := runner.SendCallbackSuccess(cb.CallbackID, "picked"); err != nil {
		t.Fatal(err)
	}
	result = runner.RunUntilComplete(t, event)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	out, err := durabletest.ResultAs[OrderResult](result)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != "DELIVERED" {
		t.Fatalf("order status = %s, want DELIVERED", out.Status)
	}
}
```

Run it with the standard toolchain:

```console
go test ./...
```

Other runner utilities: `CompletePendingTimers` completes pending wait and
step-retry timers, `SendCallbackFailure` resolves a callback with an error,
and `SendCallbackHeartbeat` extends a callback timeout.

For testing against a deployed function, use `NewCloudRunner`:

```go
runner := durabletest.NewCloudRunner(api, functionName)
result := runner.Run(t, event)
```

## Logging

`Context.Logger()` and `StepContext.Logger()` return a `*slog.Logger`. By
default it writes one JSON object per record to stderr with the field names
the other durable execution SDKs use, so one CloudWatch query works across
languages:

| Field | Present |
| --- | --- |
| `timestamp`, `level`, `message` | Always. |
| `requestId`, `executionArn` | Always. |
| `tenantId` | When the invocation has one. |
| `operationId`, `operationName` | Inside a child context (`RunInChildContext`, `Go`, `Map`, `Parallel`, `WaitForCallback`) or an operation body. `operationName` only when the operation is named. |
| `attempt` | Inside a step body, condition check, or callback submitter. |
| `replay` | Only on a record emitted while its context replays, and only under `ReplayLogModeEmit` (see below). Always `true` when present. |

A field that does not apply in a scope is omitted, never emitted empty.
Each scope carries its own identifiers only: a step inside a child context
reports the step, not the child.

To use your own logging library, pass its `slog.Handler` to
`WithLogHandler`. The SDK attaches the fields above through the handler's
`WithAttrs` method as structured attributes, wraps the handler with replay
suppression, and adds the fields a plugin returns from
`Plugin.EnrichLogContext` as record attributes. Plugin fields never
overwrite the SDK's fields or the attributes you pass; keys are compared by
qualified path, so a plugin field under an open `slog` group collides only
with your attributes at that same path. See the `EnrichLogContext`
documentation for the full precedence.

To choose the handler from inside the handler body, for example from the
event payload, call `ConfigureLogging`. It replaces the handler for the
rest of the invocation, on the calling context and on every child context
and branch derived from it after the call; the SDK attaches the same fields
to the new handler. The change lasts for the current invocation only and
does not affect checkpoints or operation ordering, so it is safe to call
conditionally.

```go
if event.Debug {
	if err := durable.ConfigureLogging(ctx, durable.LogConfig{
		Handler: slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}),
	}); err != nil {
		return OrderResult{}, err
	}
}
```

### Replayed log records

While a context replays checkpointed operations, its log records are
dropped, so replayed code does not duplicate the lines it wrote when it
first ran. Suppression is decided per branch: a `Go` branch that is still
replaying stays quiet while a sibling that has reached live execution logs
normally.

To see the records of the replayed portion when diagnosing a replay
problem, select `ReplayLogModeEmit` with `WithReplayLogMode` at
construction or with `ConfigureLogging` inside the handler. Replayed
records are then emitted with the field `replay` set to `true`; live
records carry no `replay` field. Expect duplicate lines: every line written
before a suspension appears again on each later invocation that replays
it. `ReplayLogModeSuppress` is the default. The top-level `replay` key
belongs to the SDK in every mode: a value you attach under that name with
`Logger.With` or pass with a record is dropped, while the same name inside a
group opened with `WithGroup` is kept.

```go
durable.Start(handler, durable.WithReplayLogMode(durable.ReplayLogModeEmit))
```

## Plugin API

The plugin instrumentation API gives observability and tracing integrations
hooks into the lifecycle of an execution. A `durable.Plugin` is a struct of
optional hook functions; set the ones you need and register the plugin with
`durable.WithPlugins`.

```go
tracer := durable.Plugin{
	OnOperationStart: func(ctx context.Context, info durable.OperationHookInfo) {
		log.Printf("start %s %s replay=%v", info.Type, info.Name, info.IsReplay)
	},
	WrapOperationAttemptFn: func(ctx context.Context, info durable.AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
		ctx, span := startSpan(ctx, info.Name)
		defer span.End()
		return fn(ctx)
	},
}
durable.Start(handler, durable.WithPlugins(tracer))
```

The hooks cover the invocation (`OnInvocationStart`, `OnInvocationEnd`,
`OnOperationChange`, `WrapInvocation`), each operation (`OnOperationStart`,
`OnOperationEnd`), each attempt of a retryable operation
(`OnOperationAttemptStart`, `OnOperationAttemptEnd`,
`WrapOperationAttemptFn`), child contexts (`WrapChildContextFn`), and log
records (`EnrichLogContext`). The wrap hooks receive the wrapped work as a
function and may pass it a derived context, which becomes the parent of the
context the user code observes. `WithPluginChildOperationsDepth` bounds how
deep in the operation tree hooks are reported.

The `durable.Plugin` documentation states, for every hook, when it fires,
whether it fires on replay, its order relative to the other hooks, and the
goroutine that dispatches it. With one plugin registered a notification hook
runs on that goroutine; with several, each plugin's hook runs on a goroutine
the dispatch joins, so the plugins' hooks for one event run in parallel.
Hooks of concurrent operations run in parallel too, so a plugin must be
safe for concurrent use. The
[insight](insight/README.md) module is a complete plugin built on this API.

### Compatibility policy

The plugin API is stable. Within a major version of the module, a release
may add hook fields to `Plugin`, add fields to the hook info types, and add
status or outcome constants. A release does not remove or rename an
exported identifier of the plugin API, change a hook's signature, or change
the documented dispatch semantics; a change of that kind requires a new
major version. Construct `Plugin` and the hook info types with keyed fields
so that added fields do not break your code, and tolerate status values you
do not know. Each addition is listed in
[docs/release-notes.md](docs/release-notes.md). The policy holds from the
first release that carries it, including releases at v0. The pre-release
notice at the top of this file states that any part of the SDK can change
without notice; this API is the exception it names. The notice's warning
against production use still applies to the SDK as a whole.

## Feedback & Support

- [Bug report](https://github.com/aws/aws-durable-execution-sdk-go/issues/new)
- [Feature request](https://github.com/aws/aws-durable-execution-sdk-go/issues/new)

## Contributing

We welcome contributions and feedback. Please open an issue before submitting a
pull request so we can discuss the change. See [CONTRIBUTING.md](CONTRIBUTING.md)
for guidelines.

## License

Apache-2.0. See [LICENSE](LICENSE).
