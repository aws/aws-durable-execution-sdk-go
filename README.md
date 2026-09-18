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
> this code that works today can stop working at any time.
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

Requires Go 1.24 or later.

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

## Plugin API

The plugin instrumentation API (`WithPlugins`) is EXPERIMENTAL. It provides lifecycle hooks for observability and tracing. The API may change in future releases.

## Feedback & Support

- [Bug report](https://github.com/aws/aws-durable-execution-sdk-go/issues/new)
- [Feature request](https://github.com/aws/aws-durable-execution-sdk-go/issues/new)

## Contributing

We welcome contributions and feedback. Please open an issue before submitting a
pull request so we can discuss the change. See [CONTRIBUTING.md](CONTRIBUTING.md)
for guidelines.

## License

Apache-2.0. See [LICENSE](LICENSE).
