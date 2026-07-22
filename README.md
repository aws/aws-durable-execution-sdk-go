# AWS Durable Execution SDK for Go

The AWS Durable Execution SDK for Go lets you write AWS Lambda handlers whose
progress is automatically checkpointed after each operation. When a Lambda
invocation is paused, times out, or restarts, the SDK replays the function from
its checkpoint log using deterministic re-execution, skipping completed
operations and resuming exactly where it left off. This gives you durable steps,
waits, callbacks, and parallel fan-out without managing state machines or
external orchestrators.

> **Pre-release.** APIs are not stable and may change without notice. Do not
> depend on this module for production workloads yet.

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

Requires Go 1.25.12 or later.

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

## Local Testing

The `durable/durabletest` package provides an in-memory test runner that executes durable handlers without network access or AWS credentials.

```go
runner := durabletest.NewLocalRunner(handler)
result := runner.RunUntilComplete(t, input)

val, err := durabletest.ResultAs[MyOutput](result)
```

Key testing utilities:

- `NewLocalRunner` - Create a local runner from a durable handler function.
- `RunUntilComplete` - Loop invocations with automatic time advancement until terminal.
- `AdvanceTime` - Advance pending wait and step-retry timers.
- `SendCallbackSuccess` - Resolve a pending callback with a success payload.
- `SendCallbackFailure` - Resolve a pending callback with an error.
- `SendCallbackHeartbeat` - Extend a pending callback timeout.
- `ResultAs` - Deserialize the execution result into a typed value.

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
