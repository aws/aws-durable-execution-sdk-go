# AWS Durable Execution SDK for Go

[![Build](https://github.com/aws/aws-durable-execution-sdk-go/actions/workflows/build.yml/badge.svg)](https://github.com/aws/aws-durable-execution-sdk-go/actions/workflows/build.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/aws/aws-durable-execution-sdk-go.svg)](https://pkg.go.dev/github.com/aws/aws-durable-execution-sdk-go)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go](https://img.shields.io/badge/go-%3E%3D1.24-brightgreen)](https://go.dev/)

---

Build **resilient, long-running AWS Lambda functions** in Go with automatic state persistence, retry logic, and workflow orchestration. Lambda durable functions can run for up to one year while maintaining reliable progress through checkpoints and automatic failure recovery.

> This is the `golang` branch of the repository (the `main` branch is an unstarted template for a future C#/.NET SDK — see the repo name). The SDK's operation set is modeled on the [Java SDK](https://github.com/aws/aws-durable-execution-sdk-java), expressed in idiomatic Go.

## ✨ Key Features

- **Durable Execution** — Automatically persists state and resumes from checkpoints after failures, via `durable.WithDurableExecution`
- **Automatic Retries** — Configurable retry strategies (`utils.Presets`, `utils.CreateRetryStrategy`) with exponential backoff and jitter
- **Workflow Orchestration** — Compose complex workflows with `Step`, `RunInChildContext`, `Map`, and `Parallel` (including a FLAT/virtual-context nesting mode for reduced checkpoint overhead)
- **External Integration** — `CreateCallback`/`WaitForCallback` for external systems (human-in-the-loop approvals, webhooks), with configurable timeouts, heartbeats, and custom serdes
- **Conditional Polling** — `WaitForCondition` to poll an external system until it reports done, suspending (no compute charges) between polls
- **Chained Invocation** — `Invoke` another Lambda function (durable or standard) and wait for its result
- **Batch Operations** — `Map`/`Parallel` with concurrency control, completion policies (`MinSuccessful`, tolerated failure count/percentage), and Promise-style combinators (`All`, `AllSettled`, `Any`, `Race`)
- **Cost Efficient** — Pay only for active compute time; waits and callbacks suspend without charges
- **Replay-Safe Logging** — A `DurableContext`/`StepContext`-scoped logger that suppresses duplicate log lines on replay by default
- **Local and Cloud Testing** — `pkg/durable/testing`'s `LocalTestRunner` (with time-skipping) and `CloudTestRunner` (against real deployed functions)
- **Instrumentation Plugins** *(experimental)* — `Config.Plugins` accepts lifecycle-hook plugins (`pkg/durable/plugin`) for observability/tracing integrations (OpenTelemetry, X-Ray, custom metrics): invocation/operation/attempt start-end hooks, `Wrap*` function-wrapping hooks, operation-change notifications, and log-context enrichment — a full port of the JS reference SDK's own experimental plugin system

## 🚀 Quick Start

### Installation

```bash
go get github.com/aws/aws-durable-execution-sdk-go
```

### Your First Durable Function

```go
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/awssdk"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

type OrderEvent struct {
	OrderID string `json:"orderId"`
}

type OrderResult struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
}

func handler(event OrderEvent, dc types.DurableContext) (OrderResult, error) {
	dc.Logger().Info("starting workflow", map[string]any{"orderId": event.OrderID})

	// Execute a durable step with automatic retry.
	reservation, err := operations.Step(dc, "reserve-inventory", func(sc types.StepContext) (string, error) {
		return reserveInventory(event.OrderID)
	})
	if err != nil {
		return OrderResult{}, err
	}

	// Wait for 5 seconds — no compute charges while suspended.
	if err := operations.Wait(dc, "await-confirmation", types.Duration{Seconds: 5}); err != nil {
		return OrderResult{}, err
	}

	// Process the reservation in another step.
	shipment, err := operations.Step(dc, "confirm-shipment", func(sc types.StepContext) (string, error) {
		return confirmShipment(reservation)
	})
	if err != nil {
		return OrderResult{}, err
	}

	return OrderResult{OrderID: event.OrderID, Status: shipment}, nil
}

// durableEntry is the actual Lambda entry point, wrapping handler with
// the checkpoint/replay runtime. awssdk.New resolves AWS credentials and
// region from the Lambda execution role's own standard credential chain.
func main() {
	awsClient, err := awssdk.New(context.Background())
	if err != nil {
		log.Fatalf("failed to construct awssdk.Client: %v", err)
	}
	durableEntry := durable.WithDurableExecution(handler, &durable.Config{Client: awsClient})
	// ... wire durableEntry into the Lambda Runtime API — see
	// examples/simple-step-go/main.go for a complete, working example.
}
```

See [`examples/`](examples/) for 26 complete, deployable examples (one Go module each, with local and real-cloud tests) covering every operation above, including `Step` retries, `Wait`, `CreateCallback`/`WaitForCallback` (timeouts, heartbeats, submitter retries, custom serdes, nesting), `WaitForCondition`, `RunInChildContext`, `Invoke`, `Map`/`Parallel` (completion policies, FLAT nesting), Promise combinators, custom logging, and custom `Config`.

### Invoking Your Durable Function

Durable functions require a **qualified identifier** for invocation — a version or alias, never an unqualified ARN or `$LATEST` bare name, to ensure deterministic replay behavior:

```bash
aws lambda invoke \
  --function-name my-durable-function:1 \
  --invocation-type Event \
  --cli-binary-format raw-in-base64-out \
  --payload '{"orderId": "12345"}' \
  response.json
```

> [!TIP]
> Asynchronous invocation (`--invocation-type Event`) queues the event and returns immediately, enabling executions that can run for up to one year. See [Invoking durable Lambda functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-invoking.html) for details.

## 🧪 Testing

`pkg/durable/testing` provides both a local, in-memory test runner and a cloud test runner against real deployed functions:

```go
runner := testing.New(handler, nil) // SkipTime defaults to true — fast-forwards Wait/retry delays

result, err := runner.Run(OrderEvent{OrderID: "test-1"})
if err != nil {
	t.Fatalf("Run: %v", err)
}

if result.GetStatus() != types.ExecutionStatusSucceeded {
	t.Fatalf("expected SUCCEEDED, got %s", result.GetStatus())
}

step, _ := result.GetOperation("reserve-inventory")
if step.GetStatus() != types.OperationStatusSucceeded {
	t.Fatalf("expected reserve-inventory to succeed")
}
```

`testing.CloudTestRunner` drives the same assertions against a real, already-deployed Lambda function via `GetDurableExecutionHistory` polling — see any `examples/*/cloud_integration_test.go` for a complete pattern (gated behind the `cloudintegration` build tag, since it makes real, billed AWS calls).

## 📁 Repository Layout

- [`pkg/durable/`](pkg/durable/) — the SDK itself: `operations` (Step, Wait, Map, Parallel, callbacks, Invoke, WaitForCondition), `types`, `testing`, `utils` (retry/serdes helpers), `awssdk`/`awscli` (checkpoint clients), `plugin` (experimental instrumentation-plugin hooks)
- [`insight/`](insight/) — Workflow Insight, an experimental observability plugin (its own Go module, since its exporters depend on AWS SDK clients — S3, DynamoDB, etc. — the core SDK has no reason to pull in), with all 13 of the JS reference SDK's own documented exporters ported (LambdaLog, CloudWatchLogs, S3, DynamoDB, Aurora, Redshift, OpenSearch, Firehose, EventBridge, SQS, OTel, Http, File)
- [`examples/`](examples/) — 26 standalone, deployable example Lambda functions, one Go module each, demonstrating every SDK operation
- [`conformance/`](conformance/) — a per-requirement Lambda handler harness validating this SDK against the language-neutral, cross-SDK conformance test suite
- [`docs/`](docs/) — design docs, the cross-SDK checkpoint/replay research this runtime is based on, and session-by-session progress tracking

## 📚 Documentation

- **[AWS Documentation](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)** – Official AWS Lambda durable functions guide
- **[`docs/go-sdk-design.md`](docs/go-sdk-design.md)** – This SDK's own scope and design
- **[`docs/checkpoint-replay-design.md`](docs/checkpoint-replay-design.md)** – Cross-SDK research the runtime is based on
- **[`docs/remaining-work.md`](docs/remaining-work.md)** – Feature-by-feature status against the other SDKs
- **[`docs/ts-sdk-examples-comparison.md`](docs/ts-sdk-examples-comparison.md)** – Example coverage compared against the JS reference SDK

## Related SDKs

* [JavaScript/TypeScript SDK](https://github.com/aws/aws-durable-execution-sdk-js)
* [Python SDK](https://github.com/aws/aws-durable-execution-sdk-python)
* [Java SDK](https://github.com/aws/aws-durable-execution-sdk-java)

## Security

See [CONTRIBUTING](CONTRIBUTING.md) for information about reporting security issues.

## License

This project is licensed under the Apache-2.0 License.
