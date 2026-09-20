# AWS Durable Execution SDK for Go

A durable function is a Lambda function whose progress the service
checkpoints as it runs. The SDK records the result of each operation when the
operation completes. An invocation ends when the handler suspends on a timer,
on an external signal, or on the function timeout. The service then invokes
the function again. On that invocation the SDK replays the recorded results
instead of running the completed work again, and the handler continues from
the first operation that has no recorded result. So an orchestration that
spans minutes or a month fits in one ordinary Go function.

> [!WARNING]
> This is an experimental preview, not intended for production use. The API
> may change without notice, and the final version may look different from
> what you see today.

The plugin instrumentation API is the one exception to that notice. It
follows the compatibility policy in the [Plugin API](#plugin-api) section.

The SDK requires Go 1.24 or later.

## Your first durable function

Create a module and add the SDK to it.

```console
mkdir first-durable-function && cd first-durable-function
go mod init example.com/first-durable-function
go get github.com/aws/aws-durable-execution-sdk-go
```

The repository has no release tags. `go get` therefore records a
pseudo-version of the latest commit on `main`, such as
`v0.0.0-20260920051622-7f3b62f70004`. Run
`go get github.com/aws/aws-durable-execution-sdk-go@latest` to move to a
newer commit.

Put this in `main.go`. The handler runs a step, suspends on a two second
timer, then runs a second step that reads the first result.

```go
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	name, err := durable.Step(ctx, "fetch-name", func(_ durable.StepContext) (string, error) {
		return "world", nil
	})
	if err != nil {
		return "", err
	}
	if err := durable.Wait(ctx, "cooldown", 2*time.Second); err != nil {
		return "", err
	}
	return durable.Step(ctx, "format", func(_ durable.StepContext) (string, error) {
		return fmt.Sprintf("hello, %s", name), nil
	})
}

func main() {
	durable.Start(handler)
}
```

Build a static Linux binary named `bootstrap`. That is the file the
`provided.al2023` runtime executes.

```console
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o dist/bootstrap .
```

Point a SAM template at the `dist` directory and give the function a
`DurableConfig`. The `DurableConfig` is what makes the service checkpoint
the function.

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Description: First durable function

Parameters:
  ExecutionRoleArn:
    Type: String
    Description: IAM role the function assumes

Resources:
  FirstDurableFunction:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: first-durable-function
      CodeUri: dist/
      Handler: bootstrap
      Runtime: provided.al2023
      Architectures: [x86_64]
      Timeout: 60
      MemorySize: 128
      Role: !Ref ExecutionRoleArn
      DurableConfig:
        RetentionPeriodInDays: 7
        ExecutionTimeout: 300
```

The template takes an existing execution role. The role needs permission to
write CloudWatch Logs, and it needs the two Lambda API actions the SDK
calls, `lambda:CheckpointDurableExecution` and
`lambda:GetDurableExecutionState`.

```console
sam deploy --template-file template.yaml \
  --stack-name first-durable-function \
  --resolve-s3 --region us-west-2 \
  --parameter-overrides ExecutionRoleArn=arn:aws:iam::111122223333:role/durable-lambda-role
```

Invoking a durable function starts an execution. A synchronous invoke
waits for the execution to finish and returns its result, so the call below
returns after about three seconds with `"hello, world"` in `response.json`.
The invocation metadata on stdout includes the `DurableExecutionArn`.

```console
aws lambda invoke --function-name first-durable-function \
  --qualifier '$LATEST' --payload '{}' \
  --cli-binary-format raw-in-base64-out response.json
```

A synchronous invoke is limited to 15 minutes. For a longer execution,
invoke with `--invocation-type Event`. The call then returns at once with
status code 202 and the `DurableExecutionArn`, and the execution runs for
up to its `ExecutionTimeout`. Poll it with the ARN. It reports `RUNNING`
while the timer is pending and `SUCCEEDED` with the result afterwards.

```console
aws lambda get-durable-execution \
  --durable-execution-arn <DurableExecutionArn> \
  --query '[Status,Result]' --output text
```

`get-durable-execution-history` lists the recorded events of an
execution. For the handler above it shows the first step, the wait, the
end of the first invocation, the completed wait, the second step, and the
end of the second invocation.

## The handler and the context

A durable handler is a function of two arguments that returns a value and
an error.

```go
type Order struct {
	ID string `json:"id"`
}

type Receipt struct {
	ChargeID string `json:"chargeId"`
}

func handler(ctx durable.Context, order Order) (Receipt, error) {
	return Receipt{ChargeID: "ch_" + order.ID}, nil
}
```

`durable.Start(handler)` registers the handler with the Lambda runtime and
runs one execution per invocation. The event type and the output type are
yours to choose. The SDK decodes the event and encodes the output with
`encoding/json`. `durable.Wrap` returns a raw payload function instead of
starting the runtime, for a program that composes its own Lambda entry
point.

When the handler returns an error, the execution moves to the `FAILED`
status. The service records the error's type and message, and
`get-durable-execution` returns them in its `Error` field. A synchronous
caller receives the same error object as the payload, with `FunctionError`
set to `Unhandled` in the invocation metadata. The `ErrorType` of an error
from `errors.New` or `fmt.Errorf` is `Error`. The `ErrorType` of any other
error is the name of its Go type.

`durable.Context` is the handle to the execution. It exposes `ExecutionArn()`,
`RequestID()`, `InvokedFunctionARN()`, `Logger()`, and `IsReplaying()`.
`IsReplaying` reports whether the current invocation is replaying recorded
results. Every operation takes the context as its first argument.

### Determinism

Replay pairs recorded results with operations by position. So the handler
must create the same operations in the same order on every invocation. Code
between operations must depend only on the event and on results the SDK
already recorded. Do not create operations while iterating a map, because
Go randomizes map iteration order. Sort the keys into a slice first.

Put nondeterministic work inside a step body. A step body may read the
clock, generate a random ID, or call a service. Only its recorded result
takes part in replay. `durable.StepContext`, the argument a step body
receives, is a `context.Context` with `Logger()` and `Attempt()`. It exposes
no durable operations, so a step cannot create nested operations.

### Goroutines

A `durable.Context` is owned by the goroutine that created it. Calling an
operation on it from another goroutine fails with
`durable.ErrWrongGoroutine`. That covers a `go` statement and an
`errgroup.Go` callback alike. Two goroutines claiming operations on one
context would claim them in a scheduling-dependent order, and replay would
then pair recorded results with the wrong operations. The check runs in
every default build. Build with `-tags durablenocheck` to remove it.

Use `durable.Go` to run durable work concurrently. It claims the child
operation on the calling goroutine, which keeps the order deterministic,
and then starts a goroutine that owns a fresh child context. Use the child
context inside the function, never the parent.

```go
func handler(ctx durable.Context, _ any) (string, error) {
	fut := durable.Go(ctx, "work", func(child durable.Context) (string, error) {
		return durable.Step(child, "step", func(_ durable.StepContext) (string, error) {
			return "done", nil
		})
	})
	return fut.Result()
}
```

`Future.Result` blocks until the future settles. Await several futures with
a combinator, not with a sequence of `Result` calls. When the first future
suspends, `Result` returns the suspension signal and the handler returns
before the other branches reach a checkpoint. The combinators await every
future first and propagate the suspension afterwards. The
[combinators](#combinators) section shows them.

### Errors

Return an error from an operation unchanged unless the handler treats it
as a business outcome. A non-nil error is one of two things. Either it is a
documented terminal failure of that operation, such as
`*durable.StepError`, or it is the signal that the invocation is
suspending. A terminal failure matches its public type with `errors.As`.
The suspension signal matches no public type. So an error that matches no
public type must be returned as it is.

A typed failure does not carry the error value the operation body
returned. It carries the recorded `ErrorType` and `Message`, and the SDK
rebuilds it the same way on the first invocation and on replay. So match on
`ErrorType`, not with `errors.As` against your own error types.

```go
type CardDeclinedError struct{}

func (CardDeclinedError) Error() string { return "card declined" }

func handler(ctx durable.Context, _ any) (string, error) {
	receipt, err := durable.Step(ctx, "charge", func(_ durable.StepContext) (string, error) {
		return "", CardDeclinedError{}
	}, durable.WithRetry(durable.NoRetry()))
	var stepErr *durable.StepError
	switch {
	case err == nil:
		return receipt, nil
	case errors.As(err, &stepErr) && stepErr.ErrorType == "CardDeclinedError":
		return "declined", nil
	default:
		return "", err
	}
}
```

## Operations

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
| `Select` | Run named branches concurrently. Return the first to settle along with its name. |
| `Retry` | Retry a function containing durable operations as a unit, suspending between attempts. |

Every operation takes a name as its second argument. The name identifies
the operation in the execution history. Pass `""` for an unnamed operation.

### Step

A step is the unit of checkpointing. The SDK records its result with
`encoding/json`, so the result type must round-trip through JSON. Once the
service records the result, replay returns it without running the body
again.

```go
func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "fetch-name", func(sc durable.StepContext) (string, error) {
		// Nondeterministic work belongs here. sc is a context.Context, so
		// pass it to AWS SDK calls made inside the step.
		return "world", nil
	})
}
```

Between the moment the SDK starts a step and the moment the service records
the outcome, the function can time out or the runtime can crash. Under the
default `AtLeastOncePerRetry` semantics the SDK runs the body again on
resume. So the body may run more than once for one attempt, and it should
be idempotent. `AtMostOncePerRetry` treats the interrupted attempt as a
failure instead and consults the retry strategy.

`WithRetry` sets the retry strategy. The default is `ExponentialBackoff()`,
which makes 6 attempts in total, starting 5 seconds apart, doubling each
time, capped at 60 seconds, with full jitter. `NoRetry()` fails on the first
error. `NewRetryStrategy` builds a strategy from a `RetryConfig`, and
`LinearBackoff` builds one with a constant increment. The execution suspends
for the delay between attempts, so a retrying step does not hold the
invocation open. `StepContext.Attempt()` is the 1-based attempt number.

```go
func handler(ctx durable.Context, _ any) (string, error) {
	strategy, err := durable.NewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  4,
		InitialDelay: time.Second,
		BackoffRate:  2,
	})
	if err != nil {
		return "", err
	}
	return durable.Step(ctx, "flaky-call", func(sc durable.StepContext) (string, error) {
		if sc.Attempt() < 3 {
			return "", fmt.Errorf("transient failure on attempt %d", sc.Attempt())
		}
		return fmt.Sprintf("succeeded on attempt %d", sc.Attempt()), nil
	},
		durable.WithRetry(strategy),
		durable.WithSemantics(durable.AtMostOncePerRetry),
	)
}
```

When the strategy stops, `Step` returns a `*durable.StepError` with the
attempt count and the recorded `ErrorType` and `Message` of the last
attempt.

### Wait

`Wait` suspends the execution for a duration. The invocation ends, and the
service invokes the function again when the duration elapses. On replay a
completed wait returns at once.

```go
func handler(ctx durable.Context, _ any) (string, error) {
	if err := durable.Wait(ctx, "cooldown", 60*time.Second); err != nil {
		return "", err
	}
	return "waited", nil
}
```

Return the error of `Wait` in every case. It never carries a business
outcome.

### Invoke

`Invoke` starts another durable function as its own execution and returns
its result. The calling execution suspends after starting it and resumes
when it completes. The output type parameter comes first, so it can be
written out while the input type is inferred. The target needs a version or
alias qualifier, such as `:$LATEST`. If the invoked function fails, `Invoke`
returns a `*durable.InvokeError`.

```go
func handler(ctx durable.Context, orderID string) (string, error) {
	return durable.Invoke[string](ctx, "charge", "payments-function:$LATEST", orderID)
}
```

### WaitForCondition

`WaitForCondition` runs a check repeatedly. The check receives the state
from the previous attempt and returns the new state. The SDK checkpoints
the state between attempts and suspends for the delay the wait strategy
returns. The strategy receives the new state and the attempt number. It
returns `Continue: true` with a `Delay` to poll again, `Continue: false` to
stop with the state, or an `Err` to fail the operation.

```go
func handler(ctx durable.Context, _ any) (int, error) {
	return durable.WaitForCondition(ctx, "poll-until-ready",
		func(_ durable.StepContext, state int) (int, error) {
			return state + 1, nil
		},
		durable.ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(state int, attempt int) durable.WaitDecision {
				if state >= 3 {
					return durable.WaitDecision{Continue: false}
				}
				return durable.WaitDecision{Continue: true, Delay: time.Second}
			},
		})
}
```

`NewWaitStrategy` builds a strategy from a `WaitConfig` with a
`ShouldContinue` predicate, an attempt cap, and exponential backoff. When
`WaitStrategy` is nil, the SDK polls with a 5 second initial delay
multiplied by 1.5 after each attempt, capped at 5 minutes, and fails once
60 attempts have been made. That default never reports the condition met,
so set `WaitStrategy`.

### Callbacks

`CreateCallback` creates a callback and returns it at once. The callback
exposes `ID()`, the identifier to hand to an external system, and
`Result()`, which blocks until that system completes it. The external
system completes a callback with the `SendDurableExecutionCallbackSuccess`
or `SendDurableExecutionCallbackFailure` API. The result is a JSON
document. From the CLI, the command below completes the callback with the
string `"approved"`. The callback ID also appears in the
`CallbackStarted` event of the execution history.

```console
aws lambda send-durable-execution-callback-success \
  --callback-id <ID> --result '"approved"' \
  --cli-binary-format raw-in-base64-out
```

```go
func handler(ctx durable.Context, _ any) (string, error) {
	cb, err := durable.CreateCallback[string](ctx, "approval",
		durable.WithCallbackTimeout(time.Hour))
	if err != nil {
		return "", err
	}
	ctx.Logger().Info("awaiting approval", "callbackId", cb.ID())
	return cb.Result()
}
```

`WaitForCallback` combines the two halves into one operation. It creates
the callback, runs the submitter with the callback ID, and blocks until the
result arrives. The submitter runs as a step, so it may call a service, and
`WithSubmitterRetry` retries it with a `RetryStrategy`.

```go
func handler(ctx durable.Context, _ any) (string, error) {
	return durable.WaitForCallback[string](ctx, "approval",
		func(sc durable.StepContext, callbackID string) error {
			// Hand callbackID to the approver here, for example by
			// publishing it to a queue.
			sc.Logger().Info("approval requested", "callbackId", callbackID)
			return nil
		},
		durable.WithCallbackTimeout(time.Hour))
}
```

Both operations take `WithCallbackTimeout` and
`WithCallbackHeartbeatTimeout`. A timeout fails the operation with a
`*durable.CallbackTimeoutError`. A failure sent by the external system
fails it with a `*durable.CallbackExternalError`.

### Child contexts

`RunInChildContext` runs a function with its own context. The operations
inside it are recorded under the child, and the child's overall result is
checkpointed. On replay of a completed child, the SDK returns the recorded
result without running the function.

```go
func handler(ctx durable.Context, _ any) (string, error) {
	return durable.RunInChildContext(ctx, "greet", func(child durable.Context) (string, error) {
		name, err := durable.Step(child, "fetch-name", func(_ durable.StepContext) (string, error) {
			return "world", nil
		})
		if err != nil {
			return "", err
		}
		return "hello, " + name, nil
	})
}
```

### Retry

`Retry` retries a function that contains durable operations, as one unit.
Each attempt runs in its own child context, so a failed attempt's
operations are never replayed into the next attempt. The delay between
attempts is a `Wait`, so no compute is consumed while waiting. When the
strategy stops, `Retry` returns a `*durable.RetryError` with the attempt
count and the last attempt's error. The function receives the 1-based
attempt number.

```go
func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Retry(ctx, "quote-and-book", func(c durable.Context, attempt int) (string, error) {
		quote, err := durable.Step(c, "fetch-quote", func(_ durable.StepContext) (float64, error) {
			if attempt < 2 {
				return 0, errors.New("quote service unavailable")
			}
			return 104.50, nil
		}, durable.WithRetry(durable.NoRetry()))
		if err != nil {
			return "", err
		}
		return durable.Step(c, "book", func(_ durable.StepContext) (string, error) {
			return fmt.Sprintf("booked at %.2f", quote), nil
		})
	}, durable.ExponentialBackoff())
}
```

### Map and Parallel

`Map` applies one function to every item. Each item runs in its own child
context and receives its index. `Parallel` runs a fixed set of named
branches that share an output type. Both return a `BatchResult` and accept
`WithMaxConcurrency` and `WithCompletion`.

```go
func handler(ctx durable.Context, items []string) ([]string, error) {
	results, err := durable.Map(ctx, "process-all", items,
		func(c durable.Context, item string, index int) (string, error) {
			return durable.Step(c, "process", func(_ durable.StepContext) (string, error) {
				return fmt.Sprintf("item-%d:%s", index, item), nil
			})
		},
		durable.WithMaxConcurrency(4))
	if err != nil {
		return nil, err
	}
	return results.Results(), nil
}
```

```go
func handler(ctx durable.Context, _ any) ([]int, error) {
	results, err := durable.Parallel(ctx, "fan-out", []durable.Branch[int]{
		{Name: "double", Func: func(c durable.Context) (int, error) {
			return durable.Step(c, "compute", func(_ durable.StepContext) (int, error) {
				return 42, nil
			})
		}},
		{Name: "wait-then-value", Func: func(c durable.Context) (int, error) {
			if err := durable.Wait(c, "cooldown", time.Second); err != nil {
				return 0, err
			}
			return 100, nil
		}},
	})
	if err != nil {
		return nil, err
	}
	return results.Results(), nil
}
```

The default completion policy is fail-fast. The first item failure
completes the batch, and `Map` or `Parallel` returns a
`*durable.BatchError` carrying the per-item errors. A `CompletionConfig`
changes that. `MinSuccessful` completes the batch once that many items
succeed. `ToleratedFailureCount` and `ToleratedFailurePercentage` let the
batch continue past failures up to a limit. `ShouldComplete` decides
programmatically from a `BatchProgress` snapshot. Items still running when
the batch completes early are reported with the status `BatchItemStarted`.

`BatchResult` reports each item's status through `Items`, `Succeeded()`,
`Failed()`, and `Started()`, the values through `Results()`, the per-item
errors through `Errors()`, and why the batch ended through `Reason`.

### Combinators

`StepAsync`, `WaitAsync`, `InvokeAsync`, `RunInChildContextAsync`, and `Go`
start an operation and return a `*Future`. The combinators take futures and
record the combined outcome as one operation.

`All` waits for every future to succeed and returns the values in input
order. It fails on the first error.

```go
func handler(ctx durable.Context, _ any) ([]int, error) {
	a := durable.StepAsync(ctx, "a", func(_ durable.StepContext) (int, error) { return 1, nil })
	b := durable.StepAsync(ctx, "b", func(_ durable.StepContext) (int, error) { return 2, nil })
	c := durable.StepAsync(ctx, "c", func(_ durable.StepContext) (int, error) { return 3, nil })
	return durable.All(ctx, "gather", []*durable.Future[int]{a, b, c})
}
```

`AllSettled` waits for every future and returns a `Settled` per future,
with either a `Value` or an `Err`. It never fails fast.

```go
func handler(ctx durable.Context, _ any) (int, error) {
	good := durable.StepAsync(ctx, "good", func(_ durable.StepContext) (int, error) { return 1, nil })
	bad := durable.StepAsync(ctx, "bad", func(_ durable.StepContext) (int, error) {
		return 0, errors.New("boom")
	}, durable.WithRetry(durable.NoRetry()))
	settled, err := durable.AllSettled(ctx, "collect", []*durable.Future[int]{good, bad})
	if err != nil {
		return 0, err
	}
	failures := 0
	for _, s := range settled {
		if s.Err != nil {
			failures++
		}
	}
	return failures, nil
}
```

`Any` returns the value of the first future to succeed. It fails with a
`*durable.CombinatorError` when every future fails. `Race` returns the
outcome of the first future to settle, success or failure.

```go
func handler(ctx durable.Context, _ any) (int, error) {
	primary := durable.StepAsync(ctx, "primary", func(_ durable.StepContext) (int, error) {
		return 0, errors.New("unavailable")
	}, durable.WithRetry(durable.NoRetry()))
	fallback := durable.StepAsync(ctx, "fallback", func(_ durable.StepContext) (int, error) { return 42, nil })
	return durable.Any(ctx, "first-ok", []*durable.Future[int]{primary, fallback})
}
```

`Join` takes futures of different result types through the `Awaitable`
interface. It waits for all of them and returns the first error in argument
order. Read the values afterwards with `Result`, which returns at once after
`Join` returned nil.

```go
func handler(ctx durable.Context, _ any) (string, error) {
	charge := durable.StepAsync(ctx, "charge", func(_ durable.StepContext) (string, error) {
		return "ch_123", nil
	})
	reserve := durable.Go(ctx, "reserve", func(c durable.Context) (int, error) {
		return durable.Step(c, "reserve-items", func(_ durable.StepContext) (int, error) {
			return 3, nil
		})
	})
	if err := durable.Join(ctx, "settle", []durable.Awaitable{charge, reserve}); err != nil {
		return "", err
	}
	receipt, _ := charge.Result()
	reserved, _ := reserve.Result()
	return fmt.Sprintf("%s reserved %d items", receipt, reserved), nil
}
```

`Select` runs named branches and returns the name and value of the first
branch to settle. Use it in place of `Race` when the caller must know
which branch won.

```go
func handler(ctx durable.Context, _ any) (string, error) {
	winner, quote, err := durable.Select(ctx, "quote", []durable.Branch[float64]{
		{Name: "primary", Func: func(c durable.Context) (float64, error) {
			return durable.Step(c, "primary-quote", func(_ durable.StepContext) (float64, error) {
				time.Sleep(2 * time.Second)
				return 100.00, nil
			})
		}},
		{Name: "fallback", Func: func(c durable.Context) (float64, error) {
			return durable.Step(c, "fallback-quote", func(_ durable.StepContext) (float64, error) {
				return 104.50, nil
			})
		}},
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s quoted %.2f", winner, quote), nil
}
```

## Serialization

The SDK stores operation results as JSON by default. `WithSerdes` sets a
different `durable.Serdes` for the whole handler at construction time, and
`ConfigureSerdes` changes it from inside the handler. Per-operation options
such as `WithStepSerdes`, `WithChildSerdes`, and `WithBatchSerdes` override
it for one operation. The `Serdes` interface is untyped. `Marshal` receives
the value as `any` and `Unmarshal` fills a pointer passed as `any`, so one
value can serve every result type in a handler. `SerdesOf` builds a
`Serdes` for one result type from typed marshal and unmarshal functions.

`NewFileSystemSerdes` stores payloads as files under a base path and
records a reference in the checkpoint. In the default
`FileSystemSerdesModeAlways` every value goes to a file. In
`FileSystemSerdesModeOverflow` a value goes to a file only when it exceeds
255 KB. Point the base path at a
durable mount such as Amazon EFS, not at the ephemeral `/tmp` of the
function.

```go
func handler(ctx durable.Context, _ any) (string, error) {
	upper := durable.SerdesOf(
		func(_ context.Context, _ durable.SerdesContext, v string) ([]byte, error) {
			return []byte(strings.ToUpper(v)), nil
		},
		func(_ context.Context, _ durable.SerdesContext, data []byte) (string, error) {
			return strings.ToLower(string(data)), nil
		},
	)
	return durable.Step(ctx, "shout", func(_ durable.StepContext) (string, error) {
		return "hello", nil
	}, durable.WithStepSerdes(upper))
}
```

## Testing a handler locally

The `durable/durabletest` package runs a handler in process, without
network access or AWS credentials. `RunUntilComplete` invokes the handler
as many times as the execution needs. Between invocations it completes
pending timers and step retries, and it returns when the execution
finishes or blocks on external action.

```go
func TestHandler(t *testing.T) {
	handler := func(ctx durable.Context, n int) (int, error) {
		if err := durable.Wait(ctx, "cooldown", time.Hour); err != nil {
			return 0, err
		}
		return durable.Step(ctx, "add", func(_ durable.StepContext) (int, error) {
			return n + 1, nil
		})
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, 41)

	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	got, err := durabletest.ResultAs[int](result)
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("result = %d, want 42", got)
	}
}
```

`TestResult` also lists every recorded operation in `Operations`, so a test
can assert that a step ran once across replays. `SendCallbackSuccess`,
`SendCallbackFailure`, and `SendCallbackHeartbeat` resolve a callback the
handler is blocked on. `CompleteChainedInvoke` and `FailChainedInvoke`
resolve an `Invoke`. `RegisterFunction` registers a handler for a function
name so that `Invoke` runs it in process. `NewCloudRunner` runs the same
assertions against a deployed function.

## Logging

`Context.Logger()` and `StepContext.Logger()` return a `*slog.Logger`. By
default it writes one JSON object per record to stderr with the field names
the other durable execution SDKs use, so one CloudWatch query works across
languages.

| Field | Present |
| --- | --- |
| `timestamp`, `level`, `message` | Always. |
| `requestId`, `executionArn` | Always. |
| `tenantId` | When the invocation has one. |
| `operationId`, `operationName` | Inside a child context (`RunInChildContext`, `Go`, `Map`, `Parallel`, `WaitForCallback`) or an operation body. `operationName` only when the operation is named. |
| `attempt` | Inside a step body, condition check, or callback submitter. |
| `replay` | Only on a record emitted while its context replays, and only under `ReplayLogModeEmit` (see below). Always `true` when present. |

A field that does not apply in a scope is omitted, never emitted empty.
Each scope carries its own identifiers only. A step inside a child context
reports the step, not the child.

To use your own logging library, pass its `slog.Handler` to
`WithLogHandler`. The SDK attaches the fields above through the handler's
`WithAttrs` method as structured attributes, wraps the handler with replay
suppression, and adds the fields a plugin returns from
`Plugin.EnrichLogContext` as record attributes. Plugin fields never
overwrite the SDK's fields or the attributes you pass. Keys are compared by
qualified path, so a plugin field under an open `slog` group collides only
with your attributes at that same path. See the `EnrichLogContext`
documentation for the full precedence.

To choose the handler from inside the handler body, for example from the
event payload, call `ConfigureLogging`. It replaces the handler for the
rest of the invocation, on the calling context and on every child context
and branch derived from it after the call. The SDK attaches the same fields
to the new handler. The change lasts for the current invocation only and
does not affect checkpoints or operation ordering, so it is safe to call
conditionally.

```go
type Event struct {
	Debug bool `json:"debug"`
}

func handler(ctx durable.Context, event Event) (string, error) {
	if event.Debug {
		if err := durable.ConfigureLogging(ctx, durable.LogConfig{
			Handler: slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}),
		}); err != nil {
			return "", err
		}
	}
	return "configured", nil
}
```

### Replayed log records

While a context replays checkpointed operations, its log records are
dropped, so replayed code does not duplicate the lines it wrote when it
first ran. Suppression is decided per branch. A `Go` branch that is still
replaying stays quiet while a sibling that has reached live execution logs
normally.

To see the records of the replayed portion when diagnosing a replay
problem, select `ReplayLogModeEmit` with `WithReplayLogMode` at
construction or with `ConfigureLogging` inside the handler. Replayed
records are then emitted with the field `replay` set to `true`. Live
records carry no `replay` field. Expect duplicate lines. Every line written
before a suspension appears again on each later invocation that replays
it. `ReplayLogModeSuppress` is the default. The top-level `replay` key
belongs to the SDK in every mode. A value you attach under that name with
`Logger.With` or pass with a record is dropped, while the same name inside a
group opened with `WithGroup` is kept. At construction the option is
`durable.Start(handler, durable.WithReplayLogMode(durable.ReplayLogModeEmit))`.

## Plugin API

The plugin instrumentation API gives observability and tracing integrations
hooks into the lifecycle of an execution. A `durable.Plugin` is a struct of
optional hook functions. Set the ones you need and register the plugin with
`durable.WithPlugins`.

```go
func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "work", func(_ durable.StepContext) (string, error) {
		return "done", nil
	})
}

func main() {
	tracer := durable.Plugin{
		OnOperationStart: func(_ context.Context, info durable.OperationHookInfo) {
			log.Printf("start %s %s replay=%v", info.Type, info.Name, info.IsReplay)
		},
		WrapOperationAttemptFn: func(ctx context.Context, info durable.AttemptHookInfo, fn func(context.Context) (any, error)) (any, error) {
			start := time.Now()
			defer func() { log.Printf("%s took %s", info.Name, time.Since(start)) }()
			return fn(ctx)
		},
	}
	durable.Start(handler, durable.WithPlugins(tracer))
}
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
runs on that goroutine. With several, each plugin's hook runs on a goroutine
the dispatch joins, so the plugins' hooks for one event run in parallel.
Hooks of concurrent operations run in parallel too, so a plugin must be
safe for concurrent use. The
[insight](insight/README.md) module is a complete plugin built on this API.

### Compatibility policy

The plugin API is stable. Within a major version of the module, a release
may add hook fields to `Plugin`, add fields to the hook info types, and add
status or outcome constants. A release does not remove or rename an
exported identifier of the plugin API, change a hook's signature, or change
the documented dispatch semantics. A change of that kind requires a new
major version. Construct `Plugin` and the hook info types with keyed fields
so that added fields do not break your code, and tolerate status values you
do not know. Each addition is listed in
[docs/release-notes.md](docs/release-notes.md). The policy holds from the
first release that carries it, including releases at v0. The warning at the
top of this file states that the SDK can change without notice. This API
is the exception it names. The warning against production use still applies
to the SDK as a whole.

## Examples

`examples/` holds one deployable function per pattern.
[examples/README.md](examples/README.md) lists them by operation with the
terminal status each one reaches. Each is a Lambda function on
`provided.al2023`. `examples/build.sh` builds them and the SAM template in
that directory deploys them.

## Feedback & Support

- [Bug report](https://github.com/aws/aws-durable-execution-sdk-go/issues/new)
- [Feature request](https://github.com/aws/aws-durable-execution-sdk-go/issues/new)

## Contributing

We welcome contributions and feedback. Please open an issue before submitting a
pull request so we can discuss the change. See [CONTRIBUTING.md](CONTRIBUTING.md)
for guidelines.

## Acknowledgments

Big thanks to [@embano1](https://github.com/embano1), whose insightful
advice on the early experimental versions caught the bugs before they became
bugs, and whose eagle-eyed reviews caught the subtle (and not-so-subtle!)
ones before they could bite anyone.

## License

Apache-2.0. See [LICENSE](LICENSE).
