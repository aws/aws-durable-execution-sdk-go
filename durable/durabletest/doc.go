// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Package durabletest provides an in-memory local testing runner for durable
// handler functions. It enables fast, deterministic unit tests without
// requiring AWS credentials or network access.
//
// The package runs a real durable executor against a thin in-memory
// [durable.ExecutionClient] implementation, so handler code executes
// exactly as it would in production — including checkpoint replay,
// operation ordering, and suspension semantics.
//
// # Quick Start
//
//	runner := durabletest.NewLocalRunner(myHandler)
//	result, err := runner.RunUntilComplete(myInput)
//	if err != nil {
//	    t.Fatal(err) // the runner failed
//	}
//	output, err := durabletest.ResultAs[MyOutput](result)
//	if err != nil {
//	    t.Fatal(err) // the handler failed, is pending, or the result did not decode
//	}
//
// The runners do not take a test handle, so the same calls work in a plain
// main package, for example a program started with go run during local
// development.
//
// # Errors
//
// The runner methods return a non-nil error only when the runner itself
// fails, and the [TestResult] is then nil:
//
//   - [LocalRunner.Run] and [LocalRunner.RunUntilComplete]: the event does
//     not marshal to JSON, the invocation returns an error that indicates
//     an SDK or runner bug, or the response does not parse.
//   - [CloudRunner.Run] and [CloudRunner.RunWithArn]: the Lambda Invoke
//     call fails, the invoke response has no DurableExecutionArn, polling
//     fails, or the context ends.
//
// The outcome of the handler is not an error. A handler that returns an
// error produces a nil error and a result with Status [Failed] and the
// recorded error in [TestResult.Error]. A run blocked on a callback or
// invoke produces a nil error and Status [Pending]. A run that reaches the
// invocation cap produces a nil error and [TestResult.CapReached] set to
// true. [ResultAs] returns an error for any result that did not succeed;
// for a failed handler its message includes the recorded error type and
// message.
//
// The assertion helpers, such as [AssertGoldenSignature], take a
// [testing.TB], so tests, benchmarks, and fuzz tests can call them.
//
// # Execution Model
//
// [LocalRunner.Run] performs a single invocation cycle.
// [LocalRunner.RunUntilComplete] loops automatically, advancing time between
// invocations until the execution reaches a terminal status or is blocked
// awaiting external resolution (callbacks, chained invokes).
//
// The in-memory client validates each checkpoint against the service's
// limits and rejects an update the service rejects, with the service's
// error code and message: a step result or WaitForCondition state over
// 262144 bytes, an error object over 262144 bytes, an invoke input over
// 1048576 bytes, and a handler result checkpointed on the execution over
// 6291456 bytes. The SDK handles the rejection as it handles the
// service's: it fails the execution with a [*durable.CheckpointError].
//
// The client also rejects a checkpointed wait of zero seconds, as the
// service does. [durable.Wait] never sends one: it rejects a duration under
// one second at the call, with a plain error, and records no operation. So
// this check applies only to a malformed checkpoint sent to the client
// directly.
//
// The client also reports an operation's completion in the invocation that
// observes it, not only between invocations. It keeps a virtual clock,
// which it uses for every timestamp it records. The clock is set when the
// execution starts, and wall-clock time does not move it. A wait records
// its scheduled end time and a step retry its next attempt time on this
// clock. The clock moves forward only on a request where the handler spent
// time: a poll, which the SDK sends while a goroutine is blocked, or a
// request that reports a step attempt or a child context finished. A
// request that only starts work, such as a step's START, moves nothing. On
// such a request the client moves the clock to the earliest due time among
// the waits and step retries that earlier requests started, and reports
// each one then due in that request's response: a wait as SUCCEEDED, a
// step retry or condition check as READY. A callback resolved
// with [LocalRunner.SendCallbackSuccess] or [LocalRunner.SendCallbackFailure]
// and an invoke settled by a registered function are reported in the
// response to the next checkpoint request. The handler then continues in
// the same invocation, as it does under the service. A wait the handler
// awaits with no other work under way still suspends the invocation.
//
// The runner rejects a PENDING response that reports no pending operation,
// as the service does. It invokes the handler again, and the fourth such
// response in a row fails the execution with an
// InvalidParameterValueException.
//
// # External Resolution
//
// For handlers that use callbacks or chained invokes, use RunUntilComplete
// to reach the PENDING state, then resolve the pending operation:
//
//	result, err := runner.RunUntilComplete(input) // returns PENDING
//	cbs := runner.OpenCallbacks()                 // enumerate pending callbacks
//	runner.SendCallbackSuccess(cbs[0].CallbackID, "payload")
//	result, err = runner.RunUntilComplete(input)  // now reaches SUCCEEDED
//
// Chained invokes follow the same pattern:
//
//	result, err := runner.RunUntilComplete(input) // PENDING on invoke
//	runner.CompleteChainedInvoke("invoke-op", reply)
//	result, err = runner.RunUntilComplete(input)  // SUCCEEDED
//
// # Multi-Function Tests
//
// Instead of stubbing an invoke's result, register the target function and
// let the invoke run it. [LocalRunner.RegisterFunction] binds a function
// identifier — the name or ARN the handler passes to [durable.Invoke] — to
// a [Function] built with [DurableFunction] or [PlainFunction]. A durable
// target runs as its own local execution with its own checkpoint log, so
// it suspends and resumes like the handler under test; a plain target is
// called once with the decoded input. The target runs when the invoke's
// START checkpoint arrives. Its result or error is recorded on the invoke
// and reported in the response to that checkpoint, so the caller reads it
// in the same invocation.
// Registered targets may invoke other registered identifiers, up to
// [MaxInvokeDepth] levels deep:
//
//	runner := durabletest.NewLocalRunner(orderHandler)
//	runner.RegisterFunction("pricing-function", durabletest.DurableFunction(pricingHandler))
//	runner.RegisterFunction("tax-function", durabletest.PlainFunction(taxHandler))
//	result, err := runner.RunUntilComplete(order) // runs both targets; SUCCEEDED
//
// Invokes of identifiers that are not registered still block for
// [LocalRunner.CompleteChainedInvoke], [LocalRunner.FailChainedInvoke], or
// [LocalRunner.TimeoutChainedInvoke], so the two styles mix within one
// test. This holds inside registered durable targets too: when a running
// target invokes an unregistered identifier, the same methods resolve
// that invoke, provided its name is open in only one execution. A durable
// target blocked on a callback leaves the caller's invoke STARTED;
// resolving that callback is not yet supported through the runner.
//
// # Inspecting Operations
//
// [TestResult] provides accessors to look up operations by name, index, or
// ID. [TestResult.OperationByNameAndIndex] selects one occurrence of a name
// that a loop or a repeated child context checkpointed more than once. Each
// [TestOperation] carries typed detail getters for the operation's
// checkpoint state. When an assertion fails, [TestResult.FormatTree]
// renders the operations as an indented tree for the test log:
//
//	if result.Operation("charge") == nil {
//	    t.Fatalf("charge step missing:\n%s", result.FormatTree())
//	}
//
// # Reusing a Runner
//
// A [LocalRunner] keeps its checkpoint log across calls so that successive
// invocations continue one execution. [LocalRunner.Reset] discards that log
// and every open callback and invoke while keeping the handler, its options,
// and the registered functions, so one runner can run several independent
// test cases.
//
// # Inspecting History
//
// [TestResult.Events] is the execution's history event sequence and
// [TestResult.Invocations] has one record per completed invocation. Both
// runners populate them, so a test can assert on the event order or the
// number of invocations an execution needed in either environment:
//
//	result, err := runner.RunUntilComplete(input)
//	if err != nil {
//	    t.Fatal(err)
//	}
//	if got := len(result.Invocations); got != 3 {
//	    t.Fatalf("invocations = %d, want 3", got)
//	}
//	types := result.EventTypes() // e.g. ["ExecutionStarted", "StepStarted", ...]
package durabletest
