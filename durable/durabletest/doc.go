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
//	result := runner.RunUntilComplete(t, myInput)
//	if result.Status != durabletest.Succeeded {
//	    t.Fatalf("expected SUCCEEDED, got %s", result.Status)
//	}
//	output, err := durabletest.ResultAs[MyOutput](result)
//
// # Execution Model
//
// [LocalRunner.Run] performs a single invocation cycle.
// [LocalRunner.RunUntilComplete] loops automatically, advancing time between
// invocations until the execution reaches a terminal status or is blocked
// awaiting external resolution (callbacks, chained invokes).
//
// # External Resolution
//
// For handlers that use callbacks or chained invokes, use RunUntilComplete
// to reach the PENDING state, then resolve the pending operation:
//
//	result := runner.RunUntilComplete(t, input) // returns PENDING
//	cbs := runner.OpenCallbacks()               // enumerate pending callbacks
//	runner.SendCallbackSuccess(cbs[0].CallbackID, "payload")
//	result = runner.RunUntilComplete(t, input)  // now reaches SUCCEEDED
//
// Chained invokes follow the same pattern:
//
//	result := runner.RunUntilComplete(t, input)   // PENDING on invoke
//	runner.CompleteChainedInvoke("invoke-op", result)
//	result = runner.RunUntilComplete(t, input)    // SUCCEEDED
//
// # Multi-Function Tests
//
// Instead of stubbing an invoke's result, register the target function and
// let the invoke run it. [LocalRunner.RegisterFunction] binds a function
// identifier — the name or ARN the handler passes to [durable.Invoke] — to
// a [Function] built with [DurableFunction] or [PlainFunction]. A durable
// target runs as its own local execution with its own checkpoint log, so
// it suspends and resumes like the handler under test; a plain target is
// called once with the decoded input. The target's result or error is
// recorded on the invoke, and the caller reads it on its next invocation.
// Registered targets may invoke other registered identifiers, up to
// [MaxInvokeDepth] levels deep:
//
//	runner := durabletest.NewLocalRunner(orderHandler)
//	runner.RegisterFunction("pricing-function", durabletest.DurableFunction(pricingHandler))
//	runner.RegisterFunction("tax-function", durabletest.PlainFunction(taxHandler))
//	result := runner.RunUntilComplete(t, order) // runs both targets; SUCCEEDED
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
//	result := runner.RunUntilComplete(t, input)
//	if got := len(result.Invocations); got != 3 {
//	    t.Fatalf("invocations = %d, want 3", got)
//	}
//	types := result.EventTypes() // e.g. ["ExecutionStarted", "StepStarted", ...]
package durabletest
