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
// # Inspecting Operations
//
// [TestResult] provides accessors to look up operations by name, index, or
// ID. Each [TestOperation] carries typed detail getters for the operation's
// checkpoint state.
package durabletest
