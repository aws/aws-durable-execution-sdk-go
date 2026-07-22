// Package handlers contains one file per conformance requirement,
// implementing exactly the scenario its
// test-requirements/<suite>/<id>.yaml file describes (see the upstream
// aws-durable-execution-conformance-tests repo). Each file's own doc
// comment cites the requirement id and quotes its YAML's own
// description/handler/invocations fields, so the Go implementation and
// its requirement source stay traceably linked.
//
// Every file registers itself with this package's own Registry map via
// an init() calling Register(id, factory). main.go (package main)
// imports handlers purely for its side effects (every handler file's
// init() running) and copies Registry's contents into its own
// requirementHandlerFactories map at startup - see main.go's blank
// import and registry_bridge.go for that one-line bridge. Declaring
// Registry here (not in package main) lets every handler file be a
// normal member of the handlers package - matching this repo's
// established one-purpose-per-file convention - without needing to be
// literally `package main` itself, and without handlers importing
// package main (which would be circular; main imports handlers, never
// the reverse).
package handlers

import (
	"context"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// Handler is the uniform, type-erased shape every requirement's
// durable.WithDurableExecution(...) return value is stored as - see this
// package's own doc comment for the full registration contract.
type Handler func(ctx context.Context, input types.DurableExecutionInvocationInput) (types.DurableExecutionOutput, error)

// Factory builds a Handler given the shared production
// checkpoint.Client, constructed once in main.go's main() at cold start
// (matching every other example in this repo's own established pattern
// of one awssdk.Client per Lambda execution environment, not per
// invocation).
type Factory func(client checkpoint.Client) Handler

// Registry maps a conformance requirement id (e.g. "1-1") to its handler
// factory. Populated by each *.go file in this package via its own
// init() calling Register.
var Registry = map[string]Factory{}

// Register adds a requirement handler factory to Registry. Panics on a
// duplicate id - a real programming error (two files claiming the same
// requirement id) that should fail loudly at program startup rather than
// silently overwrite one implementation with another.
func Register(requirementID string, factory Factory) {
	if _, exists := Registry[requirementID]; exists {
		panic("conformance/handlers: duplicate handler registered for requirement " + requirementID)
	}
	Registry[requirementID] = factory
}

// config is a small shared helper every handler file uses to build a
// *durable.Config wrapping the shared checkpoint.Client - avoids
// repeating the same struct literal in every one of the 148 handler
// files.
func config(client checkpoint.Client) *durable.Config {
	return &durable.Config{Client: client}
}
