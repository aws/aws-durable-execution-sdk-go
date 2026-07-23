// registry.go bridges conformance/handlers' own Registry (see that
// package's doc comment for the full registration contract - every
// handlers/*.go file's init() populates handlers.Registry, not anything
// in this file) into package main's own convenience alias, used by
// main.go to look up and build the handler for this deployed function's
// CONFORMANCE_TEST_ID.
package main

import (
	// Imported for its Registry value below, AND for its side effect:
	// importing this package runs every handlers/*.go file's own
	// init() (which calls handlers.Register) - Go's standard idiom for
	// a self-registering plugin-style package where main never calls
	// anything in handlers by name beyond reading the resulting
	// Registry map.
	"github.com/aws/aws-durable-execution-sdk-go/conformance/handlers"
)

// requirementHandlerFactories is just handlers.Registry under this
// package's own name, for readability at main.go's own call site.
var requirementHandlerFactories = handlers.Registry
