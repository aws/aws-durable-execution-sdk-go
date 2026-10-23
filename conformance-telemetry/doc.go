// Package conformancetelemetry is the root of the Durable Execution Telemetry
// conformance test handlers for the Go SDK.
//
// The module holds one command per requirement, each in its own directory.
// This file exists so that the module has more than one package. With a
// single package, `go build ./...` writes an executable named after the
// package directory, and that name collides with the directory itself.
package conformancetelemetry
