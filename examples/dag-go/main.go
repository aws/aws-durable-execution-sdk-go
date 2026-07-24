// Command dag-go demonstrates the EXPERIMENTAL dag package. The durable
// DAG handler lives in handler.go and is exercised by handler_test.go
// against the SDK's LocalTestRunner. A real deployment would wrap Handler
// with the Lambda Runtime API plumbing shown in the other Go examples;
// this main keeps that plumbing out of the way so the example stays focused
// on the DAG API itself.
package main

import (
	"log"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
)

func main() {
	// Wrapping proves Handler satisfies durable.Handler; run
	// `go test ./...` to exercise it end to end against the local runner.
	_ = durable.WithDurableExecution(Handler, nil)
	log.Println("dag-go example: run `go test ./...` to exercise the DAG handler")
}
