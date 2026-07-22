// Command hello-world is the simplest possible durable function: it
// performs no durable operations and returns a greeting string.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(_ durable.Context, _ any) (string, error) {
	return "Hello World!", nil
}

func main() { durable.Start(handler) }
