// Command durablelint runs the determinism analyzers for the AWS Durable
// Execution SDK for Go over a set of packages.
//
// Usage:
//
//	durablelint ./...
//	durablelint -durablenondeterminism=false ./...
//
// Each rule can be switched off with -<rule>=false. Run durablelint -help
// for the full flag list, including the standard -json and -fix output
// options of the go/analysis framework.
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/analysis/durablelint"
	"golang.org/x/tools/go/analysis/multichecker"
)

func main() {
	multichecker.Main(durablelint.Analyzers...)
}
