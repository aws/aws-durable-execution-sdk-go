// Package gclplugin registers the durablelint analyzers as a golangci-lint
// module plugin under the linter name "durablelint".
//
// Build a golangci-lint binary that includes the plugin with a
// .custom-gcl.yml at the repository root:
//
//	version: v2.12.2
//	plugins:
//	  - module: github.com/aws/aws-durable-execution-sdk-go/analysis
//	    import: github.com/aws/aws-durable-execution-sdk-go/analysis/gclplugin
//	    version: latest
//
// then run "golangci-lint custom" and enable the linter in .golangci.yml:
//
//	linters:
//	  enable:
//	    - durablelint
//	  settings:
//	    custom:
//	      durablelint:
//	        type: module
//	        description: determinism rules for AWS Durable Execution handlers
//
// The resulting ./custom-gcl binary replaces golangci-lint for that
// repository, and "//nolint:durablelint" suppresses the rules alongside the
// //durable:ignore directives.
package gclplugin

import (
	"github.com/aws/aws-durable-execution-sdk-go/analysis/durablelint"
	"github.com/golangci/plugin-module-register/register"
	"golang.org/x/tools/go/analysis"
)

// Name is the linter name under which the plugin registers.
const Name = "durablelint"

func init() {
	register.Plugin(Name, newPlugin)
}

type plugin struct{}

func newPlugin(settings any) (register.LinterPlugin, error) {
	return plugin{}, nil
}

// BuildAnalyzers returns every durablelint rule.
func (plugin) BuildAnalyzers() ([]*analysis.Analyzer, error) {
	return durablelint.Analyzers, nil
}

// GetLoadMode requests type information, which every rule needs.
func (plugin) GetLoadMode() string {
	return register.LoadModeTypesInfo
}
