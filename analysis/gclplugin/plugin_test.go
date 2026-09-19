package gclplugin

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/analysis/durablelint"
	"github.com/golangci/plugin-module-register/register"
)

func TestPlugin(t *testing.T) {
	p, err := newPlugin(nil)
	if err != nil {
		t.Fatal(err)
	}
	analyzers, err := p.BuildAnalyzers()
	if err != nil {
		t.Fatal(err)
	}
	if len(analyzers) != len(durablelint.Analyzers) {
		t.Fatalf("got %d analyzers, want %d", len(analyzers), len(durablelint.Analyzers))
	}
	if got := p.GetLoadMode(); got != register.LoadModeTypesInfo {
		t.Fatalf("load mode = %q, want %q", got, register.LoadModeTypesInfo)
	}
}
