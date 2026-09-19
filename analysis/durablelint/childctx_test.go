package durablelint

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestChildContext(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), ChildContextAnalyzer, "childctx")
}
