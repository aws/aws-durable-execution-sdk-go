package durablelint

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestClosure(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), ClosureAnalyzer, "closure")
}
