package durablelint

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestNondeterminism(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), NondeterminismAnalyzer, "nondeterminism")
}
