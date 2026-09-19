package durablelint

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestNestedOp(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), NestedOpAnalyzer, "nestedop")
}
