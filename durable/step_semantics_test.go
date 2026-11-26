// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// The deprecated names keep the values of the names that replace them.
func TestDeprecatedStepSemanticsNames(t *testing.T) {
	if durable.AtLeastOncePerRetry != durable.AtLeastOnce { //nolint:staticcheck // checks the deprecated name
		t.Error("AtLeastOncePerRetry != AtLeastOnce")
	}
	if durable.AtMostOncePerRetry != durable.AtMostOnce { //nolint:staticcheck // checks the deprecated name
		t.Error("AtMostOncePerRetry != AtMostOnce")
	}
}
