package durable

import (
	"testing"
	"time"
)

// The checkpoint API rejects NextAttemptDelaySeconds below 1 with a
// ValidationException, which fails the operation outright instead of retrying it.
// Cloud validation caught this: a DAG task with a zero retry delay never reached
// its second attempt. JS, Python and Java all clamp; Go clamped in
// waitForCondition but not in step.
func TestRetryDelaySeconds_ClampsToPlatformMinimum(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   time.Duration
		want int32
	}{
		{"zero clamps to one", 0, 1},
		{"negative clamps to one", -5 * time.Second, 1},
		{"sub-second clamps to one", 900 * time.Millisecond, 1},
		{"exact second is kept", time.Second, 1},
		{"fractional rounds up", 1500 * time.Millisecond, 2},
		{"whole seconds kept", 30 * time.Second, 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryDelaySeconds(tc.in); got != tc.want {
				t.Fatalf("retryDelaySeconds(%v) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
