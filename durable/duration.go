package durable

import (
	"fmt"
	"math"
	"time"
)

// durationToSeconds converts a time.Duration to an int32 number of whole
// seconds, rounding up (ceiling). It rejects negative durations and
// durations that exceed math.MaxInt32 seconds (~68 years).
func durationToSeconds(d time.Duration) (int32, error) {
	if d < 0 {
		return 0, fmt.Errorf("durable: duration must not be negative, got %v", d)
	}
	sec := math.Ceil(d.Seconds())
	if sec > math.MaxInt32 {
		return 0, fmt.Errorf("durable: duration %v exceeds maximum of %d seconds", d, math.MaxInt32)
	}
	return int32(sec), nil
}
