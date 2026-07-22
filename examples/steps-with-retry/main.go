// Command steps-with-retry demonstrates a workflow with multiple steps that
// each use retry. It simulates polling a data store with transient failures,
// using a bounded retry strategy so the execution terminates deterministically.
package main

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input carries the lookup key.
type Input struct {
	Name string `json:"name"`
}

// Record is the found item.
type Record struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// retryFiveTimesOneSecond retries up to 5 times with a 1-second fixed delay.
func retryFiveTimesOneSecond() durable.RetryStrategy {
	return durable.NewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  6, // 1 initial + 5 retries
		InitialDelay: 1 * time.Second,
		BackoffRate:  1,
		Jitter:       durable.JitterNone,
	})
}

func handler(ctx durable.Context, event Input) (*Record, error) {
	var found *Record

	for poll := 1; poll <= 5; poll++ {
		name := fmt.Sprintf("poll-%d", poll)

		result, err := durable.Step(ctx, name,
			func(_ durable.StepContext) (*Record, error) {
				// Simulate transient failure 50% of the time.
				if rand.Float64() < 0.5 { //nolint:gosec // demo randomness
					return nil, errors.New("random failure")
				}
				// Simulate "not found" for first 3 polls; found on 4th+.
				if poll < 4 {
					return nil, nil
				}
				return &Record{ID: "rec-001", Name: event.Name}, nil
			},
			durable.WithRetry(retryFiveTimesOneSecond()),
		)
		if err != nil {
			return nil, fmt.Errorf("poll %d failed: %w", poll, err)
		}
		if result != nil {
			found = result
			break
		}

		// Wait 1 second before next poll attempt.
		if err := durable.Wait(ctx, fmt.Sprintf("wait-%d", poll), 1*time.Second); err != nil {
			return nil, err
		}
	}

	if found == nil {
		return nil, errors.New("item not found")
	}
	return found, nil
}

func main() { durable.Start(handler) }
