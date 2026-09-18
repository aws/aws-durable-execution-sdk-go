package durable

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// keepPolling is a predicate that never reports the condition met.
func keepPolling(int) bool { return true }

// callerStrategy is a caller-defined function type with the wait strategy
// signature. It stands in for user code that named its own strategy type
// before NewWaitStrategy existed.
type callerStrategy func(state int, attempt int) WaitDecision

func TestConditionConfigWaitStrategyAssignability(t *testing.T) {
	// ConditionConfig.WaitStrategy keeps its unnamed function type. So a
	// WaitStrategy[S], a function literal, and a caller-defined function
	// type all assign to it without conversion. This compiles or it does
	// not; the assertions only confirm each value survived assignment.
	var (
		built                  = MustNewWaitStrategy(WaitConfig[int]{})
		literal                = func(int, int) WaitDecision { return WaitDecision{} }
		named   callerStrategy = func(int, int) WaitDecision { return WaitDecision{Continue: true} }
	)
	for name, cfg := range map[string]ConditionConfig[int]{
		"WaitStrategy":  {WaitStrategy: built},
		"literal":       {WaitStrategy: literal},
		"callerDefined": {WaitStrategy: named},
	} {
		if cfg.WaitStrategy == nil {
			t.Errorf("%s: WaitStrategy is nil after assignment", name)
		}
	}
	if d := (ConditionConfig[int]{WaitStrategy: named}).WaitStrategy(0, 1); !d.Continue {
		t.Error("caller-defined strategy: Continue = false, want true")
	}
}

func TestNewWaitStrategyDeterministic(t *testing.T) {
	// Jitter NONE makes delays exact: initial × rate^(attempt-1), capped
	// at MaxDelay. Reaching MaxAttempts fails rather than stops.
	strategy := MustNewWaitStrategy(WaitConfig[int]{
		MaxAttempts:    5,
		InitialDelay:   2 * time.Second,
		MaxDelay:       20 * time.Second,
		BackoffRate:    3,
		Jitter:         JitterNone,
		ShouldContinue: keepPolling,
	})

	tests := []struct {
		attempt      int
		wantContinue bool
		wantDelay    time.Duration
	}{
		{1, true, 2 * time.Second},
		{2, true, 6 * time.Second},
		{3, true, 18 * time.Second},
		{4, true, 20 * time.Second}, // 54s capped at MaxDelay
		{5, false, 0},               // attempts exhausted
		{6, false, 0},
	}
	for _, tt := range tests {
		d := strategy(0, tt.attempt)
		if d.Continue != tt.wantContinue {
			t.Errorf("attempt %d: Continue = %v, want %v", tt.attempt, d.Continue, tt.wantContinue)
		}
		if tt.wantContinue && d.Delay != tt.wantDelay {
			t.Errorf("attempt %d: Delay = %v, want %v", tt.attempt, d.Delay, tt.wantDelay)
		}
		if tt.wantContinue && d.Err != nil {
			t.Errorf("attempt %d: Err = %v, want nil while continuing", tt.attempt, d.Err)
		}
		if !tt.wantContinue {
			if d.Err == nil {
				t.Errorf("attempt %d: Err = nil, want a max-attempts failure", tt.attempt)
			} else if !strings.Contains(d.Err.Error(), "exceeded maximum attempts (5)") {
				t.Errorf("attempt %d: Err = %q, want it to name the 5-attempt cap", tt.attempt, d.Err)
			}
		}
	}
}

func TestNewWaitStrategyDefaultsPinned(t *testing.T) {
	// The zero value of every field except Jitter selects the documented
	// default: 5s initial delay, backoff rate 1.5, 5 minute cap, 60
	// attempts. Jitter NONE exposes the exact schedule.
	strategy := MustNewWaitStrategy(WaitConfig[int]{Jitter: JitterNone, ShouldContinue: keepPolling})

	delays := map[int]time.Duration{
		1:  5 * time.Second,   // 5
		2:  8 * time.Second,   // 7.5 rounds to 8
		3:  11 * time.Second,  // 11.25 rounds to 11
		4:  17 * time.Second,  // 16.875 rounds to 17
		10: 192 * time.Second, // 5 × 1.5^9 ≈ 192.2
		11: 288 * time.Second, // 5 × 1.5^10 ≈ 288.3
		12: 300 * time.Second, // ≈ 432.5 capped at 5 minutes
		59: 300 * time.Second, // still capped on the last continuing attempt
	}
	for attempt, want := range delays {
		d := strategy(0, attempt)
		if !d.Continue || d.Err != nil {
			t.Errorf("attempt %d = %+v, want Continue with no error", attempt, d)
			continue
		}
		if d.Delay != want {
			t.Errorf("attempt %d: Delay = %v, want %v", attempt, d.Delay, want)
		}
	}

	d := strategy(0, 60)
	if d.Continue || d.Err == nil {
		t.Fatalf("attempt 60 = %+v, want a failure decision", d)
	}
	if !strings.Contains(d.Err.Error(), "exceeded maximum attempts (60)") {
		t.Errorf("attempt 60: Err = %q, want it to name the 60-attempt cap", d.Err)
	}
}

func TestNewWaitStrategyDefaultMatchesConditionDefault(t *testing.T) {
	// The strategy WaitForCondition substitutes for a nil WaitStrategy is
	// the one the zero WaitConfig builds, so both are the same
	// implementation with the same constants. With jitter the delays are
	// random, so compare the decisions that jitter does not affect: every
	// attempt before the cap continues and the cap fails identically.
	def := defaultConditionWaitStrategy[int]()
	built := MustNewWaitStrategy(WaitConfig[int]{})

	for attempt := 1; attempt < defaultConditionMaxAttempts; attempt++ {
		a, b := def(0, attempt), built(0, attempt)
		if !a.Continue || a.Err != nil || !b.Continue || b.Err != nil {
			t.Fatalf("attempt %d: default = %+v, built = %+v, want both to continue", attempt, a, b)
		}
		if a.Delay < time.Second || b.Delay < time.Second {
			t.Fatalf("attempt %d: default delay %v, built delay %v, want at least 1s", attempt, a.Delay, b.Delay)
		}
	}
	a, b := def(0, defaultConditionMaxAttempts), built(0, defaultConditionMaxAttempts)
	if a.Continue || b.Continue || a.Err == nil || b.Err == nil {
		t.Fatalf("attempt %d: default = %+v, built = %+v, want both to fail", defaultConditionMaxAttempts, a, b)
	}
	if a.Err.Error() != b.Err.Error() {
		t.Errorf("failure messages differ: default %q, built %q", a.Err, b.Err)
	}
}

func TestNewWaitStrategyPredicateStops(t *testing.T) {
	// ShouldContinue sees the observed state. A false result stops the
	// wait with no error, whatever the attempt number.
	var seen []int
	strategy := MustNewWaitStrategy(WaitConfig[int]{
		MaxAttempts: 3,
		Jitter:      JitterNone,
		ShouldContinue: func(state int) bool {
			seen = append(seen, state)
			return state < 10
		},
	})

	if d := strategy(4, 1); !d.Continue || d.Err != nil {
		t.Errorf("state 4 attempt 1 = %+v, want Continue", d)
	}
	if d := strategy(10, 2); d.Continue || d.Err != nil {
		t.Errorf("state 10 attempt 2 = %+v, want stop with no error", d)
	}
	if want := []int{4, 10}; len(seen) != 2 || seen[0] != want[0] || seen[1] != want[1] {
		t.Errorf("predicate saw %v, want %v", seen, want)
	}
}

func TestNewWaitStrategyConditionMetOnFinalAttemptSucceeds(t *testing.T) {
	// A met condition takes precedence over exhaustion: on the last
	// permitted attempt a false predicate stops rather than fails.
	strategy := MustNewWaitStrategy(WaitConfig[int]{
		MaxAttempts:    3,
		Jitter:         JitterNone,
		ShouldContinue: func(state int) bool { return state == 0 },
	})
	if d := strategy(1, 3); d.Continue || d.Err != nil {
		t.Errorf("met on attempt 3 = %+v, want stop with no error", d)
	}
	if d := strategy(0, 3); d.Continue || d.Err == nil {
		t.Errorf("unmet on attempt 3 = %+v, want failure", d)
	}
}

func TestNewWaitStrategyNilPredicateNeverStops(t *testing.T) {
	// Without a predicate the strategy continues on every attempt below
	// the cap and fails at the cap; it never reports the condition met.
	strategy := MustNewWaitStrategy(WaitConfig[int]{MaxAttempts: 2, Jitter: JitterNone})
	if d := strategy(1, 1); !d.Continue {
		t.Errorf("attempt 1 = %+v, want Continue", d)
	}
	if d := strategy(1, 2); d.Continue || d.Err == nil {
		t.Errorf("attempt 2 = %+v, want failure", d)
	}
}

func TestNewWaitStrategyValidConfigs(t *testing.T) {
	// Configs that must be accepted, including boundary cases that
	// validation deliberately does not reject.
	tests := []struct {
		name string
		cfg  WaitConfig[int]
	}{
		{"zero value", WaitConfig[int]{}},
		{"single attempt", WaitConfig[int]{MaxAttempts: 1}},
		{"max delay below initial delay", WaitConfig[int]{InitialDelay: 10 * time.Second, MaxDelay: 2 * time.Second}},
		{"fractional backoff rate", WaitConfig[int]{BackoffRate: 0.5}},
		{"one second delays", WaitConfig[int]{InitialDelay: time.Second, MaxDelay: time.Second}},
		{"jitter half", WaitConfig[int]{Jitter: JitterHalf}},
		{"jitter none", WaitConfig[int]{Jitter: JitterNone}},
		{"predicate only", WaitConfig[int]{ShouldContinue: keepPolling}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, err := NewWaitStrategy(tt.cfg)
			if err != nil {
				t.Fatalf("NewWaitStrategy(%+v) error = %v, want nil", tt.cfg, err)
			}
			if strategy == nil {
				t.Fatal("strategy is nil")
			}
		})
	}
}

func TestNewWaitStrategyInvalidConfig(t *testing.T) {
	tests := []struct {
		name       string
		cfg        WaitConfig[int]
		wantFields []string
	}{
		{"negative max attempts", WaitConfig[int]{MaxAttempts: -1}, []string{"MaxAttempts"}},
		{"sub-second initial delay", WaitConfig[int]{InitialDelay: 500 * time.Millisecond}, []string{"InitialDelay"}},
		{"negative initial delay", WaitConfig[int]{InitialDelay: -time.Second}, []string{"InitialDelay"}},
		{"sub-second max delay", WaitConfig[int]{MaxDelay: time.Millisecond}, []string{"MaxDelay"}},
		{"negative max delay", WaitConfig[int]{MaxDelay: -time.Minute}, []string{"MaxDelay"}},
		{"negative backoff rate", WaitConfig[int]{BackoffRate: -1}, []string{"BackoffRate"}},
		{"NaN backoff rate", WaitConfig[int]{BackoffRate: math.NaN()}, []string{"BackoffRate"}},
		{"positive infinite backoff rate", WaitConfig[int]{BackoffRate: math.Inf(1)}, []string{"BackoffRate"}},
		{"negative infinite backoff rate", WaitConfig[int]{BackoffRate: math.Inf(-1)}, []string{"BackoffRate"}},
		{"undefined jitter", WaitConfig[int]{Jitter: "BOGUS"}, []string{"Jitter"}},
		{
			"multiple invalid fields",
			WaitConfig[int]{MaxAttempts: -3, InitialDelay: -time.Second, MaxDelay: 10 * time.Millisecond, BackoffRate: -0.5, Jitter: "??"},
			[]string{"MaxAttempts", "InitialDelay", "MaxDelay", "BackoffRate", "Jitter"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, err := NewWaitStrategy(tt.cfg)
			if err == nil {
				t.Fatalf("NewWaitStrategy(%+v) error = nil, want error", tt.cfg)
			}
			if strategy != nil {
				t.Error("strategy is non-nil, want nil on invalid config")
			}
			for _, field := range tt.wantFields {
				if !strings.Contains(err.Error(), "WaitConfig."+field) {
					t.Errorf("error %q does not name field WaitConfig.%s", err, field)
				}
			}
		})
	}
}

func TestMustNewWaitStrategyPanicsOnInvalidConfig(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustNewWaitStrategy did not panic on invalid config")
		}
	}()
	MustNewWaitStrategy(WaitConfig[int]{MaxAttempts: -1})
}

func TestNewWaitStrategyFullJitterBounds(t *testing.T) {
	// Full jitter: delay in [1s, base], rounded to whole seconds.
	strategy := MustNewWaitStrategy(WaitConfig[int]{
		InitialDelay:   10 * time.Second,
		BackoffRate:    1,
		Jitter:         JitterFull,
		ShouldContinue: keepPolling,
	})
	for range 200 {
		d := strategy(0, 1)
		if d.Delay < time.Second || d.Delay > 10*time.Second {
			t.Fatalf("Delay = %v, want within [1s, 10s]", d.Delay)
		}
		if d.Delay%time.Second != 0 {
			t.Fatalf("Delay = %v, want a whole number of seconds", d.Delay)
		}
	}
}

func TestNewWaitStrategyHalfJitterBounds(t *testing.T) {
	// Half jitter: delay in [base/2, base], rounded to whole seconds.
	strategy := MustNewWaitStrategy(WaitConfig[int]{
		InitialDelay:   10 * time.Second,
		BackoffRate:    1,
		Jitter:         JitterHalf,
		ShouldContinue: keepPolling,
	})
	for range 200 {
		d := strategy(0, 1)
		if d.Delay < 5*time.Second || d.Delay > 10*time.Second {
			t.Fatalf("Delay = %v, want within [5s, 10s]", d.Delay)
		}
	}
}

func TestNewWaitStrategyMinimumOneSecond(t *testing.T) {
	// A backoff rate below one shrinks the base below a second; the
	// delay never drops under one second.
	strategy := MustNewWaitStrategy(WaitConfig[int]{
		InitialDelay:   time.Second,
		BackoffRate:    0.1,
		Jitter:         JitterNone,
		ShouldContinue: keepPolling,
	})
	if d := strategy(0, 5); d.Delay != time.Second {
		t.Errorf("attempt 5: Delay = %v, want 1s floor", d.Delay)
	}
}

func TestWaitForConditionBuiltStrategySucceeds(t *testing.T) {
	// A built strategy drives WaitForCondition: the predicate stops the
	// wait and the final state is checkpointed as SUCCEED.
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "STARTED", &wireStepDetails{Attempt: 2, Result: "2"}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		result, err := WaitForCondition(ctx, "built", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			WaitStrategy: MustNewWaitStrategy(WaitConfig[int]{
				MaxAttempts:    3,
				ShouldContinue: func(state int) bool { return state < 3 },
			}),
		})
		if err != nil {
			return "", err
		}
		return strconv.Itoa(result), nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"3\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	found := false
	for _, u := range updateBatch(t, fake) {
		if u.Action == OperationActionSucceed && aws.ToString(u.Payload) == "3" {
			found = true
		}
	}
	if !found {
		t.Error("expected SUCCEED checkpoint with payload 3")
	}
}

func TestWaitForConditionBuiltStrategyDelay(t *testing.T) {
	// The built strategy's delay reaches the RETRY checkpoint: attempt 2
	// with a 2s initial delay, rate 3, and no jitter schedules 6s.
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "STARTED", &wireStepDetails{Attempt: 1, Result: "1"}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "built", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			WaitStrategy: MustNewWaitStrategy(WaitConfig[int]{
				InitialDelay:   2 * time.Second,
				BackoffRate:    3,
				Jitter:         JitterNone,
				ShouldContinue: keepPolling,
			}),
		})
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	var retry *OperationUpdate
	updates := updateBatch(t, fake)
	for i, u := range updates {
		if u.Action == OperationActionRetry {
			retry = &updates[i]
		}
	}
	if retry == nil {
		t.Fatal("expected a RETRY checkpoint")
	}
	if got := aws.ToInt32(retry.StepOptions.NextAttemptDelaySeconds); got != 6 {
		t.Errorf("delay = %ds, want 6s", got)
	}
}

func TestWaitForConditionBuiltStrategyExhaustionFails(t *testing.T) {
	// Reaching MaxAttempts with the condition unmet fails the operation:
	// a FAIL checkpoint and a *WaitForConditionError, not a silent stop.
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "STARTED", &wireStepDetails{Attempt: 2, Result: "2"}),
	)
	var got error
	invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "built", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			WaitStrategy: MustNewWaitStrategy(WaitConfig[int]{
				MaxAttempts:    3,
				ShouldContinue: keepPolling,
			}),
		})
		got = err
		return "", err
	})

	var wfcErr *WaitForConditionError
	if !errors.As(got, &wfcErr) {
		t.Fatalf("error = %v (%T), want *WaitForConditionError", got, got)
	}
	if wfcErr.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3", wfcErr.Attempts)
	}
	if !strings.Contains(got.Error(), "exceeded maximum attempts (3)") {
		t.Errorf("error %q does not mention exceeding 3 attempts", got)
	}
	failed := false
	for _, u := range updateBatch(t, fake) {
		if u.Action == OperationActionFail {
			failed = true
		}
	}
	if !failed {
		t.Error("expected a FAIL checkpoint at the attempt cap")
	}
}
