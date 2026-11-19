// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	smithy "github.com/aws/smithy-go"
)

// retryFake is an ExecutionClient for pause tests. It records every
// checkpoint call in events, in order. When readyAfterRetry is set, the
// response to the RETRY of step "1" reports it READY after attempt 1, as
// the service does once the retry delay elapses. So a step that retries as
// usual runs attempt 2 in the same invocation, and a step that paused does
// not.
// tokenlessRetry makes the response to a call carrying a RETRY omit the
// token.
type retryFake struct {
	mu              sync.Mutex
	events          []string
	updates         []OperationUpdate
	readyAfterRetry bool
	tokenlessRetry  bool
	n               int
}

func (f *retryFake) GetExecutionState(context.Context, GetExecutionStateInput) (GetExecutionStateOutput, error) {
	return GetExecutionStateOutput{}, nil
}

func (f *retryFake) Checkpoint(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	out := CheckpointOutput{CheckpointToken: "token-" + string(rune('a'+f.n))}
	if len(in.Updates) == 0 {
		f.events = append(f.events, "poll")
		return out, nil
	}
	for _, u := range in.Updates {
		f.events = append(f.events, "checkpoint:"+string(u.Action))
		f.updates = append(f.updates, u)
		if u.Action == OperationActionRetry && f.tokenlessRetry {
			out.CheckpointToken = ""
		}
		if u.Action == OperationActionRetry && f.readyAfterRetry && aws.ToString(u.Id) == hashID("1") {
			out.NewExecutionState = []Operation{{
				Id: aws.String(hashID("1")), Type: OperationTypeStep, Status: OperationStatusReady,
				StepDetails: &StepDetails{Attempt: 1},
			}}
		}
	}
	return out, nil
}

// recorded returns a copy of the recorded events and updates.
func (f *retryFake) recorded() ([]string, []OperationUpdate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...), append([]OperationUpdate(nil), f.updates...)
}

// pausingFake is a retryFake that also implements ExecutionPauser.
type pausingFake struct {
	*retryFake
	pauseErr error
	pauses   []PauseExecutionInput
}

func (f *pausingFake) PauseExecution(_ context.Context, in PauseExecutionInput) (PauseExecutionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "pause")
	f.pauses = append(f.pauses, in)
	return PauseExecutionOutput{}, f.pauseErr
}

func (f *pausingFake) pauseCalls() []PauseExecutionInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]PauseExecutionInput(nil), f.pauses...)
}

// syncBuffer is a bytes.Buffer safe for concurrent writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// runPauseHandler runs handler once against client and returns the raw
// response and the log output.
func runPauseHandler(t *testing.T, client ExecutionClient, payload []byte, handler Handler[string, string]) (string, string) {
	t.Helper()
	var logs syncBuffer
	h := Wrap(handler, WithExecutionClient(client), WithLogHandler(slog.NewTextHandler(&logs, nil)))
	got, err := h(context.Background(), payload)
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	return string(got), logs.String()
}

// failingStep returns a handler with one step "s" that fails attempt 1
// and succeeds later, using strategy.
func failingStep(strategy RetryStrategy) Handler[string, string] {
	return func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s", func(sc StepContext) (string, error) {
			if sc.Attempt() == 1 {
				return "", errors.New("transient")
			}
			return "ok", nil
		}, WithRetry(strategy))
	}
}

func pauseOnFirst(RetryAttempt) RetryDecision {
	return RetryDecision{Retry: true, Delay: time.Second, Pause: true}
}

func actions(updates []OperationUpdate) []OperationAction {
	var out []OperationAction
	for _, u := range updates {
		out = append(out, u.Action)
	}
	return out
}

func TestStepPauseDecisionPausesAfterRetryCheckpoint(t *testing.T) {
	fake := &pausingFake{retryFake: &retryFake{readyAfterRetry: true}}
	resp, _ := runPauseHandler(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := Step(ctx, "s", func(StepContext) (string, error) {
			return "", errors.New("transient")
		}, WithRetry(pauseOnFirst))
		// The handler ignores the suspension. The SDK records nothing
		// more from this invocation.
		_, _ = Step(ctx, "after", func(StepContext) (string, error) { return "x", nil })
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	events, updates := fake.recorded()
	if got, want := strings.Join(events, ","), "checkpoint:START,checkpoint:RETRY,pause"; got != want {
		t.Errorf("events = %s, want %s", got, want)
	}
	if len(updates) != 2 {
		t.Fatalf("updates = %v, want START and RETRY of step s only", actions(updates))
	}
	pauses := fake.pauseCalls()
	if len(pauses) != 1 {
		t.Fatalf("pause calls = %d, want 1", len(pauses))
	}
	p := pauses[0]
	if p.ExecutionArn != "arn:test" {
		t.Errorf("ExecutionArn = %q, want arn:test", p.ExecutionArn)
	}
	if want := pauseClientToken("arn:test", hashID("1"), 1); p.ClientToken != want {
		t.Errorf("ClientToken = %q, want %q", p.ClientToken, want)
	}
	if want := `Step "s" failed attempt 1`; p.Reason != want {
		t.Errorf("Reason = %q, want %q", p.Reason, want)
	}
}

func TestStepUnnamedPauseReasonUsesWireID(t *testing.T) {
	fake := &pausingFake{retryFake: &retryFake{}}
	runPauseHandler(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		return Step(ctx, "", func(StepContext) (string, error) {
			return "", errors.New("transient")
		}, WithRetry(pauseOnFirst))
	})
	pauses := fake.pauseCalls()
	if len(pauses) != 1 {
		t.Fatalf("pause calls = %d, want 1", len(pauses))
	}
	if want := `Step "` + hashID("1") + `" failed attempt 1`; pauses[0].Reason != want {
		t.Errorf("Reason = %q, want %q", pauses[0].Reason, want)
	}
}

// assertRetriedAsUsual checks that step "s" retried without a pause: the
// poll reported it READY, attempt 2 ran, and the execution succeeded.
func assertRetriedAsUsual(t *testing.T, resp string, fake *retryFake) {
	t.Helper()
	if !strings.Contains(resp, `"Status":"SUCCEEDED"`) || !strings.Contains(resp, `ok`) {
		t.Errorf("response = %s, want SUCCEEDED with the result of attempt 2", resp)
	}
	_, updates := fake.recorded()
	got := actions(updates)
	want := []OperationAction{OperationActionStart, OperationActionRetry, OperationActionStart, OperationActionSucceed}
	if len(got) != len(want) {
		t.Fatalf("actions = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("actions = %v, want %v", got, want)
		}
	}
}

func TestStepNoPauseWithoutPauseDecision(t *testing.T) {
	for name, strategy := range map[string]RetryStrategy{
		"Pause false":      func(RetryAttempt) RetryDecision { return RetryDecision{Retry: true, Delay: time.Second, Pause: false} },
		"Pause not set":    func(RetryAttempt) RetryDecision { return RetryDecision{Retry: true, Delay: time.Second} },
		"built-in, no opt": MustNewRetryStrategy(RetryConfig{InitialDelay: time.Second, Jitter: JitterNone}),
	} {
		t.Run(name, func(t *testing.T) {
			fake := &pausingFake{retryFake: &retryFake{readyAfterRetry: true}}
			resp, _ := runPauseHandler(t, fake, stepPayload(`""`), failingStep(strategy))
			if n := len(fake.pauseCalls()); n != 0 {
				t.Errorf("pause calls = %d, want 0", n)
			}
			assertRetriedAsUsual(t, resp, fake.retryFake)
		})
	}
}

func TestStepPauseIgnoredWhenNotRetrying(t *testing.T) {
	fake := &pausingFake{retryFake: &retryFake{}}
	resp, _ := runPauseHandler(t, fake, stepPayload(`""`), failingStep(func(RetryAttempt) RetryDecision {
		return RetryDecision{Retry: false, Pause: true}
	}))
	if !strings.Contains(resp, `"Status":"FAILED"`) {
		t.Errorf("response = %s, want FAILED", resp)
	}
	if n := len(fake.pauseCalls()); n != 0 {
		t.Errorf("pause calls = %d, want 0", n)
	}
	_, updates := fake.recorded()
	if got := actions(updates); len(got) != 2 || got[1] != OperationActionFail {
		t.Errorf("actions = %v, want START, FAIL", got)
	}
}

func TestStepInterruptedAttemptIgnoresPause(t *testing.T) {
	fake := &pausingFake{retryFake: &retryFake{}}
	var asked int
	resp, _ := runPauseHandler(t, fake, stepPayload(`""`, checkpointedStep("1", "STARTED", nil)),
		func(ctx Context, _ string) (string, error) {
			return Step(ctx, "s", func(StepContext) (string, error) {
				t.Error("step body re-executed under at-most-once semantics")
				return "", nil
			}, WithSemantics(AtMostOncePerRetry), WithRetry(func(RetryAttempt) RetryDecision {
				asked++
				return RetryDecision{Retry: true, Delay: time.Minute, Pause: true}
			}))
		})
	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if asked != 1 {
		t.Errorf("strategy calls = %d, want 1", asked)
	}
	if n := len(fake.pauseCalls()); n != 0 {
		t.Errorf("pause calls = %d, want 0", n)
	}
	_, updates := fake.recorded()
	if got := actions(updates); len(got) != 1 || got[0] != OperationActionRetry {
		t.Errorf("actions = %v, want RETRY", got)
	}
}

func TestStepTokenlessRetryResponseDoesNotPause(t *testing.T) {
	fake := &pausingFake{retryFake: &retryFake{tokenlessRetry: true}}
	resp, _ := runPauseHandler(t, fake, stepPayload(`""`), failingStep(pauseOnFirst))
	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if n := len(fake.pauseCalls()); n != 0 {
		t.Errorf("pause calls = %d, want 0", n)
	}
}

func TestStepPauseFailureLogsWarningAndRetries(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		want     []string
		wantNone string
	}{
		{
			name: "other error",
			err:  &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized"},
			want: []string{
				`Could not pause the execution after attempt 1 of step \"s\". Retrying as usual.`,
				"Error: AccessDeniedException: api error AccessDeniedException: not authorized.",
				"A missing lambda:PauseDurableExecution permission on the execution role is one possible cause.",
			},
		},
		{
			name: "plain error",
			err:  errors.New("boom"),
			want: []string{"Error: *errors.errorString: boom.", "one possible cause"},
		},
		{
			name: "API error without a code",
			err:  &smithy.GenericAPIError{Message: "no code"},
			want: []string{"Error: *smithy.GenericAPIError: api error : no code.", "one possible cause"},
		},
		{
			name:     "ResourceConflictException",
			err:      &smithy.GenericAPIError{Code: "ResourceConflictException", Message: "paused"},
			want:     []string{"Error: ResourceConflictException:", "The execution has closed (for example, stopped) or is already paused."},
			wantNone: "permission",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &pausingFake{retryFake: &retryFake{readyAfterRetry: true}, pauseErr: tt.err}
			resp, logs := runPauseHandler(t, fake, stepPayload(`""`), failingStep(pauseOnFirst))
			if n := len(fake.pauseCalls()); n != 1 {
				t.Errorf("pause calls = %d, want 1", n)
			}
			assertRetriedAsUsual(t, resp, fake.retryFake)
			if !strings.Contains(logs, "level=WARN") {
				t.Errorf("logs = %s, want a WARN record", logs)
			}
			for _, w := range tt.want {
				if !strings.Contains(logs, w) {
					t.Errorf("logs = %s, want containing %q", logs, w)
				}
			}
			if tt.wantNone != "" && strings.Contains(logs, tt.wantNone) {
				t.Errorf("logs = %s, want no %q", logs, tt.wantNone)
			}
		})
	}
}

func TestStepClientWithoutPauserRetriesAsUsual(t *testing.T) {
	fake := &retryFake{readyAfterRetry: true}
	var _ ExecutionClient = fake
	if _, ok := any(fake).(ExecutionPauser); ok {
		t.Fatal("retryFake implements ExecutionPauser; the test needs a client without it")
	}
	resp, _ := runPauseHandler(t, fake, stepPayload(`""`), failingStep(pauseOnFirst))
	assertRetriedAsUsual(t, resp, fake)
}

// An abandoned branch records its RETRY, but it must not pause the
// execution, and it must not hang.
func TestAbandonedStepDoesNotPause(t *testing.T) {
	fake := &pausingFake{retryFake: &retryFake{}}
	// started makes the fast branch wait until the slow step runs its
	// body. Without it, the batch can abandon the slow branch before its
	// step starts, and no RETRY is recorded.
	started := make(chan struct{})
	resp, _ := runPauseHandler(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := Parallel(ctx, "p", []Branch[string]{
			{Name: "fast", Func: func(c Context) (string, error) {
				<-started
				return "fast", nil
			}},
			{Name: "slow", Func: func(c Context) (string, error) {
				return Step(c, "slow-step", func(StepContext) (string, error) {
					close(started)
					time.Sleep(200 * time.Millisecond)
					return "", errors.New("transient")
				}, WithRetry(pauseOnFirst))
			}},
		}, WithCompletion(CompletionConfig{MinSuccessful: 1}))
		return "done", err
	})
	if !strings.Contains(resp, `"Status":"SUCCEEDED"`) {
		t.Errorf("response = %s, want SUCCEEDED", resp)
	}
	if n := len(fake.pauseCalls()); n != 0 {
		t.Errorf("pause calls = %d, want 0", n)
	}
	_, updates := fake.recorded()
	sawRetry := false
	for _, u := range updates {
		if u.Action == OperationActionRetry && aws.ToString(u.Name) == "slow-step" {
			sawRetry = true
		}
	}
	if !sawRetry {
		t.Errorf("actions = %v, want the abandoned step's RETRY recorded", actions(updates))
	}
}

func TestPauseClientToken(t *testing.T) {
	hex64 := regexp.MustCompile(`^[0-9a-f]{64}$`)
	a := pauseClientToken("arn:a", "id", 3)
	if !hex64.MatchString(a) {
		t.Errorf("token = %q, want 64 lowercase hex characters", a)
	}
	if b := pauseClientToken("arn:a", "id", 3); a != b {
		t.Errorf("token not stable: %q != %q", a, b)
	}
	for name, other := range map[string]string{
		"other ARN":     pauseClientToken("arn:b", "id", 3),
		"other step":    pauseClientToken("arn:a", "id2", 3),
		"other attempt": pauseClientToken("arn:a", "id", 4),
	} {
		if other == a {
			t.Errorf("%s: token = %q, want a different token", name, other)
		}
	}
}

func TestPauseReasonTruncated(t *testing.T) {
	long := strings.Repeat("é", 2000)
	r := pauseReason(long, "id", 1)
	if n := len([]rune(r)); n != pauseReasonMaxLength {
		t.Errorf("reason length = %d runes, want %d", n, pauseReasonMaxLength)
	}
	if !strings.HasPrefix(r, `Step "é`) {
		t.Errorf("reason = %.20q..., want the step name", r)
	}
}

func TestRetryStrategyPauseEvery(t *testing.T) {
	s := MustNewRetryStrategy(RetryConfig{MaxAttempts: 20, InitialDelay: 10 * time.Second, MaxDelay: 10 * time.Second, BackoffRate: 1, Jitter: JitterNone, PauseEvery: 5})
	for attempt, want := range map[int]bool{1: false, 4: false, 5: true, 6: false, 10: true, 15: true} {
		d := s(RetryAttempt{Err: errors.New("x"), Attempt: attempt})
		if !d.Retry {
			t.Fatalf("attempt %d: Retry = false, want true", attempt)
		}
		if d.Pause != want {
			t.Errorf("attempt %d: Pause = %v, want %v", attempt, d.Pause, want)
		}
		if d.Delay != 10*time.Second {
			t.Errorf("attempt %d: Delay = %v, want 10s", attempt, d.Delay)
		}
	}
	// No pause when the strategy stops.
	if d := s(RetryAttempt{Err: errors.New("x"), Attempt: 20}); d.Retry || d.Pause {
		t.Errorf("last attempt: decision = %+v, want no retry and no pause", d)
	}
	// No pause without the option.
	plain := MustNewRetryStrategy(RetryConfig{MaxAttempts: 20})
	if d := plain(RetryAttempt{Err: errors.New("x"), Attempt: 5}); d.Pause {
		t.Error("PauseEvery unset: Pause = true on attempt 5, want false")
	}
	if d := ExponentialBackoff()(RetryAttempt{Err: errors.New("x"), Attempt: 5}); d.Pause {
		t.Error("ExponentialBackoff: Pause = true, want false")
	}
}

func TestRetryStrategyPauseEveryKeepsJitter(t *testing.T) {
	s := MustNewRetryStrategy(RetryConfig{MaxAttempts: 20, InitialDelay: 100 * time.Second, MaxDelay: 100 * time.Second, BackoffRate: 1, PauseEvery: 5})
	seen := map[time.Duration]bool{}
	for range 200 {
		d := s(RetryAttempt{Err: errors.New("x"), Attempt: 5})
		if !d.Pause {
			t.Fatal("attempt 5: Pause = false, want true")
		}
		if d.Delay < time.Second || d.Delay > 100*time.Second {
			t.Fatalf("attempt 5: Delay = %v, want within the full jitter range", d.Delay)
		}
		seen[d.Delay] = true
	}
	if len(seen) < 10 {
		t.Errorf("attempt 5: %d distinct delays in 200 draws, want full jitter", len(seen))
	}
}
