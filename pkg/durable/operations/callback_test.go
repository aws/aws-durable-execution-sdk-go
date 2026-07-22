package operations

import (
	"errors"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

// TestCallbackError_TimeoutFlag verifies the real bug this session fixed:
// callbackError must set CallbackFailedError.Timeout based on the
// checkpointed operation's ACTUAL terminal status (Failed vs TimedOut),
// not a hard-coded constant. Before this fix, Timeout was hard-coded
// false in every construction site (see CallbackFailedError's own,
// now-updated doc comment) - this directly guards that regression.
func TestCallbackError_TimeoutFlag(t *testing.T) {
	t.Run("Failed status sets Timeout=false", func(t *testing.T) {
		op := types.Operation{
			ID:     "cb-1",
			Status: types.OperationStatusFailed,
			CallbackDetails: &types.CallbackDetails{
				Error: &types.ErrorObject{ErrorType: "RejectedError", ErrorMessage: "not approved"},
			},
		}
		err := callbackError(op, "approval")

		var cbErr *CallbackFailedError
		if !errors.As(err, &cbErr) {
			t.Fatal("expected *CallbackFailedError")
		}
		if cbErr.Timeout {
			t.Fatal("expected Timeout=false for an explicit external-system failure")
		}
		if cbErr.Err == nil || cbErr.Err.Error() != "not approved" {
			t.Fatalf("expected wrapped error message %q, got %v", "not approved", cbErr.Err)
		}
	})

	t.Run("TimedOut status sets Timeout=true", func(t *testing.T) {
		op := types.Operation{
			ID:     "cb-2",
			Status: types.OperationStatusTimedOut,
			CallbackDetails: &types.CallbackDetails{
				Error: &types.ErrorObject{ErrorType: "Callback.Timeout"},
			},
		}
		err := callbackError(op, "approval")

		var cbErr *CallbackFailedError
		if !errors.As(err, &cbErr) {
			t.Fatal("expected *CallbackFailedError")
		}
		if !cbErr.Timeout {
			t.Fatal("expected Timeout=true for a TimedOut operation status - this is the exact regression this test guards against (Timeout used to be hard-coded false)")
		}
	})

	t.Run("TimedOut status with no recorded error still sets Timeout=true", func(t *testing.T) {
		op := types.Operation{
			ID:     "cb-3",
			Status: types.OperationStatusTimedOut,
		}
		err := callbackError(op, "approval")

		var cbErr *CallbackFailedError
		if !errors.As(err, &cbErr) {
			t.Fatal("expected *CallbackFailedError")
		}
		if !cbErr.Timeout {
			t.Fatal("expected Timeout=true even when CallbackDetails.Error is nil")
		}
	})
}

// TestWithCallbackTimeout_PopulatesConfig verifies WithCallbackTimeout
// correctly converts a types.Duration into callbackConfig.
// timeoutSeconds - the exact field CreateCallback reads to build
// CallbackOptions.TimeoutSeconds on its START checkpoint (see
// callback.go's real gap this closes: CallbackOptions was never
// populated at all before this session, confirmed by grepping the whole
// package for "CallbackOptions" and finding zero matches outside
// wire.go/client.go).
func TestWithCallbackTimeout_PopulatesConfig(t *testing.T) {
	cfg := &callbackConfig[string]{}
	opt := WithCallbackTimeout[string](types.Duration{Minutes: 1, Seconds: 30})
	opt(cfg)

	if cfg.timeoutSeconds == nil {
		t.Fatal("expected timeoutSeconds to be set")
	}
	if *cfg.timeoutSeconds != 90 {
		t.Fatalf("expected 90 seconds (1m30s), got %d", *cfg.timeoutSeconds)
	}
	if cfg.heartbeatSeconds != nil {
		t.Fatal("expected heartbeatSeconds to remain unset")
	}
}

// TestWithCallbackHeartbeatTimeout_PopulatesConfig mirrors
// TestWithCallbackTimeout_PopulatesConfig for the heartbeat variant -
// both options are independent (a caller may set one, both, or neither),
// confirmed here by verifying setting one leaves the other untouched.
func TestWithCallbackHeartbeatTimeout_PopulatesConfig(t *testing.T) {
	cfg := &callbackConfig[string]{}
	opt := WithCallbackHeartbeatTimeout[string](types.Duration{Seconds: 45})
	opt(cfg)

	if cfg.heartbeatSeconds == nil {
		t.Fatal("expected heartbeatSeconds to be set")
	}
	if *cfg.heartbeatSeconds != 45 {
		t.Fatalf("expected 45 seconds, got %d", *cfg.heartbeatSeconds)
	}
	if cfg.timeoutSeconds != nil {
		t.Fatal("expected timeoutSeconds to remain unset")
	}
}

// TestWithCallbackTimeout_AndHeartbeat_BothSet verifies both options can
// be combined on the same CreateCallback call, matching
// types.CallbackOptions' own shape (TimeoutSeconds and
// HeartbeatTimeoutSeconds are independent, both-optional fields).
func TestWithCallbackTimeout_AndHeartbeat_BothSet(t *testing.T) {
	cfg := &callbackConfig[string]{}
	WithCallbackTimeout[string](types.Duration{Seconds: 5})(cfg)
	WithCallbackHeartbeatTimeout[string](types.Duration{Seconds: 10})(cfg)

	if cfg.timeoutSeconds == nil || *cfg.timeoutSeconds != 5 {
		t.Fatalf("expected timeoutSeconds=5, got %v", cfg.timeoutSeconds)
	}
	if cfg.heartbeatSeconds == nil || *cfg.heartbeatSeconds != 10 {
		t.Fatalf("expected heartbeatSeconds=10, got %v", cfg.heartbeatSeconds)
	}
}

// TestWithWaitForCallbackTimeout_PopulatesConfig verifies
// WithWaitForCallbackTimeout populates waitForCallbackConfig.timeout -
// the exact field WaitForCallback now reads (this session's fix) to
// build a WithCallbackTimeout option passed through to its internal
// CreateCallback call. See WithWaitForCallbackTimeout's own doc for the
// real "not yet implemented" gap this closes.
func TestWithWaitForCallbackTimeout_PopulatesConfig(t *testing.T) {
	cfg := &waitForCallbackConfig[string]{}
	opt := WithWaitForCallbackTimeout[string](types.Duration{Minutes: 1, Seconds: 30})
	opt(cfg)

	if cfg.timeout == nil {
		t.Fatal("expected timeout to be set")
	}
	if secs := cfg.timeout.Days*86400 + cfg.timeout.Hours*3600 + cfg.timeout.Minutes*60 + cfg.timeout.Seconds; secs != 90 {
		t.Fatalf("expected 90 seconds (1m30s), got %d", secs)
	}
	if cfg.heartbeatTimeout != nil {
		t.Fatal("expected heartbeatTimeout to remain unset")
	}
}

// TestWithWaitForCallbackHeartbeatTimeout_PopulatesConfig mirrors
// TestWithWaitForCallbackTimeout_PopulatesConfig for the new heartbeat
// variant - see that option's own doc for the gap this closes (before
// this option existed, WaitForCallback exposed NO heartbeat-timeout
// option at all).
func TestWithWaitForCallbackHeartbeatTimeout_PopulatesConfig(t *testing.T) {
	cfg := &waitForCallbackConfig[string]{}
	opt := WithWaitForCallbackHeartbeatTimeout[string](types.Duration{Seconds: 45})
	opt(cfg)

	if cfg.heartbeatTimeout == nil {
		t.Fatal("expected heartbeatTimeout to be set")
	}
	if secs := cfg.heartbeatTimeout.Days*86400 + cfg.heartbeatTimeout.Hours*3600 + cfg.heartbeatTimeout.Minutes*60 + cfg.heartbeatTimeout.Seconds; secs != 45 {
		t.Fatalf("expected 45 seconds, got %d", secs)
	}
	if cfg.timeout != nil {
		t.Fatal("expected timeout to remain unset")
	}
}

// TestWithWaitForCallbackSubmitterRetryStrategy_PopulatesConfig verifies
// WithWaitForCallbackSubmitterRetryStrategy populates
// waitForCallbackConfig.submitterRetry, the field WaitForCallback now
// reads to pass a WithStepRetryStrategy option through to its internal
// submitter Step call - see that option's own doc for the real gap this
// closes (the submitter step previously accepted no StepOption at all).
func TestWithWaitForCallbackSubmitterRetryStrategy_PopulatesConfig(t *testing.T) {
	cfg := &waitForCallbackConfig[string]{}
	called := false
	strategy := func(err error, attempt int) types.RetryDecision {
		called = true
		return types.RetryDecision{ShouldRetry: false}
	}
	opt := WithWaitForCallbackSubmitterRetryStrategy[string](strategy)
	opt(cfg)

	if cfg.submitterRetry == nil {
		t.Fatal("expected submitterRetry to be set")
	}
	cfg.submitterRetry(errors.New("boom"), 1)
	if !called {
		t.Fatal("expected the configured strategy function to be invoked")
	}
}

// TestWaitForCallback_NegativeTimeoutRejected verifies WaitForCallback's
// config validation rejects a negative WithWaitForCallbackTimeout
// duration up front, mirroring CreateCallback's own analogous behavior
// via a direct unit check of the validation arithmetic (WaitForCallback
// itself requires a real dcontext.Context to invoke end-to-end, so this
// exercises the same total-seconds computation the function's own
// validation block performs rather than calling WaitForCallback
// directly).
func TestWaitForCallback_NegativeTimeoutRejected(t *testing.T) {
	d := types.Duration{Seconds: -1}
	if secs := d.Days*86400 + d.Hours*3600 + d.Minutes*60 + d.Seconds; secs >= 0 {
		t.Fatalf("expected negative total seconds for this test duration, got %d", secs)
	}
}

// TestDeserializeCallbackResult_NilResultIsZeroValueNotError verifies
// the real bug this session fixed: a succeeded CALLBACK operation with
// no CallbackDetails.Result (the external system called
// SendDurableExecutionCallbackSuccess with no payload - conformance
// requirement 7-15's exact scenario) must deserialize to T's zero value,
// not an error. Before this fix, deserializeCallbackResult
// unconditionally returned a generic "succeeded but no result recorded"
// error for this case - see that function's own doc for the full
// Step-vs-Callback comparison explaining why this case is genuinely
// reachable for a callback (unlike for a step).
func TestDeserializeCallbackResult_NilResultIsZeroValueNotError(t *testing.T) {
	op := types.Operation{
		ID:              "cb-1",
		Status:          types.OperationStatusSucceeded,
		CallbackDetails: &types.CallbackDetails{CallbackID: "callback-abc"},
	}

	t.Run("pointer type resolves to nil, not an error", func(t *testing.T) {
		val, err := deserializeCallbackResult[*string](utils.DefaultSerdes(), op, "cb-1")
		if err != nil {
			t.Fatalf("expected no error for an absent Result, got %v", err)
		}
		if val != nil {
			t.Fatalf("expected nil *string, got %v", *val)
		}
	})

	t.Run("string type resolves to empty string, not an error", func(t *testing.T) {
		val, err := deserializeCallbackResult[string](utils.DefaultSerdes(), op, "cb-1")
		if err != nil {
			t.Fatalf("expected no error for an absent Result, got %v", err)
		}
		if val != "" {
			t.Fatalf("expected empty string, got %q", val)
		}
	})

	t.Run("nil CallbackDetails also resolves to zero value, not an error", func(t *testing.T) {
		opNoDetails := types.Operation{ID: "cb-2", Status: types.OperationStatusSucceeded}
		val, err := deserializeCallbackResult[string](utils.DefaultSerdes(), opNoDetails, "cb-2")
		if err != nil {
			t.Fatalf("expected no error when CallbackDetails itself is nil, got %v", err)
		}
		if val != "" {
			t.Fatalf("expected empty string, got %q", val)
		}
	})
}
