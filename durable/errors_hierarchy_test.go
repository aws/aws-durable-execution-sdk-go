package durable

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// TestCallbackSubtypeWireNames pins the wire ErrorType of each callback
// failure mode to the names the other Durable Execution SDKs use.
func TestCallbackSubtypeWireNames(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{&CallbackError{Name: "c"}, "CallbackError"},
		{&CallbackExternalError{CallbackError{Name: "c"}}, "CallbackExternalError"},
		{&CallbackTimeoutError{CallbackError: CallbackError{Name: "c"}}, "CallbackTimeoutError"},
		{&CallbackSubmitterError{CallbackError{Name: "c"}}, "CallbackSubmitterError"},
	}
	seen := map[string]bool{}
	for _, tt := range tests {
		if got := wireErrorType(tt.err); got != tt.want {
			t.Errorf("wireErrorType(%T) = %q, want %q", tt.err, got, tt.want)
		}
		if seen[tt.want] {
			t.Errorf("wire name %q is not distinct", tt.want)
		}
		seen[tt.want] = true
		if got := errorObjectFromError(tt.err, nil).ErrorType; got != tt.want {
			t.Errorf("FAILED response ErrorType for %T = %q, want %q", tt.err, got, tt.want)
		}
	}
}

// TestCallbackSubtypesMatchBaseTypes verifies that errors.As against
// *CallbackError matches all three subtypes, and that *OperationError
// matches every typed operation error, carrying the shared fields.
func TestCallbackSubtypesMatchBaseTypes(t *testing.T) {
	rec := errorRecord{errType: "RejectedError", message: "not approved", data: `{"k":1}`}
	subtypes := []error{
		newCallbackExternalError("cb", "id-1", rec),
		newCallbackTimeoutError("cb", "id-1", rec),
		newCallbackSubmitterError("cb", "id-1", rec),
	}
	for _, err := range subtypes {
		var cbErr *CallbackError
		if !errors.As(err, &cbErr) {
			t.Fatalf("errors.As(%T, *CallbackError) = false", err)
		}
		if cbErr.Name != "cb" || cbErr.CallbackID != "id-1" || cbErr.ErrorType != "RejectedError" || cbErr.Message != "not approved" || cbErr.ErrorData != `{"k":1}` {
			t.Errorf("%T: CallbackError fields = %+v", err, *cbErr)
		}
		var opErr *OperationError
		if !errors.As(err, &opErr) {
			t.Fatalf("errors.As(%T, *OperationError) = false", err)
		}
		if opErr.Name != "cb" || opErr.ErrorType != "RejectedError" || opErr.Message != "not approved" || opErr.ErrorData != `{"k":1}` || opErr.Err == nil {
			t.Errorf("%T: OperationError fields = %+v", err, *opErr)
		}
	}

	everything := []error{
		newStepError("s", 2, rec),
		&StepInterruptedError{Name: "s"},
		&InvokeError{Name: "i", ErrorType: "E", Message: "m", Err: rec.standIn(nil)},
		newChildContextError("c", rec),
		newWaitForConditionError("w", 1, rec),
		&CombinatorError{Name: "any"},
		&BatchError{Name: "b", Reason: CompletionAllCompleted},
		&NonDeterministicReplayError{Name: "n"},
		&ResultTooLargeError{Name: "r"},
	}
	everything = append(everything, subtypes...)
	for _, err := range everything {
		var opErr *OperationError
		if !errors.As(err, &opErr) {
			t.Errorf("errors.As(%T, *OperationError) = false", err)
		}
	}
}

// TestCallbackTimeoutErrorHeartbeat verifies the Heartbeat discriminator is
// derived from the recorded timeout message or from an older heartbeat
// ErrorType, and that both timeout kinds match the sentinel.
func TestCallbackTimeoutErrorHeartbeat(t *testing.T) {
	tests := []struct {
		errType   string
		message   string
		heartbeat bool
		wantType  string
	}{
		{"", "Callback timed out", false, "CallbackTimeoutError"},
		{"", "Callback timed out on heartbeat", true, "CallbackTimeoutError"},
		{"", "", false, "CallbackTimeoutError"},
		{legacyCallbackTimeoutType, "", false, legacyCallbackTimeoutType},
		{legacyCallbackHeartbeatType, "", true, legacyCallbackHeartbeatType},
		{legacyCallbackHeartbeatType, "callback heartbeat timed out", true, legacyCallbackHeartbeatType},
	}
	for _, tt := range tests {
		err := newCallbackTimeoutError("cb", "id", errorRecord{errType: tt.errType, message: tt.message})
		if err.Heartbeat != tt.heartbeat {
			t.Errorf("type %q message %q: Heartbeat = %v, want %v", tt.errType, tt.message, err.Heartbeat, tt.heartbeat)
		}
		if !errors.Is(err, ErrCallbackTimedOut) {
			t.Errorf("type %q message %q: errors.Is(ErrCallbackTimedOut) = false", tt.errType, tt.message)
		}
		if err.ErrorType != tt.wantType {
			t.Errorf("type %q: ErrorType = %q, want %q", tt.errType, err.ErrorType, tt.wantType)
		}
		wantMsg := tt.message
		if wantMsg == "" {
			wantMsg = callbackTimedOutMessage
		}
		if err.Message != wantMsg {
			t.Errorf("message %q: Message = %q, want %q", tt.message, err.Message, wantMsg)
		}
	}
}

// TestLegacyCallbackTimeoutNamesReconstruct verifies that a record written
// under an older callback timeout name rebuilds as a CallbackTimeoutError
// wherever records are rebuilt by name: as a child-context cause and as a
// rejected Settled value.
func TestLegacyCallbackTimeoutNamesReconstruct(t *testing.T) {
	for _, legacy := range []string{legacyCallbackTimeoutType, legacyCallbackHeartbeatType} {
		wantHeartbeat := legacy == legacyCallbackHeartbeatType

		childErr := newChildContextError("c", errorRecord{errType: legacy})
		var timeoutErr *CallbackTimeoutError
		if !errors.As(childErr, &timeoutErr) {
			t.Fatalf("%s as child cause: errors.As(*CallbackTimeoutError) = false; cause %T", legacy, childErr.Err)
		}
		if timeoutErr.Heartbeat != wantHeartbeat || !errors.Is(childErr, ErrCallbackTimedOut) {
			t.Errorf("%s as child cause: Heartbeat = %v, Is(sentinel) = %v", legacy, timeoutErr.Heartbeat, errors.Is(childErr, ErrCallbackTimedOut))
		}

		raw := `{"status":"rejected","error":"x","errorType":"` + legacy + `","operation":{"name":"cb","errorType":"` + legacy + `"}}`
		var settled Settled[string]
		if err := json.Unmarshal([]byte(raw), &settled); err != nil {
			t.Fatal(err)
		}
		timeoutErr = nil
		if !errors.As(settled.Err, &timeoutErr) {
			t.Fatalf("%s in Settled: deserialized error = %T, want *CallbackTimeoutError", legacy, settled.Err)
		}
		if timeoutErr.Name != "cb" || timeoutErr.Heartbeat != wantHeartbeat || !errors.Is(settled.Err, ErrCallbackTimedOut) {
			t.Errorf("%s in Settled: Name = %q, Heartbeat = %v, Is(sentinel) = %v", legacy, timeoutErr.Name, timeoutErr.Heartbeat, errors.Is(settled.Err, ErrCallbackTimedOut))
		}
	}
}

// TestSettledRoundTripsErrorType covers the Settled acceptance criterion:
// SDK error types are rebuilt by name, sentinels survive, unknown names
// yield the documented fallback, and the older message-only form still
// deserializes.
func TestSettledRoundTripsErrorType(t *testing.T) {
	roundTrip := func(t *testing.T, in Settled[string]) Settled[string] {
		t.Helper()
		raw, err := json.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		var out Settled[string]
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	t.Run("step error", func(t *testing.T) {
		in := Settled[string]{Err: newStepError("s", 3, errorRecord{errType: "PaymentDeclinedError", message: "declined", data: "d"})}
		out := roundTrip(t, in)
		var stepErr *StepError
		if !errors.As(out.Err, &stepErr) {
			t.Fatalf("deserialized error = %T, want *StepError", out.Err)
		}
		if stepErr.Name != "s" || stepErr.ErrorType != "PaymentDeclinedError" || stepErr.Message != "declined" || stepErr.ErrorData != "d" {
			t.Errorf("StepError fields = %+v", *stepErr)
		}
		if raw, _ := json.Marshal(in); !strings.Contains(string(raw), `"errorType":"StepError"`) {
			t.Errorf("serialized form lacks errorType: %s", raw)
		}
	})

	t.Run("child context error keeps its nested step error", func(t *testing.T) {
		// A child context whose step failed records ErrorType "StepError".
		// On the first invocation the cause is a rebuilt *StepError. The
		// Settled round trip must rebuild the same shape, so errors.As
		// against the inner type answers the same before and after the
		// checkpoint.
		in := Settled[string]{Err: newChildContextError("child", errorRecord{errType: "StepError", message: "declined", data: "d"})}
		var liveStep *StepError
		if !errors.As(in.Err, &liveStep) {
			t.Fatalf("live cause = %T, want *StepError", errors.Unwrap(in.Err))
		}
		out := roundTrip(t, in)
		var childErr *ChildContextError
		if !errors.As(out.Err, &childErr) {
			t.Fatalf("deserialized error = %T, want *ChildContextError", out.Err)
		}
		if childErr.Name != "child" || childErr.ErrorType != "StepError" || childErr.Message != "declined" || childErr.ErrorData != "d" {
			t.Errorf("ChildContextError fields = %+v", *childErr)
		}
		var stepErr *StepError
		if !errors.As(out.Err, &stepErr) {
			t.Fatalf("deserialized cause = %T, want *StepError", childErr.Err)
		}
		if stepErr.ErrorType != liveStep.ErrorType || stepErr.Message != liveStep.Message || stepErr.ErrorData != liveStep.ErrorData {
			t.Errorf("inner StepError after round trip = %+v, live = %+v", *stepErr, *liveStep)
		}
		if out.Err.Error() != in.Err.Error() {
			t.Errorf("Error() = %q, want %q", out.Err.Error(), in.Err.Error())
		}

		// A record naming only the outer type ends the chain at a leaf.
		self := roundTrip(t, Settled[string]{Err: newChildContextError("child", errorRecord{errType: "ChildContextError", message: "m"})})
		if !errors.As(self.Err, &childErr) {
			t.Fatalf("deserialized error = %T, want *ChildContextError", self.Err)
		}
		if _, ok := childErr.Err.(*replayedError); !ok {
			t.Errorf("self-named cause = %T, want leaf stand-in", childErr.Err)
		}
	})

	t.Run("callback timeout keeps sentinel", func(t *testing.T) {
		in := Settled[string]{Err: newCallbackTimeoutError("cb", "id", errorRecord{message: "Callback timed out on heartbeat"})}
		out := roundTrip(t, in)
		var timeoutErr *CallbackTimeoutError
		if !errors.As(out.Err, &timeoutErr) {
			t.Fatalf("deserialized error = %T, want *CallbackTimeoutError", out.Err)
		}
		if !errors.Is(out.Err, ErrCallbackTimedOut) || !timeoutErr.Heartbeat {
			t.Errorf("sentinel/heartbeat lost: Is = %v, Heartbeat = %v", errors.Is(out.Err, ErrCallbackTimedOut), timeoutErr.Heartbeat)
		}
	})

	t.Run("operation error", func(t *testing.T) {
		in := Settled[string]{Err: &OperationError{Name: "op", ErrorType: "QuotaError", Message: "over quota", ErrorData: "d", Err: errors.New("over quota")}}
		out := roundTrip(t, in)
		opErr, ok := out.Err.(*OperationError)
		if !ok {
			t.Fatalf("deserialized error = %T, want *OperationError", out.Err)
		}
		if opErr.Name != "op" || opErr.ErrorType != "QuotaError" || opErr.Message != "over quota" || opErr.ErrorData != "d" {
			t.Errorf("OperationError fields = %+v", *opErr)
		}
		if opErr.Err == nil || opErr.Err.Error() != "QuotaError: over quota" {
			t.Errorf("stand-in = %v", opErr.Err)
		}

		bare := roundTrip(t, Settled[string]{Err: &OperationError{Name: "op"}})
		if opErr, ok := bare.Err.(*OperationError); !ok || opErr.Name != "op" || opErr.ErrorType != "" || opErr.Err != nil {
			t.Errorf("OperationError without a cause = %#v", bare.Err)
		}
	})

	t.Run("batch error", func(t *testing.T) {
		in := Settled[string]{Err: &BatchError{Name: "b", Reason: CompletionFailureToleranceExceeded, Errors: []error{errors.New("x")}}}
		out := roundTrip(t, in)
		var batchErr *BatchError
		if !errors.As(out.Err, &batchErr) {
			t.Fatalf("deserialized error = %T, want *BatchError", out.Err)
		}
		if batchErr.Reason != CompletionFailureToleranceExceeded {
			t.Errorf("Reason = %v, want %v", batchErr.Reason, CompletionFailureToleranceExceeded)
		}
		if out.Err.Error() != in.Err.Error() {
			t.Errorf("Error() = %q, want %q", out.Err.Error(), in.Err.Error())
		}
	})

	t.Run("non-deterministic replay error", func(t *testing.T) {
		in := Settled[string]{Err: &NonDeterministicReplayError{
			Name: "n", StepID: "1.2", ExpectedType: "STEP", ActualType: "WAIT",
		}}
		out := roundTrip(t, in)
		var ndErr *NonDeterministicReplayError
		if !errors.As(out.Err, &ndErr) {
			t.Fatalf("deserialized error = %T, want *NonDeterministicReplayError", out.Err)
		}
		if ndErr.Name != "n" {
			t.Errorf("Name = %q, want n", ndErr.Name)
		}
		if out.Err.Error() != in.Err.Error() {
			t.Errorf("Error() = %q, want %q", out.Err.Error(), in.Err.Error())
		}
	})

	t.Run("result too large error", func(t *testing.T) {
		in := Settled[string]{Err: &ResultTooLargeError{Name: "r", SizeBytes: 900_000, LimitBytes: resultSizeLimitBytes}}
		out := roundTrip(t, in)
		var tooLarge *ResultTooLargeError
		if !errors.As(out.Err, &tooLarge) {
			t.Fatalf("deserialized error = %T, want *ResultTooLargeError", out.Err)
		}
		if tooLarge.Name != "r" {
			t.Errorf("Name = %q, want r", tooLarge.Name)
		}
		if out.Err.Error() != in.Err.Error() {
			t.Errorf("Error() = %q, want %q", out.Err.Error(), in.Err.Error())
		}
		var opErr *OperationError
		if !errors.As(out.Err, &opErr) || opErr.Name != "r" {
			t.Errorf("errors.As(*OperationError) after round trip failed: %v", out.Err)
		}
	})

	t.Run("every SDK wire name rebuilds its type", func(t *testing.T) {
		// Every error type that serializes under an SDK wire name and is
		// matchable as *OperationError must deserialize as itself.
		for _, in := range []error{
			newStepError("s", 1, errorRecord{errType: "E", message: "m"}),
			&StepInterruptedError{Name: "s"},
			&InvokeError{Name: "i", ErrorType: "E", Message: "m", Err: errors.New("m")},
			&CallbackError{Name: "c", ErrorType: "E", Message: "m", Err: errors.New("m")},
			newCallbackExternalError("c", "id", errorRecord{errType: "E", message: "m"}),
			newCallbackTimeoutError("c", "id", errorRecord{}),
			newCallbackSubmitterError("c", "id", errorRecord{errType: "E", message: "m"}),
			newChildContextError("c", errorRecord{errType: "E", message: "m"}),
			newWaitForConditionError("w", 1, errorRecord{errType: "E", message: "m"}),
			&CombinatorError{Name: "any", Errors: []error{errors.New("m")}},
			&BatchError{Name: "b", Reason: CompletionAllCompleted, Errors: []error{errors.New("m")}},
			&OperationError{Name: "o", ErrorType: "E", Message: "m", Err: errors.New("m")},
			&NonDeterministicReplayError{Name: "n"},
			&ResultTooLargeError{Name: "r"},
		} {
			out := roundTrip(t, Settled[string]{Err: in})
			if got, want := reflect.TypeOf(out.Err), reflect.TypeOf(in); got != want {
				t.Errorf("%v round-trips as %v", want, got)
			}
		}
	})

	t.Run("unknown type falls back to stand-in", func(t *testing.T) {
		in := Settled[string]{Err: &userDefinedError{"boom"}}
		out := roundTrip(t, in)
		re, ok := out.Err.(*replayedError)
		if !ok {
			t.Fatalf("deserialized error = %T, want stand-in", out.Err)
		}
		if re.errType != "userDefinedError" || re.message != "boom" {
			t.Errorf("stand-in = %+v", *re)
		}
		if out.Err.Error() != "userDefinedError: boom" {
			t.Errorf("Error() = %q", out.Err.Error())
		}

		// A second round trip must not prefix the type name again.
		again := roundTrip(t, out)
		if again.Err.Error() != "userDefinedError: boom" {
			t.Errorf("Error() after second round trip = %q, want %q", again.Err.Error(), "userDefinedError: boom")
		}
		if re, ok := again.Err.(*replayedError); !ok || re.errType != "userDefinedError" || re.message != "boom" {
			t.Errorf("stand-in after second round trip = %#v", again.Err)
		}
	})

	t.Run("legacy message-only form", func(t *testing.T) {
		var out Settled[string]
		if err := json.Unmarshal([]byte(`{"status":"rejected","error":"old failure"}`), &out); err != nil {
			t.Fatal(err)
		}
		re, ok := out.Err.(*replayedError)
		if !ok {
			t.Fatalf("deserialized error = %T, want stand-in", out.Err)
		}
		if re.errType != "Error" || re.message != "old failure" {
			t.Errorf("stand-in = %+v", *re)
		}

		// Re-serializing a legacy value must keep its raw message, so a
		// second round trip yields the same stand-in.
		again := roundTrip(t, out)
		if re, ok := again.Err.(*replayedError); !ok || re.errType != "Error" || re.message != "old failure" {
			t.Errorf("stand-in after second round trip = %#v", again.Err)
		}
		if again.Err.Error() != out.Err.Error() {
			t.Errorf("Error() after second round trip = %q, want %q", again.Err.Error(), out.Err.Error())
		}
	})

	t.Run("fulfilled", func(t *testing.T) {
		out := roundTrip(t, Settled[string]{Value: "v"})
		if out.Err != nil || out.Value != "v" {
			t.Errorf("fulfilled round trip = %+v", out)
		}
	})
}

// TestWithErrorData covers the public ErrorData mechanism: the wrapper is
// transparent to ErrorType and errors.Is, the outermost payload wins, and
// oversized payloads are truncated on a UTF-8 boundary.
func TestWithErrorData(t *testing.T) {
	base := &userDefinedError{"inner"}
	err := WithErrorData(base, "payload")
	if !errors.Is(err, base) {
		t.Error("WithErrorData hides the wrapped error from errors.Is")
	}
	if got := wireErrorType(err); got != "userDefinedError" {
		t.Errorf("wireErrorType = %q, want userDefinedError", got)
	}
	if got := errorDataOf(err); got != "payload" {
		t.Errorf("errorDataOf = %q, want payload", got)
	}
	if got := errorDataOf(WithErrorData(err, "outer")); got != "outer" {
		t.Errorf("outermost data = %q, want outer", got)
	}
	if got := errorDataOf(WithErrorData(err, "")); got != "payload" {
		t.Errorf("empty outer data must not hide inner data; got %q", got)
	}
	if WithErrorData(nil, "x") != nil {
		t.Error("WithErrorData(nil) must return nil")
	}
	if obj := errorObject(err); obj.ErrorData == nil || *obj.ErrorData != "payload" {
		t.Errorf("checkpoint ErrorData = %v, want payload", obj.ErrorData)
	}
	if obj := errorObjectFromError(err, nil); obj.ErrorData != "payload" {
		t.Errorf("FAILED response ErrorData = %q, want payload", obj.ErrorData)
	}

	// Oversized data is truncated on a rune boundary.
	big := strings.Repeat("é", maxErrorDataBytes) // 2 bytes per rune
	got := errorDataOf(WithErrorData(base, big))
	if len(got) > maxErrorDataBytes {
		t.Errorf("truncated data is %d bytes, want at most %d", len(got), maxErrorDataBytes)
	}
	if !strings.HasSuffix(got, "é") || strings.Count(got, "é")*2 != len(got) {
		t.Error("truncation split a multi-byte rune")
	}
}

// TestChildContextCauseRebuiltByName verifies that an SDK error escaping a
// child context is rebuilt as that type from the record, so errors.As
// reaches it on the first invocation and on replay, while a user type is
// exposed by name only.
func TestChildContextCauseRebuiltByName(t *testing.T) {
	live := newChildContextError("c", recordOf(&CombinatorError{Name: "c", Errors: []error{errors.New("a")}}))
	replay := newChildContextError("c", errorRecord{errType: "PromiseCombinatorError", message: live.Message})
	for _, err := range []*ChildContextError{live, replay} {
		var combErr *CombinatorError
		if !errors.As(err, &combErr) || combErr.Name != "c" {
			t.Errorf("errors.As(*CombinatorError) failed for %v", err)
		}
		if err.ErrorType != "PromiseCombinatorError" {
			t.Errorf("ErrorType = %q", err.ErrorType)
		}
	}
	if live.Error() != replay.Error() {
		t.Errorf("Error() differs: live %q, replay %q", live.Error(), replay.Error())
	}

	user := newChildContextError("c", recordOf(&userDefinedError{"boom"}))
	var ue *userDefinedError
	if errors.As(user, &ue) {
		t.Error("user type reachable through the stand-in")
	}
	if user.ErrorType != "userDefinedError" || user.Message != "boom" {
		t.Errorf("user failure record = %q / %q", user.ErrorType, user.Message)
	}
}
