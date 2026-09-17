// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTimestampUnmarshal(t *testing.T) {
	want := time.Date(2024, 3, 5, 6, 7, 8, 0, time.UTC)
	tests := []struct {
		name      string
		data      string
		wantValid bool
	}{
		{name: "RFC3339 string", data: `"2024-03-05T06:07:08Z"`, wantValid: true},
		{name: "epoch milliseconds", data: `1709618828000`, wantValid: true},
		{name: "null", data: `null`},
		{name: "empty string", data: `""`},
		{name: "unparseable string", data: `"not a time"`},
		{name: "non-integer number", data: `1.5`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ts Timestamp
			if err := json.Unmarshal([]byte(tt.data), &ts); err != nil {
				t.Fatalf("Unmarshal(%s) error: %v", tt.data, err)
			}
			if ts.Valid != tt.wantValid {
				t.Fatalf("Valid = %v, want %v", ts.Valid, tt.wantValid)
			}
			if tt.wantValid && !ts.Time.Equal(want) {
				t.Errorf("Time = %v, want %v", ts.Time, want)
			}
		})
	}
}

func TestTimestampMarshal(t *testing.T) {
	valid := Timestamp{Time: time.Date(2024, 3, 5, 6, 7, 8, 0, time.UTC), Valid: true}
	got, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `"2024-03-05T06:07:08Z"` {
		t.Errorf("Marshal(valid) = %s", got)
	}
	got, err = json.Marshal(Timestamp{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "null" {
		t.Errorf("Marshal(zero) = %s, want null", got)
	}
}

// TestOperationJSONNames pins the wire field names. Both the durable
// package and durabletest depend on these exact keys; a renamed tag would
// silently drop a field on decode.
func TestOperationJSONNames(t *testing.T) {
	in := InvocationInput{
		DurableExecutionArn: "arn",
		CheckpointToken:     "tok",
		UpdatedOperationIds: []string{"a"},
		InitialExecutionState: InitialExecutionState{
			NextMarker: "next",
			Operations: []Operation{{
				Id:                   "id",
				ParentId:             "parent",
				Status:               "SUCCEEDED",
				Type:                 "STEP",
				SubType:              "Step",
				Name:                 "n",
				StartTimestamp:       Timestamp{Time: time.UnixMilli(1000).UTC(), Valid: true},
				ExecutionDetails:     &ExecutionDetails{InputPayload: "in"},
				StepDetails:          &StepDetails{Attempt: 2, Result: "r", Error: &ErrorObject{ErrorType: "T", ErrorMessage: "M", ErrorData: "D"}},
				ChainedInvokeDetails: &ChainedInvokeDetails{Result: "ir"},
				ContextDetails:       &ContextDetails{Result: "cr", ReplayChildren: true},
				CallbackDetails:      &CallbackDetails{CallbackId: "cb", Result: "cbr"},
			}},
		},
	}
	got, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"DurableExecutionArn":"arn","CheckpointToken":"tok","UpdatedOperationIds":["a"],` +
		`"InitialExecutionState":{"Operations":[{"Id":"id","ParentId":"parent","Status":"SUCCEEDED",` +
		`"Type":"STEP","SubType":"Step","Name":"n","StartTimestamp":"1970-01-01T00:00:01Z","EndTimestamp":null,` +
		`"ExecutionDetails":{"InputPayload":"in"},` +
		`"StepDetails":{"Attempt":2,"Result":"r","Error":{"ErrorType":"T","ErrorMessage":"M","ErrorData":"D"}},` +
		`"ChainedInvokeDetails":{"Result":"ir"},"ContextDetails":{"Result":"cr","ReplayChildren":true},` +
		`"CallbackDetails":{"CallbackId":"cb","Result":"cbr"}}],"NextMarker":"next"}}`
	if string(got) != want {
		t.Errorf("Marshal =\n%s\nwant\n%s", got, want)
	}

	var back InvocationInput
	if err := json.Unmarshal(got, &back); err != nil {
		t.Fatal(err)
	}
	// Timestamps survive as time values; compare the rest structurally by
	// re-encoding.
	again, err := json.Marshal(back)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != want {
		t.Errorf("round trip changed the payload:\n%s", again)
	}
}

func TestInvocationResponseJSON(t *testing.T) {
	result := `{"ok":true}`
	tests := []struct {
		name string
		in   InvocationResponse
		want string
	}{
		{name: "pending", in: InvocationResponse{Status: StatusPending}, want: `{"Status":"PENDING"}`},
		{name: "succeeded", in: InvocationResponse{Status: StatusSucceeded, Result: &result}, want: `{"Status":"SUCCEEDED","Result":"{\"ok\":true}"}`},
		{
			name: "failed",
			in:   InvocationResponse{Status: StatusFailed, Error: &ErrorObject{ErrorType: "E", ErrorMessage: "m", StackTrace: []string{"f1", "f2"}}},
			want: `{"Status":"FAILED","Error":{"ErrorType":"E","ErrorMessage":"m","StackTrace":["f1","f2"]}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tt.want {
				t.Errorf("Marshal = %s, want %s", got, tt.want)
			}
		})
	}
}
