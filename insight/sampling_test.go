package insight

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestSampledIn_RateOne_AlwaysTrue(t *testing.T) {
	for i := 0; i < 100; i++ {
		arn := "arn:aws:lambda:us-east-1:123456789012:function:f:" + strconv.Itoa(i)
		if !sampledIn(arn, 1.0) {
			t.Fatalf("expected rate=1.0 to always sample in, got false for %s", arn)
		}
	}
}

func TestSampledIn_RateZero_AlwaysFalse(t *testing.T) {
	for i := 0; i < 100; i++ {
		arn := "arn:aws:lambda:us-east-1:123456789012:function:f:" + strconv.Itoa(i)
		if sampledIn(arn, 0) {
			t.Fatalf("expected rate=0 to always sample out, got true for %s", arn)
		}
	}
}

func TestSampledIn_DeterministicAcrossCalls(t *testing.T) {
	arn := "arn:aws:lambda:us-east-1:123456789012:function:f:exec-42"
	first := sampledIn(arn, 0.5)
	for i := 0; i < 20; i++ {
		if got := sampledIn(arn, 0.5); got != first {
			t.Fatalf("expected the same ARN at the same rate to always reach the same decision (deterministic across replays), got %v then %v", first, got)
		}
	}
}

func TestSampledIn_ApproximatesRateAcrossManyExecutions(t *testing.T) {
	const n = 10000
	const rate = 0.3
	sampledCount := 0
	for i := 0; i < n; i++ {
		arn := "arn:aws:lambda:us-east-1:123456789012:function:f:exec-" + strconv.Itoa(i)
		if sampledIn(arn, rate) {
			sampledCount++
		}
	}
	got := float64(sampledCount) / float64(n)
	// Allow a reasonably generous tolerance (+/- 0.03) - this is testing
	// statistical distribution, not an exact value, and a tight
	// tolerance would make this test flaky for no real benefit.
	if got < rate-0.03 || got > rate+0.03 {
		t.Errorf("expected roughly %.2f of %d executions to sample in at rate=%.2f, got %.4f (%d/%d)", rate, n, rate, got, sampledCount, n)
	}
}

func TestSampledIn_OutOfRangeRates_ClampToOne(t *testing.T) {
	// sampledIn itself only handles the >=1.0/<=0 fast paths - the
	// actual clamping of an out-of-range Config.SamplingRate (e.g. 1.5
	// or -0.2) to 1.0 happens in New, tested via TestNew_ClampsInvalidSamplingRate
	// below (this test covers sampledIn's own direct >1.0 fast path,
	// which New's clamping means sampledIn should never actually receive
	// in practice, but the function should still behave sensibly if
	// ever called directly with such a value).
	arn := "arn:test"
	if !sampledIn(arn, 1.5) {
		t.Error("expected a rate above 1.0 to behave like 1.0 (always sample in)")
	}
}

func TestNew_ClampsInvalidSamplingRate(t *testing.T) {
	tests := []struct {
		name string
		rate float64
		want float64
	}{
		{"zero value (unset)", 0, 1.0},
		{"negative", -0.5, 1.0},
		{"above one", 1.5, 1.0},
		{"valid fraction", 0.25, 0.25},
		{"valid one", 1.0, 1.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(Config{SamplingRate: tt.rate})
			if p.samplingRate != tt.want {
				t.Errorf("expected samplingRate=%v, got %v", tt.want, p.samplingRate)
			}
		})
	}
}

func TestPlugin_SampledOutExecution_EmitsNoRecords(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	// SamplingRate so small that this specific, fixed ARN is
	// deterministically sampled OUT - confirmed by first checking
	// sampledIn directly, then asserting the SAME ARN's real end-to-end
	// execution emits nothing.
	arn := "arn:aws:lambda:us-east-1:123456789012:function:f:1"
	rate := 0.0000001
	if sampledIn(arn, rate) {
		t.Skip("this specific ARN happens to sample IN at this rate - not a real failure, just an unlucky test-input choice; re-run or adjust the ARN")
	}

	insightPlugin := New(Config{SamplingRate: rate, Exporters: []Exporter{exp}})
	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		return testResult{Status: "done"}, nil
	}
	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-sampling", arn, eventPayload)

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got %s", out.Status)
	}
	if records := exp.snapshot(); len(records) != 0 {
		t.Fatalf("expected 0 emitted records for a sampled-out execution, got %d", len(records))
	}
}

func TestPlugin_DefaultSamplingRate_EmitsEveryExecution(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	insightPlugin := New(Config{Exporters: []Exporter{exp}}) // SamplingRate left unset

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		return testResult{Status: "done"}, nil
	}
	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-default-sampling", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if records := exp.snapshot(); len(records) != 1 {
		t.Fatalf("expected exactly 1 emitted record under the default (unset) SamplingRate, got %d", len(records))
	}
}
