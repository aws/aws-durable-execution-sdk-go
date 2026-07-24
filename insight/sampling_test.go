package insight

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
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
			t.Fatalf("expected deterministic result, got %v then %v", first, got)
		}
	}
}

func TestSampledIn_ApproximatesRate(t *testing.T) {
	const n = 10000
	const rate = 0.3
	count := 0
	for i := 0; i < n; i++ {
		arn := "arn:aws:lambda:us-east-1:123456789012:function:f:exec-" + strconv.Itoa(i)
		if sampledIn(arn, rate) {
			count++
		}
	}
	got := float64(count) / float64(n)
	if got < rate-0.03 || got > rate+0.03 {
		t.Errorf("expected roughly %.2f, got %.4f (%d/%d)", rate, got, count, n)
	}
}

func TestSampledIn_AboveOne_BehavesLikeOne(t *testing.T) {
	if !sampledIn("arn:test", 1.5) {
		t.Error("expected rate above 1.0 to always sample in")
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
			ip := New(Config{SamplingRate: tt.rate})
			if ip.samplingRate != tt.want {
				t.Errorf("expected samplingRate=%v, got %v", tt.want, ip.samplingRate)
			}
		})
	}
}

func TestPlugin_SampledOutExecution_EmitsNoRecords(t *testing.T) {
	exp := &capturingExporter{}
	arn := "arn:aws:lambda:us-east-1:123456789012:function:f:1"
	rate := 0.0000001
	if sampledIn(arn, rate) {
		t.Skip("this ARN samples in at this rate")
	}

	ip := New(Config{SamplingRate: rate, Exporters: []Exporter{exp}})
	ctx := context.Background()

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})
	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	if records := exp.snapshot(); len(records) != 0 {
		t.Fatalf("expected 0 records for sampled-out execution, got %d", len(records))
	}
}

func TestPlugin_DefaultSamplingRate_EmitsEveryExecution(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{Exporters: []Exporter{exp}}) // SamplingRate left unset

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:f:1"

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})
	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	if records := exp.snapshot(); len(records) != 1 {
		t.Fatalf("expected 1 record with default SamplingRate, got %d", len(records))
	}
}
