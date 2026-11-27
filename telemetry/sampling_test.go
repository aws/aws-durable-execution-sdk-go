package telemetry

import (
	"context"
	"maps"
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

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})
	reportInvocationEnd(ctx, plugin, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	if records := exp.snapshot(); len(records) != 0 {
		t.Fatalf("expected 0 records for sampled-out execution, got %d", len(records))
	}
}

// TestPlugin_SampledOutRegistersOnlyLogContext asserts which hooks a
// sampled-out invocation registers. The SDK tracks operation changes and
// builds an Operations map for a plugin that implements OnOperationChange
// or OnInvocationEnd. So a sampled-out invocation must register neither.
// Its log records still carry the plugin's fields.
func TestPlugin_SampledOutRegistersOnlyLogContext(t *testing.T) {
	const arn = "arn:aws:lambda:us-east-1:123456789012:function:orders:1/durable-execution/name/id"
	// At this rate the sampling threshold is 0 buckets, so every ARN
	// samples out.
	const rate = 1e-9
	for _, mode := range []EmitMode{EmitOnComplete, EmitAlways, EmitOnChange} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := t.Context()
			factory := New(Config{EmitMode: mode, SamplingRate: rate, Exporters: []Exporter{&capturingExporter{}}})
			plugin := createTestPlugin(t, ctx, factory, durable.InvocationHookInfo{ExecutionArn: arn})

			if plugin.OnInvocationStart != nil || plugin.OnOperationChange != nil || plugin.OnInvocationEnd != nil {
				t.Errorf("sampled-out plugin registered hooks: start=%t change=%t end=%t, want none",
					plugin.OnInvocationStart != nil, plugin.OnOperationChange != nil, plugin.OnInvocationEnd != nil)
			}
			if plugin.EnrichLogContext == nil {
				t.Fatal("sampled-out plugin registered no EnrichLogContext")
			}
			want := map[string]any{"execution_arn": arn, "function_name": "orders"}
			if got := plugin.EnrichLogContext(ctx); !maps.Equal(got, want) {
				t.Errorf("EnrichLogContext = %v, want %v", got, want)
			}
		})
	}
}

func TestPlugin_DefaultSamplingRate_EmitsEveryExecution(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{Exporters: []Exporter{exp}}) // SamplingRate left unset

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:f:1"

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	if records := exp.snapshot(); len(records) != 1 {
		t.Fatalf("expected 1 record with default SamplingRate, got %d", len(records))
	}
}

func TestPlugin_ConcurrentInvocations_IsolateSampling(t *testing.T) {
	const rate = 0.5
	arns := make(map[bool]string)
	for i := 0; i < 1000 && len(arns) < 2; i++ {
		arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1/durable-execution/name/id-" + strconv.Itoa(i)
		arns[sampledIn(arn, rate)] = arn
	}
	if len(arns) != 2 {
		t.Fatal("could not find sampled-in and sampled-out fixtures")
	}
	for _, mode := range []EmitMode{EmitOnComplete, EmitAlways, EmitOnChange} {
		for _, firstSampled := range []bool{true, false} {
			t.Run(string(mode)+"/first-sampled="+strconv.FormatBool(firstSampled), func(t *testing.T) {
				ctx := t.Context()
				exp := &capturingExporter{}
				factory := New(Config{EmitMode: mode, SamplingRate: rate, Exporters: []Exporter{exp}, Content: withResults("shared")})
				first := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{ExecutionArn: arns[firstSampled], ExecutionInput: "first"})
				firstReady := make(chan struct{})
				secondReady := make(chan struct{})
				release := make(chan struct{})
				t.Cleanup(func() { close(release) })
				done := make(chan struct{})
				go func() {
					defer close(done)
					reportChange(ctx, first, durable.OperationHookInfo{ID: "shared-id", Type: "STEP", Status: durable.PluginOperationStarted})
					close(firstReady)
					select {
					case <-secondReady:
					case <-release:
						return
					}
					succeeded := durable.OperationHookInfo{ID: "shared-id", Name: "shared", Type: "STEP", Status: durable.PluginOperationSucceeded, Result: "first"}
					reportChange(ctx, first, succeeded)
					reportInvocationEnd(ctx, first, durable.InvocationEndHookInfo{Status: durable.PluginInvocationSucceeded, ExecutionResult: "first", Operations: opsByID(succeeded)})
				}()
				awaitSignal(t, firstReady)
				second := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{ExecutionArn: arns[!firstSampled], ExecutionInput: "second"})
				reportChange(ctx, second, durable.OperationHookInfo{ID: "shared-id", Type: "STEP", Status: durable.PluginOperationStarted})
				close(secondReady)
				succeeded := durable.OperationHookInfo{ID: "shared-id", Name: "shared", Type: "STEP", Status: durable.PluginOperationSucceeded, Result: "second"}
				reportChange(ctx, second, succeeded)
				reportInvocationEnd(ctx, second, durable.InvocationEndHookInfo{Status: durable.PluginInvocationSucceeded, ExecutionResult: "second", Operations: opsByID(succeeded)})
				awaitSignal(t, done)
				records := exp.snapshot()
				if len(records) == 0 {
					t.Fatal("sampled-in execution emitted no records")
				}
				wantValue := "second"
				if firstSampled {
					wantValue = "first"
				}
				for _, rec := range records {
					if rec.ExecutionArn != arns[true] || string(rec.Input) != jsonString(wantValue) {
						t.Errorf("record contains sampled-out execution data: %+v", rec)
					}
				}
				last := records[len(records)-1]
				if last.Status != StatusSucceeded || string(last.Output) != jsonString(wantValue) || len(last.Operations) != 1 || string(last.Operations[0].Result) != jsonString(wantValue) {
					t.Errorf("sampled-in final record = %+v", last)
				}
			})
		}
	}
}
