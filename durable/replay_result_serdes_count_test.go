package durable_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// markingSerdes is an untyped JSON serdes. Its Unmarshal appends
// markerSuffix when the target is a *string, so the number of markers in
// a string value equals the number of Unmarshal calls applied to it.
type markingSerdes struct{}

func (markingSerdes) Marshal(_ context.Context, _ durable.SerdesContext, v any) ([]byte, error) {
	return json.Marshal(v)
}

func (markingSerdes) Unmarshal(_ context.Context, _ durable.SerdesContext, data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return err
	}
	if s, ok := v.(*string); ok {
		*s += markerSuffix
	}
	return nil
}

// TestSelectReplayResultConsistency checks that Select over a branch whose
// result exceeds 256 KiB returns the same value on the first run and on
// replay. The branch and the Select record are both too large to store,
// so replay rebuilds both by running the bodies again.
func TestSelectReplayResultConsistency(t *testing.T) {
	big := strings.Repeat("x", bigValueBytes)
	handler := func(ctx durable.Context, _ struct{}) (markerObservation, error) {
		_, v, err := durable.Select(ctx, "select", []durable.Branch[string]{
			{Name: "b0", Func: func(durable.Context) (string, error) { return big, nil }},
		})
		if err != nil {
			return markerObservation{}, err
		}
		first, err := durable.Step(ctx, "capture", func(durable.StepContext) (int, error) {
			return strings.Count(v, markerSuffix), nil
		})
		if err != nil {
			return markerObservation{}, err
		}
		if err := durable.Wait(ctx, "force-replay", time.Second); err != nil {
			return markerObservation{}, err
		}
		return markerObservation{FirstMarkers: first, ReplayMarkers: strings.Count(v, markerSuffix)}, nil
	}
	r, err := durabletest.NewLocalRunner(handler, durable.WithSerdes(markingSerdes{})).RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	obs, err := durabletest.ResultAs[markerObservation](r)
	if err != nil {
		t.Fatal(err)
	}
	if obs.FirstMarkers != 1 || obs.ReplayMarkers != 1 {
		t.Fatalf("firstMarkers=%d replayMarkers=%d, want 1 and 1", obs.FirstMarkers, obs.ReplayMarkers)
	}
}

// TestBatchItemSerdesOncePerDirection checks that a Map item and a
// Parallel branch whose result is stored in the batch aggregate run the
// item serdes once per direction. The handler suspends once, so two
// invocations load the item result. So Marshal runs once and Unmarshal
// runs twice.
func TestBatchItemSerdesOncePerDirection(t *testing.T) {
	cases := []struct {
		name string
		run  func(ctx durable.Context, ser durable.Serdes) (durable.BatchResult[string], error)
	}{
		{"map", func(ctx durable.Context, ser durable.Serdes) (durable.BatchResult[string], error) {
			return durable.Map(ctx, "m", []string{"v"},
				func(_ durable.Context, item string, _ int) (string, error) { return item, nil },
				durable.WithBatchSerdes(ser))
		}},
		{"parallel", func(ctx durable.Context, ser durable.Serdes) (durable.BatchResult[string], error) {
			return durable.Parallel(ctx, "p", []durable.Branch[string]{
				{Name: "b0", Func: func(durable.Context) (string, error) { return "v", nil }},
			}, durable.WithBatchSerdes(ser))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var marshal, unmarshal atomic.Int64
			ser := countingSerdes(&marshal, &unmarshal)
			handler := func(ctx durable.Context, _ struct{}) (string, error) {
				res, err := tc.run(ctx, ser)
				if err != nil {
					return "", err
				}
				if err := durable.Wait(ctx, "force-replay", time.Second); err != nil {
					return "", err
				}
				return res.Results()[0], nil
			}
			r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(struct{}{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := durabletest.ResultAs[string](r)
			if err != nil {
				t.Fatal(err)
			}
			if want := "v" + markerSuffix; got != want {
				t.Errorf("result = %q, want %q", got, want)
			}
			if m, u := marshal.Load(), unmarshal.Load(); m != 1 || u != 2 {
				t.Errorf("marshal=%d unmarshal=%d, want 1 and 2", m, u)
			}
		})
	}
}

// emptyPayloadSerdes marshals every value to the payload it holds, nil or
// an empty slice. Its Unmarshal ignores the bytes and sets a *string to
// "decoded" followed by markerSuffix. It counts both directions.
type emptyPayloadSerdes struct {
	payload            []byte
	marshal, unmarshal *atomic.Int64
}

func (s emptyPayloadSerdes) Marshal(_ context.Context, _ durable.SerdesContext, _ any) ([]byte, error) {
	s.marshal.Add(1)
	return s.payload, nil
}

func (s emptyPayloadSerdes) Unmarshal(_ context.Context, _ durable.SerdesContext, _ []byte, v any) error {
	s.unmarshal.Add(1)
	if p, ok := v.(*string); ok {
		*p = "decoded" + markerSuffix
	}
	return nil
}

// TestBatchItemEmptyPayloadReplay checks that a Map item and a Parallel
// branch whose item serdes marshals to a nil or empty payload are decoded
// on every load. The handler suspends once, so two invocations load the
// item result. So Marshal runs once, Unmarshal runs twice, and replay
// returns the decoded value rather than the zero value.
func TestBatchItemEmptyPayloadReplay(t *testing.T) {
	payloads := []struct {
		name    string
		payload []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
	}
	ops := []struct {
		name string
		run  func(ctx durable.Context, ser durable.Serdes) (durable.BatchResult[string], error)
	}{
		{"map", func(ctx durable.Context, ser durable.Serdes) (durable.BatchResult[string], error) {
			return durable.Map(ctx, "m", []string{"v"},
				func(_ durable.Context, item string, _ int) (string, error) { return item, nil },
				durable.WithBatchSerdes(ser))
		}},
		{"parallel", func(ctx durable.Context, ser durable.Serdes) (durable.BatchResult[string], error) {
			return durable.Parallel(ctx, "p", []durable.Branch[string]{
				{Name: "b0", Func: func(durable.Context) (string, error) { return "v", nil }},
			}, durable.WithBatchSerdes(ser))
		}},
	}
	for _, p := range payloads {
		for _, op := range ops {
			t.Run(op.name+"/"+p.name, func(t *testing.T) {
				var marshal, unmarshal atomic.Int64
				ser := emptyPayloadSerdes{payload: p.payload, marshal: &marshal, unmarshal: &unmarshal}
				handler := func(ctx durable.Context, _ struct{}) (markerObservation, error) {
					res, err := op.run(ctx, ser)
					if err != nil {
						return markerObservation{}, err
					}
					v := res.Results()[0]
					first, err := durable.Step(ctx, "capture", func(durable.StepContext) (int, error) {
						return strings.Count(v, markerSuffix), nil
					})
					if err != nil {
						return markerObservation{}, err
					}
					if err := durable.Wait(ctx, "force-replay", time.Second); err != nil {
						return markerObservation{}, err
					}
					return markerObservation{FirstMarkers: first, ReplayMarkers: strings.Count(v, markerSuffix)}, nil
				}
				r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(struct{}{})
				if err != nil {
					t.Fatal(err)
				}
				obs, err := durabletest.ResultAs[markerObservation](r)
				if err != nil {
					t.Fatal(err)
				}
				if obs.FirstMarkers != 1 || obs.ReplayMarkers != 1 {
					t.Errorf("firstMarkers=%d replayMarkers=%d, want 1 and 1", obs.FirstMarkers, obs.ReplayMarkers)
				}
				if m, u := marshal.Load(), unmarshal.Load(); m != 1 || u != 2 {
					t.Errorf("marshal=%d unmarshal=%d, want 1 and 2", m, u)
				}
			})
		}
	}
}
