package durable_test

// TestReplayResultConsistency checks that every path which returns a
// child-context or batch-item result returns the same value on the first run
// and on replay, and applies the configured serdes exactly once per
// direction (Marshal when storing, Unmarshal when loading).
//
// countingSerdes appends markerSuffix on every Unmarshal, so the number of
// markers in a returned value equals the number of Unmarshal calls applied
// to that value. A correct path returns a value with exactly one marker on
// the first run and exactly one marker on replay. A path that applies
// Unmarshal zero times on replay returns the raw body value with no marker;
// a path that applies it twice returns a value with two markers. Either way
// the replay value differs from the first-run value.
//
// Each case runs one sandwich: it reads the result once, captures the
// first-run marker count in a Step (so the count survives replay), suspends
// on a one second Wait to force a replay, then reads the result again. The
// test asserts the first-run count and the replay count are both exactly 1.

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

// markerSuffix is appended on every Unmarshal. It contains no 'x', so it never
// collides with the large value, which is a run of 'x'.
const markerSuffix = "|M|"

// bigValueBytes is 307200 bytes, over the 256 KiB (262144 byte) child-context
// checkpoint limit, so a result of this size takes the ReplayChildren path.
const bigValueBytes = 300 * 1024

// countingSerdes is a string serdes that counts Marshal and Unmarshal calls and
// appends markerSuffix on every Unmarshal.
func countingSerdes(marshal, unmarshal *atomic.Int64) durable.Serdes {
	return durable.SerdesOf[string](
		func(_ context.Context, _ durable.SerdesContext, v string) ([]byte, error) {
			marshal.Add(1)
			return json.Marshal(v)
		},
		func(_ context.Context, _ durable.SerdesContext, data []byte) (string, error) {
			unmarshal.Add(1)
			var s string
			if err := json.Unmarshal(data, &s); err != nil {
				return "", err
			}
			return s + markerSuffix, nil
		},
	)
}

// markerObservation is one case's first-run and replay marker counts.
type markerObservation struct {
	FirstMarkers  int `json:"firstMarkers"`
	ReplayMarkers int `json:"replayMarkers"`
}

func TestReplayResultConsistency(t *testing.T) {
	big := strings.Repeat("x", bigValueBytes)

	cases := []struct {
		name     string
		getValue func(ctx durable.Context, ser durable.Serdes) (string, error)
	}{
		{"child-context fit (first run, stored replay)", func(ctx durable.Context, ser durable.Serdes) (string, error) {
			return durable.RunInChildContext(ctx, "child", func(durable.Context) (string, error) {
				return "v", nil
			}, durable.WithChildSerdes(ser))
		}},
		{"child-context overflow (replay re-runs children)", func(ctx durable.Context, ser durable.Serdes) (string, error) {
			return durable.RunInChildContext(ctx, "child", func(durable.Context) (string, error) {
				return big, nil
			}, durable.WithChildSerdes(ser))
		}},
		{"map item fit", func(ctx durable.Context, ser durable.Serdes) (string, error) {
			res, err := durable.Map(ctx, "m", []string{"v"},
				func(_ durable.Context, item string, _ int) (string, error) { return item, nil },
				durable.WithBatchSerdes(ser))
			if err != nil {
				return "", err
			}
			return res.Results()[0], nil
		}},
		{"map item overflow", func(ctx durable.Context, ser durable.Serdes) (string, error) {
			res, err := durable.Map(ctx, "m", []string{big},
				func(_ durable.Context, item string, _ int) (string, error) { return item, nil },
				durable.WithBatchSerdes(ser))
			if err != nil {
				return "", err
			}
			return res.Results()[0], nil
		}},
		{"parallel branch fit", func(ctx durable.Context, ser durable.Serdes) (string, error) {
			res, err := durable.Parallel(ctx, "p", []durable.Branch[string]{
				{Name: "b0", Func: func(durable.Context) (string, error) { return "v", nil }},
			}, durable.WithBatchSerdes(ser))
			if err != nil {
				return "", err
			}
			return res.Results()[0], nil
		}},
		{"parallel branch overflow", func(ctx durable.Context, ser durable.Serdes) (string, error) {
			res, err := durable.Parallel(ctx, "p", []durable.Branch[string]{
				{Name: "b0", Func: func(durable.Context) (string, error) { return big, nil }},
			}, durable.WithBatchSerdes(ser))
			if err != nil {
				return "", err
			}
			return res.Results()[0], nil
		}},
		{"combinator All overflow", func(ctx durable.Context, ser durable.Serdes) (string, error) {
			f := durable.RunInChildContextAsync(ctx, "g0", func(durable.Context) (string, error) {
				return big, nil
			}, durable.WithChildSerdes(ser))
			vals, err := durable.All(ctx, "all", []*durable.Future[string]{f})
			if err != nil {
				return "", err
			}
			return vals[0], nil
		}},
		{"combinator AllSettled overflow", func(ctx durable.Context, ser durable.Serdes) (string, error) {
			f := durable.RunInChildContextAsync(ctx, "g0", func(durable.Context) (string, error) {
				return big, nil
			}, durable.WithChildSerdes(ser))
			settled, err := durable.AllSettled(ctx, "allsettled", []*durable.Future[string]{f})
			if err != nil {
				return "", err
			}
			return settled[0].Value, settled[0].Err
		}},
		{"combinator Any overflow", func(ctx durable.Context, ser durable.Serdes) (string, error) {
			f := durable.RunInChildContextAsync(ctx, "g0", func(durable.Context) (string, error) {
				return big, nil
			}, durable.WithChildSerdes(ser))
			return durable.Any(ctx, "any", []*durable.Future[string]{f})
		}},
		{"combinator Race overflow", func(ctx durable.Context, ser durable.Serdes) (string, error) {
			f := durable.RunInChildContextAsync(ctx, "g0", func(durable.Context) (string, error) {
				return big, nil
			}, durable.WithChildSerdes(ser))
			return durable.Race(ctx, "race", []*durable.Future[string]{f})
		}},
		{"combinator Join overflow", func(ctx durable.Context, ser durable.Serdes) (string, error) {
			f := durable.RunInChildContextAsync(ctx, "g0", func(durable.Context) (string, error) {
				return big, nil
			}, durable.WithChildSerdes(ser))
			if err := durable.Join(ctx, "join", []durable.Awaitable{f}); err != nil {
				return "", err
			}
			return f.Result(ctx)
		}},
		{"retry attempt overflow", func(ctx durable.Context, ser durable.Serdes) (string, error) {
			return durable.Retry(ctx, "grp", func(durable.Context, int) (string, error) {
				return big, nil
			}, durable.NoRetry(), durable.WithAttemptChildOptions(durable.WithChildSerdes(ser)))
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var marshal, unmarshal atomic.Int64
			ser := countingSerdes(&marshal, &unmarshal)
			handler := func(ctx durable.Context, _ struct{}) (markerObservation, error) {
				v, err := tc.getValue(ctx, ser)
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
			r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(struct{}{})
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != durabletest.Succeeded {
				errType, errMsg := "", ""
				if r.Error != nil {
					errType, errMsg = r.Error.Type, r.Error.Message
				}
				t.Fatalf("status=%s errorType=%s errorMessage=%s", r.Status, errType, errMsg)
			}
			obs, err := durabletest.ResultAs[markerObservation](r)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%-45s firstMarkers=%d replayMarkers=%d marshal=%d unmarshal=%d",
				tc.name, obs.FirstMarkers, obs.ReplayMarkers, marshal.Load(), unmarshal.Load())
			if obs.FirstMarkers != 1 {
				t.Errorf("first run applied Unmarshal %d times, want exactly 1 per direction", obs.FirstMarkers)
			}
			if obs.ReplayMarkers != 1 {
				t.Errorf("replay applied Unmarshal %d times, want exactly 1; replay value differs from the first run", obs.ReplayMarkers)
			}
		})
	}
}
