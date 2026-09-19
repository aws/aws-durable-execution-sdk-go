package durable

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// allOperationSubTypes lists every exported operation subtype constant.
// TestOperationSubTypeConstantsMatchEmittedValues checks that its cases
// cover exactly this list, so adding a constant without a case fails.
var allOperationSubTypes = []string{
	OperationSubTypeStep,
	OperationSubTypeWait,
	OperationSubTypeCallback,
	OperationSubTypeChainedInvoke,
	OperationSubTypeRunInChildContext,
	OperationSubTypeWaitForCallback,
	OperationSubTypeWaitForCondition,
	OperationSubTypeMap,
	OperationSubTypeMapIteration,
	OperationSubTypeParallel,
	OperationSubTypeParallelBranch,
}

// subTypeCase runs one SDK operation on a first invocation and names the
// subtype and type the SDK is expected to record for it.
type subTypeCase struct {
	subType string
	opType  OperationType
	handler Handler[string, string]
}

func operationSubTypeCases() []subTypeCase {
	return []subTypeCase{
		{OperationSubTypeStep, OperationTypeStep, func(ctx Context, _ string) (string, error) {
			return Step(ctx, "op", func(StepContext) (string, error) { return "ok", nil })
		}},
		{OperationSubTypeWait, OperationTypeWait, func(ctx Context, _ string) (string, error) {
			return "", Wait(ctx, "op", time.Minute)
		}},
		{OperationSubTypeCallback, OperationTypeCallback, func(ctx Context, _ string) (string, error) {
			cb, err := CreateCallback[string](ctx, "op")
			if err != nil {
				return "", err
			}
			return cb.Result()
		}},
		{OperationSubTypeChainedInvoke, OperationTypeChainedInvoke, func(ctx Context, event string) (string, error) {
			return Invoke[string](ctx, "op", "target-function", event)
		}},
		{OperationSubTypeRunInChildContext, OperationTypeContext, func(ctx Context, _ string) (string, error) {
			return RunInChildContext(ctx, "op", func(Context) (string, error) { return "ok", nil })
		}},
		{OperationSubTypeWaitForCallback, OperationTypeContext, func(ctx Context, _ string) (string, error) {
			return WaitForCallback[string](ctx, "op", func(StepContext, string) error { return nil })
		}},
		{OperationSubTypeWaitForCondition, OperationTypeStep, func(ctx Context, _ string) (string, error) {
			_, err := WaitForCondition(ctx, "op", func(_ StepContext, state int) (int, error) {
				return state + 1, nil
			}, ConditionConfig[int]{
				WaitStrategy: func(state int, _ int) WaitDecision {
					return WaitDecision{Continue: state < 2, Delay: time.Second}
				},
			})
			return "", err
		}},
		{OperationSubTypeMap, OperationTypeContext, mapHandler},
		{OperationSubTypeMapIteration, OperationTypeContext, mapHandler},
		{OperationSubTypeParallel, OperationTypeContext, parallelHandler},
		{OperationSubTypeParallelBranch, OperationTypeContext, parallelHandler},
	}
}

func mapHandler(ctx Context, _ string) (string, error) {
	_, err := Map(ctx, "op", []int{1}, func(_ Context, item int, _ int) (int, error) {
		return item, nil
	})
	return "", err
}

func parallelHandler(ctx Context, _ string) (string, error) {
	_, err := Parallel(ctx, "op", []Branch[string]{
		{Name: "branch", Func: func(Context) (string, error) { return "ok", nil }},
	})
	return "", err
}

// TestOperationSubTypeConstantsMatchEmittedValues runs one operation per
// exported subtype constant on a first invocation and asserts that the
// SDK records that constant, with the matching operation type, both in
// the checkpoint update it sends and in the OperationHookInfo it reports
// to OnOperationStart.
func TestOperationSubTypeConstantsMatchEmittedValues(t *testing.T) {
	cases := operationSubTypeCases()

	covered := make(map[string]bool, len(cases))
	for _, c := range cases {
		covered[c.subType] = true
	}
	for _, st := range allOperationSubTypes {
		if !covered[st] {
			t.Errorf("no case for exported subtype %q", st)
		}
	}
	if len(covered) != len(allOperationSubTypes) {
		t.Errorf("cases cover %d subtypes, want %d", len(covered), len(allOperationSubTypes))
	}

	for _, c := range cases {
		t.Run(c.subType, func(t *testing.T) {
			fake := &fakeLambda{}
			rec := &opRecorder{}
			h := Wrap(c.handler, withLambdaAPI(fake), WithPlugins(rec.plugin()))
			if _, err := h(context.Background(), stepPayload(`"hello"`)); err != nil {
				t.Fatalf("Invoke() error: %v", err)
			}

			var recorded bool
			for _, u := range updateBatch(t, fake) {
				if aws.ToString(u.SubType) != c.subType {
					continue
				}
				recorded = true
				if u.Type != c.opType {
					t.Errorf("checkpoint update Type = %q, want %q", u.Type, c.opType)
				}
			}
			if !recorded {
				t.Errorf("no checkpoint update carries SubType %q", c.subType)
			}

			var reported bool
			for _, ev := range rec.take() {
				if ev.hook != "start" || ev.info.SubType != c.subType {
					continue
				}
				reported = true
				if ev.info.Type != string(c.opType) {
					t.Errorf("OnOperationStart Type = %q, want %q", ev.info.Type, c.opType)
				}
			}
			if !reported {
				t.Errorf("no OnOperationStart reports SubType %q", c.subType)
			}
		})
	}
}
