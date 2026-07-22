package durable

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

func TestWaitStartsAndSuspends(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		if err := Wait(ctx, "pause", 2*time.Second); err != nil {
			return "", err
		}
		return "unreachable", nil
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	updates := updateBatch(t, fake)
	if len(updates) != 1 {
		t.Fatalf("received %d updates, want 1 (START)", len(updates))
	}
	u := updates[0]
	if got, want := aws.ToString(u.Id), hashID("1"); got != want {
		t.Errorf("update Id = %q, want %q", got, want)
	}
	if u.Type != types.OperationTypeWait {
		t.Errorf("update Type = %q, want WAIT", u.Type)
	}
	if got := aws.ToString(u.SubType); got != "Wait" {
		t.Errorf("update SubType = %q, want Wait", got)
	}
	if u.Action != types.OperationActionStart {
		t.Errorf("update Action = %q, want START", u.Action)
	}
	if u.WaitOptions == nil || aws.ToInt32(u.WaitOptions.WaitSeconds) != 2 {
		t.Errorf("WaitOptions = %+v, want WaitSeconds 2", u.WaitOptions)
	}
	if got := aws.ToString(u.Name); got != "pause" {
		t.Errorf("Name = %q, want pause", got)
	}
}

func TestWaitReplaySucceededContinues(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake,
		stepPayload(`""`, wireOperation{Id: hashID("1"), Status: "SUCCEEDED"}),
		func(ctx Context, _ string) (string, error) {
			if err := Wait(ctx, "pause", time.Second); err != nil {
				return "", err
			}
			return "resumed", nil
		})

	if want := `{"Status":"SUCCEEDED","Result":"\"resumed\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if n := len(updateBatch(t, fake)); n != 0 {
		t.Errorf("replayed wait sent %d updates, want 0", n)
	}
}

func TestWaitStartedKeepsWaiting(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake,
		stepPayload(`""`, wireOperation{Id: hashID("1"), Status: "STARTED"}),
		func(ctx Context, _ string) (string, error) {
			return "", Wait(ctx, "pause", time.Second)
		})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if n := len(updateBatch(t, fake)); n != 0 {
		t.Errorf("in-progress wait sent %d updates, want 0", n)
	}
}

func TestWaitSubSecondRoundsUp(t *testing.T) {
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		return "", Wait(ctx, "", 1500*time.Millisecond)
	})

	updates := updateBatch(t, fake)
	if len(updates) != 1 {
		t.Fatalf("received %d updates, want 1", len(updates))
	}
	if got := aws.ToInt32(updates[0].WaitOptions.WaitSeconds); got != 2 {
		t.Errorf("WaitSeconds = %d, want 2 (rounded up)", got)
	}
	if updates[0].Name != nil {
		t.Errorf("unnamed wait Name = %q, want nil", aws.ToString(updates[0].Name))
	}
}

func TestStepThenWaitSequence(t *testing.T) {
	// Conformance 1-8/1-9 shape: a step succeeds, then a wait suspends;
	// on replay both are checkpointed and the handler completes.
	t.Run("first invocation suspends after step", func(t *testing.T) {
		fake := &fakeLambda{}
		resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
			out, err := Step(ctx, "compute", func(StepContext) (string, error) {
				return "computed", nil
			})
			if err != nil {
				return "", err
			}
			if err := Wait(ctx, "pause", 2*time.Second); err != nil {
				return "", err
			}
			return out, nil
		})

		if want := `{"Status":"PENDING"}`; resp != want {
			t.Errorf("response = %s, want %s", resp, want)
		}
		updates := updateBatch(t, fake)
		if len(updates) != 3 {
			t.Fatalf("received %d updates, want 3 (step START, step SUCCEED, wait START)", len(updates))
		}
		if updates[2].Type != types.OperationTypeWait {
			t.Errorf("third update Type = %q, want WAIT", updates[2].Type)
		}
	})

	t.Run("replay completes", func(t *testing.T) {
		fake := &fakeLambda{}
		executed := false
		resp := invokeStep(t, fake,
			stepPayload(`""`,
				checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"computed"`}),
				wireOperation{Id: hashID("2"), Status: "SUCCEEDED"},
			),
			func(ctx Context, _ string) (string, error) {
				out, err := Step(ctx, "compute", func(StepContext) (string, error) {
					executed = true
					return "recomputed", nil
				})
				if err != nil {
					return "", err
				}
				if err := Wait(ctx, "pause", 2*time.Second); err != nil {
					return "", err
				}
				return out, nil
			})

		if executed {
			t.Error("step re-executed during replay")
		}
		if want := `{"Status":"SUCCEEDED","Result":"\"computed\""}`; resp != want {
			t.Errorf("response = %s, want %s", resp, want)
		}
		if n := len(updateBatch(t, fake)); n != 0 {
			t.Errorf("full replay sent %d updates, want 0", n)
		}
	})
}

func TestFailedStepCaughtThenWait(t *testing.T) {
	// Conformance 1-10 shape: a failed step's error is caught by user
	// code, execution continues into a wait.
	t.Run("first invocation", func(t *testing.T) {
		fake := &fakeLambda{}
		resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
			_, err := Step(ctx, "flaky", func(StepContext) (string, error) {
				return "", errors.New("permanent")
			}, WithRetry(NoRetry()))
			var stepErr *StepError
			if !errors.As(err, &stepErr) {
				return "", err
			}
			if err := Wait(ctx, "pause", time.Second); err != nil {
				return "", err
			}
			return "done", nil
		})

		if want := `{"Status":"PENDING"}`; resp != want {
			t.Errorf("response = %s, want %s", resp, want)
		}
	})

	t.Run("replay re-throws without re-executing", func(t *testing.T) {
		fake := &fakeLambda{}
		executed := false
		resp := invokeStep(t, fake,
			stepPayload(`""`,
				checkpointedStep("1", "FAILED", &wireStepDetails{
					Attempt: 1,
					Error:   &wireStepError{ErrorType: "Error", ErrorMessage: "permanent"},
				}),
				wireOperation{Id: hashID("2"), Status: "SUCCEEDED"},
			),
			func(ctx Context, _ string) (string, error) {
				_, err := Step(ctx, "flaky", func(StepContext) (string, error) {
					executed = true
					return "", nil
				}, WithRetry(NoRetry()))
				var stepErr *StepError
				if !errors.As(err, &stepErr) {
					return "", err
				}
				if !strings.Contains(stepErr.Error(), "permanent") {
					return "", errors.New("replayed error lost message")
				}
				if err := Wait(ctx, "pause", time.Second); err != nil {
					return "", err
				}
				return "done", nil
			})

		if executed {
			t.Error("failed step re-executed during replay")
		}
		if want := `{"Status":"SUCCEEDED","Result":"\"done\""}`; resp != want {
			t.Errorf("response = %s, want %s", resp, want)
		}
	})
}
