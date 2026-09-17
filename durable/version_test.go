package durable

import (
	"context"
	"regexp"
	"testing"
)

func TestVersionIsSemver(t *testing.T) {
	// Verify the Version constant follows semantic versioning.
	semverRe := regexp.MustCompile(`^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`)
	if !semverRe.MatchString(Version) {
		t.Errorf("Version = %q, does not match semver pattern", Version)
	}
}

func TestUserAgentKey(t *testing.T) {
	const want = "aws-durable-execution-sdk-go"
	if userAgentKey != want {
		t.Errorf("userAgentKey = %q, want %q", userAgentKey, want)
	}
}

func TestUserAgentNotSetOnUserProvidedClient(t *testing.T) {
	// Verify that WithExecutionClient bypasses internal client
	// construction (and therefore user-agent injection). The injected
	// client is returned as-is.
	fake := &fakeExecClient{}
	opts := handlerOptions{}
	WithExecutionClient(fake).applyHandler(&opts)

	h := &durableHandler[string, string]{options: opts}
	got, err := h.lambdaClient(t.Context())
	if err != nil {
		t.Fatalf("lambdaClient: %v", err)
	}
	if got != fake {
		t.Error("expected user-provided client to be returned unchanged")
	}
}

// fakeExecClient is a minimal ExecutionClient that panics if called. Used
// only to verify that WithExecutionClient returns the client without
// modification.
type fakeExecClient struct{}

func (f *fakeExecClient) GetExecutionState(_ context.Context, _ GetExecutionStateInput) (GetExecutionStateOutput, error) {
	panic("fakeExecClient: unexpected GetExecutionState call")
}

func (f *fakeExecClient) Checkpoint(_ context.Context, _ CheckpointInput) (CheckpointOutput, error) {
	panic("fakeExecClient: unexpected Checkpoint call")
}
