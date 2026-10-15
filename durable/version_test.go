package durable

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func TestVersionIsSemver(t *testing.T) {
	// Verify the Version constant follows semantic versioning.
	semverRe := regexp.MustCompile(`^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`)
	if !semverRe.MatchString(Version) {
		t.Errorf("Version = %q, does not match semver pattern", Version)
	}
}

// TestDefaultClientUserAgent sends both API calls through the default
// client to a local server and checks the User-Agent header the server
// receives. The header must carry the token
// aws-durable-execution-sdk-go/<Version>, the token format of the durable
// execution SDKs for the other languages, and name no other durable
// execution SDK.
func TestDefaultClientUserAgent(t *testing.T) {
	var (
		mu  sync.Mutex
		got = map[string][]string{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got[r.Method+" "+r.URL.Path[strings.LastIndex(r.URL.Path, "/"):]] = r.Header.Values("User-Agent")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"CheckpointToken":"token-2"}`))
			return
		}
		_, _ = w.Write([]byte(`{"Operations":[]}`))
	}))
	defer srv.Close()

	// Keep the test independent of the machine's AWS configuration, and
	// send the calls to the local server. AWS_EXECUTION_ENV and
	// AWS_SDK_UA_APP_ID add tokens of their own to the header.
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_EXECUTION_ENV", "")
	t.Setenv("AWS_SDK_UA_APP_ID", "")
	t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_LAMBDA", srv.URL)

	client, err := defaultExecutionClient(t.Context())
	if err != nil {
		t.Fatalf("defaultExecutionClient() error = %v", err)
	}
	const arn = "arn:aws:lambda:us-west-2:111122223333:function:f:$LATEST/durable-execution/e/1"
	if _, err := client.GetExecutionState(t.Context(), GetExecutionStateInput{ExecutionArn: arn, CheckpointToken: "token-1"}); err != nil {
		t.Fatalf("GetExecutionState() error = %v", err)
	}
	if _, err := client.Checkpoint(t.Context(), CheckpointInput{ExecutionArn: arn, CheckpointToken: "token-1"}); err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	wantToken := "aws-durable-execution-sdk-go/" + Version
	for _, call := range []string{"GET /state", "POST /checkpoint"} {
		values, ok := got[call]
		if !ok {
			t.Errorf("%s: the server received no request", call)
			continue
		}
		if len(values) != 1 {
			t.Errorf("%s: User-Agent has %d values %q, want 1", call, len(values), values)
			continue
		}
		ua := values[0]
		if !strings.Contains(" "+ua+" ", " "+wantToken+" ") {
			t.Errorf("%s: User-Agent %q has no token %q", call, ua, wantToken)
		}
		if n := strings.Count(ua, "durable-execution-sdk-"); n != 1 {
			t.Errorf("%s: User-Agent %q names a durable execution SDK %d times, want once", call, ua, n)
		}
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
