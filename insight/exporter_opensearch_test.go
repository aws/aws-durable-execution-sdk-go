package insight

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// fakeHTTPDoer is a test double for HTTPDoer, letting these tests verify
// the request the exporter builds without real network access -
// matching pkg/durable/awssdk's own fakeLambdaAPI convention adapted to
// plain http.Request/http.Response.
type fakeHTTPDoer struct {
	lastReq    *http.Request
	lastBody   []byte
	statusCode int
	err        error
}

func (f *fakeHTTPDoer) Do(req *http.Request) (*http.Response, error) {
	f.lastReq = req
	if req.Body != nil {
		f.lastBody, _ = io.ReadAll(req.Body)
	}
	if f.err != nil {
		return nil, f.err
	}
	status := f.statusCode
	if status == 0 {
		status = 200
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(""))}, nil
}

func staticCreds() aws.CredentialsProvider {
	return credentials.NewStaticCredentialsProvider("AKIAFAKE", "fakesecret", "")
}

func TestOpenSearchExporter_SigV4_SignsRequest(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &OpenSearchExporter{
		HTTPClient:  fake,
		Endpoint:    "https://my-domain.us-east-1.es.amazonaws.com",
		Region:      "us-east-1",
		Auth:        OpenSearchAuthSigV4,
		Credentials: staticCreds(),
	}

	rec := WorkflowInsightRecord{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionARN: "arn:aws:lambda:us-east-1:123456789012:function:f:1"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if fake.lastReq == nil {
		t.Fatal("expected a request to have been sent")
	}
	if fake.lastReq.Method != http.MethodPut {
		t.Errorf("expected PUT, got %s", fake.lastReq.Method)
	}
	if auth := fake.lastReq.Header.Get("Authorization"); !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
		t.Errorf("expected a SigV4 Authorization header, got %q", auth)
	}
}

func TestOpenSearchExporter_DocumentIDIsExecutionARN(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &OpenSearchExporter{HTTPClient: fake, Endpoint: "https://es.example.com", Region: "us-east-1", Credentials: staticCreds()}

	rec := WorkflowInsightRecord{ExecutionARN: "arn:aws:lambda:us-east-1:123456789012:function:f:1"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if !strings.Contains(fake.lastReq.URL.Path, "/_doc/") {
		t.Errorf("expected the request path to target the _doc API, got %s", fake.lastReq.URL.Path)
	}
	if !strings.HasSuffix(fake.lastReq.URL.Path, url.PathEscape(rec.ExecutionARN)) {
		t.Errorf("expected the document ID (URL-escaped) to be the ExecutionARN, got path %s", fake.lastReq.URL.Path)
	}
}

func TestOpenSearchExporter_DefaultIndexName(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &OpenSearchExporter{HTTPClient: fake, Endpoint: "https://es.example.com", Region: "us-east-1", Credentials: staticCreds()}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if !strings.Contains(fake.lastReq.URL.Path, "/workflow-insight/_doc/") {
		t.Errorf("expected the default index name workflow-insight in the path, got %s", fake.lastReq.URL.Path)
	}
}

func TestOpenSearchExporter_CustomIndexName(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &OpenSearchExporter{HTTPClient: fake, Endpoint: "https://es.example.com", Region: "us-east-1", Credentials: staticCreds(), IndexName: "my-index"}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if !strings.Contains(fake.lastReq.URL.Path, "/my-index/_doc/") {
		t.Errorf("expected the custom index name my-index in the path, got %s", fake.lastReq.URL.Path)
	}
}

func TestOpenSearchExporter_BasicAuth_SetsAuthorizationHeader(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &OpenSearchExporter{
		HTTPClient: fake,
		Endpoint:   "https://opensearch.internal:9200",
		Auth:       OpenSearchAuthBasic,
		Username:   "admin",
		Password:   "secret",
	}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	user, pass, ok := fake.lastReq.BasicAuth()
	if !ok {
		t.Fatal("expected Basic auth credentials on the request")
	}
	if user != "admin" || pass != "secret" {
		t.Errorf("expected admin/secret, got %s/%s", user, pass)
	}
	if strings.HasPrefix(fake.lastReq.Header.Get("Authorization"), "AWS4-HMAC-SHA256") {
		t.Error("expected NO SigV4 signature under OpenSearchAuthBasic")
	}
}

func TestOpenSearchExporter_TrailingSlashInEndpoint_IsHandled(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &OpenSearchExporter{HTTPClient: fake, Endpoint: "https://es.example.com/", Region: "us-east-1", Credentials: staticCreds()}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if strings.Contains(fake.lastReq.URL.String(), "//workflow-insight") {
		t.Errorf("expected no double-slash from a trailing slash on Endpoint, got %s", fake.lastReq.URL.String())
	}
}

func TestOpenSearchExporter_RecordBodyIsValidJSON(t *testing.T) {
	fake := &fakeHTTPDoer{}
	exp := &OpenSearchExporter{HTTPClient: fake, Endpoint: "https://es.example.com", Region: "us-east-1", Credentials: staticCreds()}

	rec := WorkflowInsightRecord{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionARN: "arn:test"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if !strings.Contains(string(fake.lastBody), `"recordType":"WorkflowInsight"`) {
		t.Errorf("expected the request body to contain the marshaled record, got %s", fake.lastBody)
	}
}

func TestOpenSearchExporter_MissingEndpoint_ReturnsError(t *testing.T) {
	exp := &OpenSearchExporter{HTTPClient: &fakeHTTPDoer{}}
	if err := exp.Export(context.Background(), WorkflowInsightRecord{}); err == nil {
		t.Fatal("expected an error when Endpoint is unset")
	}
}

func TestOpenSearchExporter_SigV4WithoutCredentials_ReturnsError(t *testing.T) {
	exp := &OpenSearchExporter{HTTPClient: &fakeHTTPDoer{}, Endpoint: "https://es.example.com", Region: "us-east-1"}
	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err == nil {
		t.Fatal("expected an error when Auth is SigV4 (default) but Credentials is unset")
	}
}

func TestOpenSearchExporter_NonSuccessStatus_ReturnsError(t *testing.T) {
	fake := &fakeHTTPDoer{statusCode: 500}
	exp := &OpenSearchExporter{HTTPClient: fake, Endpoint: "https://es.example.com", Region: "us-east-1", Credentials: staticCreds()}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err == nil {
		t.Fatal("expected an error for a non-2xx response status")
	}
}

func TestOpenSearchExporter_HTTPError_Propagates(t *testing.T) {
	wantErr := errors.New("connection refused")
	fake := &fakeHTTPDoer{err: wantErr}
	exp := &OpenSearchExporter{HTTPClient: fake, Endpoint: "https://es.example.com", Region: "us-east-1", Credentials: staticCreds()}

	err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying HTTP error to propagate (wrapped), got %v", err)
	}
}
