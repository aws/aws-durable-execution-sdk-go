package insight

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenSearchExporter_BasicAuth_PostsRecordAsJSON(t *testing.T) {
	var gotReq *http.Request
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotReq = r
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	exp := &OpenSearchExporter{
		Client:   srv.Client(),
		Endpoint: srv.URL,
		Username: "admin",
		Password: "secret",
	}

	rec := Record{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionArn: "arn:test", Status: StatusSucceeded}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if gotReq.Method != http.MethodPost {
		t.Errorf("expected POST, got %s", gotReq.Method)
	}
	if ct := gotReq.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}
	user, pass, ok := gotReq.BasicAuth()
	if !ok || user != "admin" || pass != "secret" {
		t.Errorf("expected basic auth admin/secret, got %s/%s ok=%v", user, pass, ok)
	}
	if !strings.Contains(gotReq.URL.Path, "/workflow-insight/_doc") {
		t.Errorf("expected path to contain /workflow-insight/_doc, got %s", gotReq.URL.Path)
	}

	var decoded Record
	if err := json.Unmarshal(gotBody, &decoded); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if decoded.ExecutionArn != "arn:test" {
		t.Errorf("expected ExecutionArn=arn:test in body, got %q", decoded.ExecutionArn)
	}
}

func TestOpenSearchExporter_BearerToken_SetsAuthorizationHeader(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	exp := &OpenSearchExporter{
		Client:      srv.Client(),
		Endpoint:    srv.URL,
		BearerToken: "my-token-123",
	}

	if err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if gotAuth != "Bearer my-token-123" {
		t.Errorf("expected Bearer token header, got %q", gotAuth)
	}
}

func TestOpenSearchExporter_CustomIndexName(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	exp := &OpenSearchExporter{
		Client:    srv.Client(),
		Endpoint:  srv.URL,
		IndexName: "my-index",
	}

	if err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if !strings.Contains(gotPath, "/my-index/_doc") {
		t.Errorf("expected path to contain /my-index/_doc, got %s", gotPath)
	}
}

func TestOpenSearchExporter_TrailingSlashInEndpoint_IsHandled(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	exp := &OpenSearchExporter{
		Client:   srv.Client(),
		Endpoint: srv.URL + "/",
	}

	if err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if strings.Contains(gotPath, "//") {
		t.Errorf("expected no double-slash in path, got %s", gotPath)
	}
}

func TestOpenSearchExporter_NonSuccessStatus_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	exp := &OpenSearchExporter{Client: srv.Client(), Endpoint: srv.URL}
	if err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test"}); err == nil {
		t.Fatal("expected an error for a non-2xx response status")
	}
}

func TestOpenSearchExporter_MissingEndpoint_ReturnsError(t *testing.T) {
	exp := &OpenSearchExporter{}
	if err := exp.Export(context.Background(), Record{}); err == nil {
		t.Fatal("expected an error when Endpoint is unset")
	}
}

func TestOpenSearchExporter_HTTPError_Propagates(t *testing.T) {
	// Use a server that immediately closes to produce a connection error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	srv.Close() // close immediately so requests fail

	exp := &OpenSearchExporter{Client: srv.Client(), Endpoint: srv.URL}
	err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test"})
	if err == nil {
		t.Fatal("expected an error when the server is unreachable")
	}
	if !strings.Contains(err.Error(), "POST") {
		t.Errorf("expected error to mention POST, got: %v", err)
	}
}

func TestOpenSearchExporter_MaxRecordSizeBytes(t *testing.T) {
	exp := &OpenSearchExporter{}
	if got := exp.MaxRecordSizeBytes(); got != DefaultMaxRecordSizeBytesOpenSearch {
		t.Errorf("expected %d, got %d", DefaultMaxRecordSizeBytesOpenSearch, got)
	}
	exp.MaxSizeBytes = 500
	if got := exp.MaxRecordSizeBytes(); got != 500 {
		t.Errorf("expected 500, got %d", got)
	}
}

// Verify the interface assertion fails at compile time if it doesn't satisfy HTTPDoer.
var _ HTTPDoer = (*http.Client)(nil)
